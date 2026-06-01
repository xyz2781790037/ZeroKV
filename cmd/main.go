package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"kvcache/internal/coordinator"
	"kvcache/internal/distributed"
	"kvcache/internal/ipc"
	"kvcache/internal/network"
	"kvcache/internal/storage"
	"kvcache/pkg/logger"
	"kvcache/proto/controlplane"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	cfg := parseConfig()
	if err := run(cfg); err != nil {
		logger.Log.Fatal("kv cache daemon exited with error", "error", err)
	}
}

type config struct {
	socketPath       string
	p2pAddr          string
	controlAddr      string
	selfID           string
	selfAddr         string
	joinControlAddrs []string
	offheapBytes     uint64
	shmName          string
	shmBytes         uint64
	diskDir          string
	memoryHighBytes  uint64
	memoryLowBytes   uint64
	p2pMaxConns      int
	membershipSync   time.Duration
	shutdownPeriod   time.Duration
}

type peerControlPlane interface {
	distributed.ControlPlane
	RegisterNode(context.Context, *controlplane.RegisterNodeRequest) (*controlplane.RegisterNodeResponse, error)
	GetMembership(context.Context, *controlplane.GetMembershipRequest) (*controlplane.GetMembershipResponse, error)
	SyncMembership(context.Context, *controlplane.SyncMembershipRequest) (*controlplane.SyncMembershipResponse, error)
}

type grpcControlPlaneAdapter struct {
	client controlplane.ControlPlaneClient
}

func (a grpcControlPlaneAdapter) RegisterNode(ctx context.Context, req *controlplane.RegisterNodeRequest) (*controlplane.RegisterNodeResponse, error) {
	return a.client.RegisterNode(ctx, req)
}

func (a grpcControlPlaneAdapter) GetMembership(ctx context.Context, req *controlplane.GetMembershipRequest) (*controlplane.GetMembershipResponse, error) {
	return a.client.GetMembership(ctx, req)
}

func (a grpcControlPlaneAdapter) SyncMembership(ctx context.Context, req *controlplane.SyncMembershipRequest) (*controlplane.SyncMembershipResponse, error) {
	return a.client.SyncMembership(ctx, req)
}

func (a grpcControlPlaneAdapter) AnnounceBlock(ctx context.Context, req *controlplane.AnnounceBlockRequest) (*controlplane.AnnounceBlockResponse, error) {
	return a.client.AnnounceBlock(ctx, req)
}

func (a grpcControlPlaneAdapter) ForgetBlock(ctx context.Context, req *controlplane.ForgetBlockRequest) (*controlplane.ForgetBlockResponse, error) {
	return a.client.ForgetBlock(ctx, req)
}

func (a grpcControlPlaneAdapter) GetBlockLocations(ctx context.Context, req *controlplane.GetBlockLocationsRequest) (*controlplane.GetBlockLocationsResponse, error) {
	return a.client.GetBlockLocations(ctx, req)
}

func parseConfig() config {
	hostname, _ := os.Hostname()
	socketPath := flag.String("socket", "/tmp/kvcache.sock", "Unix domain socket path for C++ clients")
	p2pAddr := flag.String("p2p-addr", ":19090", "TCP address for P2P block transfer")
	controlAddr := flag.String("control-addr", ":19091", "gRPC address for control plane")
	selfID := flag.String("node-id", hostname, "local node id")
	selfAddr := flag.String("node-addr", "", "advertised P2P address; defaults to -p2p-addr")
	joinControlAddrs := flag.String("join-control-addrs", "", "comma-separated peer gRPC control-plane addresses")
	offheapBytes := flag.Uint64("offheap-bytes", 1<<30, "offheap memory pool size in bytes")
	shmName := flag.String("shm-name", "", "daemon-owned POSIX shared memory pool name; empty uses kvcache_daemon_<pid>")
	shmBytes := flag.Uint64("shm-bytes", 0, "daemon-owned POSIX shared memory pool size in bytes; defaults to -offheap-bytes")
	diskDir := flag.String("disk-dir", "", "local disk tier directory; empty disables disk tier")
	memoryHighBytes := flag.Uint64("memory-high-bytes", 0, "local memory tier high watermark; requires -disk-dir")
	memoryLowBytes := flag.Uint64("memory-low-bytes", 0, "local memory tier low watermark after LRU spill; defaults to 80% of high")
	p2pMaxConns := flag.Int("p2p-max-conns", network.DefaultMaxConnections, "maximum concurrent P2P TCP connections")
	membershipSync := flag.Duration("membership-sync-interval", 5*time.Second, "interval for syncing membership with joined control planes")
	shutdownPeriod := flag.Duration("shutdown-timeout", 10*time.Second, "graceful shutdown timeout")
	flag.Parse()

	if *selfAddr == "" {
		*selfAddr = *p2pAddr
	}
	if *shmBytes == 0 {
		*shmBytes = *offheapBytes
	}
	return config{
		socketPath:       *socketPath,
		p2pAddr:          *p2pAddr,
		controlAddr:      *controlAddr,
		selfID:           *selfID,
		selfAddr:         *selfAddr,
		joinControlAddrs: parseAddrList(*joinControlAddrs),
		offheapBytes:     *offheapBytes,
		shmName:          *shmName,
		shmBytes:         *shmBytes,
		diskDir:          *diskDir,
		memoryHighBytes:  *memoryHighBytes,
		memoryLowBytes:   *memoryLowBytes,
		p2pMaxConns:      *p2pMaxConns,
		membershipSync:   *membershipSync,
		shutdownPeriod:   *shutdownPeriod,
	}
}

func run(cfg config) error {
	if cfg.selfID == "" {
		return fmt.Errorf("main: empty node id")
	}
	signalCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stopSignals()
	ctx, cancelRun := context.WithCancel(signalCtx)
	defer cancelRun()

	pool, err := storage.NewOffheapPool(cfg.offheapBytes)
	if err != nil {
		return err
	}

	shmPool, err := storage.NewSharedMemoryPool(cfg.shmName, cfg.shmBytes)
	if err != nil {
		_ = pool.Release()
		return err
	}

	handler, err := storage.NewHandlerWithSharedMemory(pool, shmPool)
	if err != nil {
		_ = shmPool.Release()
		_ = pool.Release()
		return err
	}
	defer func() {
		if err := handler.Release(); err != nil {
			logger.Log.Warn("failed to release local memory pools", "error", err)
		}
	}()
	var diskTier *storage.DiskTier
	if cfg.diskDir != "" {
		diskTier, err = storage.NewDiskTier(cfg.diskDir)
		if err != nil {
			return err
		}
	}

	self := coordinator.Node{
		ID:            coordinator.NodeID(cfg.selfID),
		Addr:          cfg.selfAddr,
		State:         coordinator.NodeStateAlive,
		LastHeartbeat: time.Now(),
	}
	membership, err := coordinator.NewMembership(self)
	if err != nil {
		return err
	}
	router, err := coordinator.NewRouter(self.ID, membership)
	if err != nil {
		return err
	}
	controlService, err := coordinator.NewControlPlaneService(membership, router)
	if err != nil {
		return err
	}
	defer controlService.Close()

	peerConns, peerControls, err := connectPeerControlPlanes(cfg.joinControlAddrs)
	if err != nil {
		return err
	}
	defer closeClientConns(peerConns)
	peerStoreControls := peerStoreControlPlanes(peerControls)

	distributedStore, err := distributed.NewStore(handler, controlService, distributed.StoreOptions{
		SelfID:               cfg.selfID,
		SelfAddr:             cfg.selfAddr,
		PeerControls:         peerStoreControls,
		DiskTier:             diskTier,
		MemoryHighWaterBytes: cfg.memoryHighBytes,
		MemoryLowWaterBytes:  cfg.memoryLowBytes,
	})
	if err != nil {
		return err
	}
	distributedStore.AnnounceDiskBlocks(ctx)

	udsServer := ipc.NewUDSServer(cfg.socketPath, distributedStore)
	if err := udsServer.Start(); err != nil {
		return err
	}

	errCh := make(chan error, 3)
	var wg sync.WaitGroup

	grpcServer := grpc.NewServer()
	coordinator.RegisterControlPlaneService(grpcServer, controlService)
	controlListener, err := net.Listen("tcp", cfg.controlAddr)
	if err != nil {
		cancelRun()
		udsServer.Stop()
		return err
	}

	p2pServer := network.NewServer(cfg.p2pAddr, distributedStore)
	p2pServer.MaxConnections = cfg.p2pMaxConns
	p2pServer.ErrorHandler = func(err error) {
		logger.Log.Warn("p2p server error", "error", err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := p2pServer.ListenAndServe(ctx); err != nil {
			errCh <- fmt.Errorf("p2p server: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := grpcServer.Serve(controlListener); err != nil {
			errCh <- fmt.Errorf("control plane grpc: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runMembershipSyncLoop(ctx, cfg, controlService, membership, peerControls)
	}()

	logger.Log.Info("kv cache daemon is ready",
		"node_id", cfg.selfID,
		"node_addr", cfg.selfAddr,
		"socket_path", cfg.socketPath,
		"p2p_addr", cfg.p2pAddr,
		"control_addr", cfg.controlAddr,
		"join_control_addrs", cfg.joinControlAddrs,
		"offheap_bytes", cfg.offheapBytes,
		"shm_name", shmPool.Name(),
		"shm_bytes", cfg.shmBytes,
		"disk_dir", cfg.diskDir,
		"memory_high_bytes", cfg.memoryHighBytes,
		"memory_low_bytes", cfg.memoryLowBytes)

	var runErr error
	select {
	case <-ctx.Done():
		logger.Log.Warn("shutdown signal received")
	case err := <-errCh:
		runErr = err
		logger.Log.Error("daemon component failed", "error", err)
	}
	cancelRun()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownPeriod)
	defer cancel()
	stopSignals()

	udsServer.Stop()
	gracefulStopGRPC(shutdownCtx, grpcServer)
	wg.Wait()

	logger.Log.Info("kv cache daemon exited")
	return runErr
}

func parseAddrList(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	addrs := make([]string, 0, len(parts))
	for _, part := range parts {
		addr := strings.TrimSpace(part)
		if addr == "" {
			continue
		}
		if strings.HasPrefix(addr, ":") {
			addr = "localhost" + addr
		}
		addrs = append(addrs, addr)
	}
	return addrs
}

func connectPeerControlPlanes(addrs []string) ([]*grpc.ClientConn, []peerControlPlane, error) {
	conns := make([]*grpc.ClientConn, 0, len(addrs))
	clients := make([]peerControlPlane, 0, len(addrs))
	for _, addr := range addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			closeClientConns(conns)
			return nil, nil, fmt.Errorf("connect peer control plane %s: %w", addr, err)
		}
		conns = append(conns, conn)
		clients = append(clients, grpcControlPlaneAdapter{
			client: controlplane.NewControlPlaneClient(conn),
		})
	}
	return conns, clients, nil
}

func peerStoreControlPlanes(peers []peerControlPlane) []distributed.ControlPlane {
	controls := make([]distributed.ControlPlane, 0, len(peers))
	for _, peer := range peers {
		controls = append(controls, peer)
	}
	return controls
}

func closeClientConns(conns []*grpc.ClientConn) {
	for _, conn := range conns {
		if conn != nil {
			_ = conn.Close()
		}
	}
}

func runMembershipSyncLoop(ctx context.Context, cfg config, local *coordinator.ControlPlaneService, membership *coordinator.Membership, peers []peerControlPlane) {
	if len(peers) == 0 {
		<-ctx.Done()
		return
	}
	interval := cfg.membershipSync
	if interval <= 0 {
		interval = 5 * time.Second
	}
	syncOnce := func() {
		for _, peer := range peers {
			if peer == nil {
				continue
			}
			if err := syncMembershipWithPeer(ctx, cfg, local, membership, peer); err != nil {
				logger.Log.Warn("membership sync failed",
					"component", "membership_sync",
					"node_id", cfg.selfID,
					"error", err)
			}
		}
	}
	syncOnce()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			syncOnce()
		}
	}
}

func syncMembershipWithPeer(ctx context.Context, cfg config, local *coordinator.ControlPlaneService, membership *coordinator.Membership, peer peerControlPlane) error {
	self := &controlplane.Node{
		Id:                    cfg.selfID,
		Addr:                  cfg.selfAddr,
		State:                 controlplane.NodeState_NODE_STATE_ALIVE,
		LastHeartbeatUnixNano: time.Now().UnixNano(),
	}
	if _, err := peer.RegisterNode(ctx, &controlplane.RegisterNodeRequest{Node: self}); err != nil {
		return err
	}
	if _, err := peer.SyncMembership(ctx, &controlplane.SyncMembershipRequest{
		Nodes: nodesToProto(membership.List()),
	}); err != nil {
		return err
	}
	resp, err := peer.GetMembership(ctx, &controlplane.GetMembershipRequest{
		IncludeTombstones: true,
	})
	if err != nil {
		return err
	}
	_, err = local.SyncMembership(ctx, &controlplane.SyncMembershipRequest{
		Nodes:             resp.GetNodes(),
		MembershipVersion: resp.GetMembershipVersion(),
	})
	return err
}

func nodesToProto(nodes []coordinator.Node) []*controlplane.Node {
	out := make([]*controlplane.Node, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, &controlplane.Node{
			Id:                    string(node.ID),
			Addr:                  node.Addr,
			State:                 nodeStateToProto(node.State),
			Version:               node.Version,
			LastHeartbeatUnixNano: node.LastHeartbeat.UnixNano(),
		})
	}
	return out
}

func nodeStateToProto(state coordinator.NodeState) controlplane.NodeState {
	switch state {
	case coordinator.NodeStateAlive:
		return controlplane.NodeState_NODE_STATE_ALIVE
	case coordinator.NodeStateSuspect:
		return controlplane.NodeState_NODE_STATE_SUSPECT
	case coordinator.NodeStateDown:
		return controlplane.NodeState_NODE_STATE_DOWN
	case coordinator.NodeStateTombstone:
		return controlplane.NodeState_NODE_STATE_TOMBSTONE
	default:
		return controlplane.NodeState_NODE_STATE_UNKNOWN
	}
}

func gracefulStopGRPC(ctx context.Context, server *grpc.Server) {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		server.Stop()
		<-done
	}
}
