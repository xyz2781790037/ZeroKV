package distributed

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"kvcache/internal/network"
	"kvcache/internal/storage"
	"kvcache/pkg/logger"
	"kvcache/pkg/protocol"
	"kvcache/proto/controlplane"
)

const (
	defaultAnnounceTimeout = 2 * time.Second
	defaultFetchTimeout    = 30 * time.Second
)

// ComputePlacement controls how Store places a compute request when the block
// is not already local.
type ComputePlacement uint8

const (
	// ComputePlacementHybrid first tries compute-to-data on healthy remote
	// holders, then falls back to fetching the block and computing locally.
	ComputePlacementHybrid ComputePlacement = iota

	// ComputePlacementRemoteOnly keeps strict compute-to-data behavior.
	ComputePlacementRemoteOnly

	// ComputePlacementFetchLocalOnly always moves the block to this node before
	// executing the operator.
	ComputePlacementFetchLocalOnly
)

// RemoteComputeAllowed lets the scheduler plug in load information. Return
// false when the node that owns loc is too busy to accept more compute work.
type RemoteComputeAllowed func(loc *controlplane.BlockLocation) bool

type ControlPlane interface {
	AnnounceBlock(context.Context, *controlplane.AnnounceBlockRequest) (*controlplane.AnnounceBlockResponse, error)
	ForgetBlock(context.Context, *controlplane.ForgetBlockRequest) (*controlplane.ForgetBlockResponse, error)
	GetBlockLocations(context.Context, *controlplane.GetBlockLocationsRequest) (*controlplane.GetBlockLocationsResponse, error)
}

type localBlockAllocator interface {
	AllocateBlock(context.Context, uint64, protocol.AllocateBlock) (protocol.BlockAllocation, error)
	OwnsSharedMemory(string) bool
}

// Store composes the local offheap store with the cluster control plane.
// It is the smallest distributed read-through layer:
// 1. local UDS writes are first committed locally, then announced to the control plane;
// 2. local misses query block locations and fetch one remote replica over P2P;
// 3. fetched blocks are imported into the local offheap pool and announced as a new replica.
type Store struct {
	local  *storage.Handler
	disk   *storage.DiskTier
	client ControlPlane
	peers  []ControlPlane
	p2p    *network.Client

	selfID   string
	selfAddr string

	announceTimeout time.Duration
	fetchTimeout    time.Duration

	computePlacement     ComputePlacement
	remoteComputeAllowed RemoteComputeAllowed

	localLRU             *localMemoryLRU
	memoryHighWaterBytes uint64
	memoryLowWaterBytes  uint64

	mu       sync.Mutex
	fetching map[uint64]struct{}
}

type StoreOptions struct {
	SelfID               string
	SelfAddr             string
	PeerControls         []ControlPlane
	DiskTier             *storage.DiskTier
	AnnounceTimeout      time.Duration
	FetchTimeout         time.Duration
	P2PClient            *network.Client
	MemoryHighWaterBytes uint64
	MemoryLowWaterBytes  uint64

	ComputePlacement     ComputePlacement
	RemoteComputeAllowed RemoteComputeAllowed
}

func NewStore(local *storage.Handler, client ControlPlane, opts StoreOptions) (*Store, error) {
	if local == nil {
		return nil, fmt.Errorf("distributed store: nil local handler")
	}
	if opts.SelfID == "" {
		return nil, fmt.Errorf("distributed store: empty self id")
	}
	if opts.MemoryHighWaterBytes > 0 && opts.DiskTier == nil {
		return nil, fmt.Errorf("distributed store: memory LRU requires disk tier")
	}
	p2pClient := opts.P2PClient
	if p2pClient == nil {
		p2pClient = network.NewClient()
	}
	announceTimeout := opts.AnnounceTimeout
	if announceTimeout <= 0 {
		announceTimeout = defaultAnnounceTimeout
	}
	fetchTimeout := opts.FetchTimeout
	if fetchTimeout <= 0 {
		fetchTimeout = defaultFetchTimeout
	}
	memoryHighWaterBytes, memoryLowWaterBytes := normalizeMemoryWatermarks(opts.MemoryHighWaterBytes, opts.MemoryLowWaterBytes)
	var localLRU *localMemoryLRU
	if memoryHighWaterBytes > 0 {
		localLRU = newLocalMemoryLRU()
	}
	return &Store{
		local:                local,
		disk:                 opts.DiskTier,
		client:               client,
		peers:                append([]ControlPlane(nil), opts.PeerControls...),
		p2p:                  p2pClient,
		selfID:               opts.SelfID,
		selfAddr:             opts.SelfAddr,
		announceTimeout:      announceTimeout,
		fetchTimeout:         fetchTimeout,
		computePlacement:     normalizeComputePlacement(opts.ComputePlacement),
		remoteComputeAllowed: opts.RemoteComputeAllowed,
		localLRU:             localLRU,
		memoryHighWaterBytes: memoryHighWaterBytes,
		memoryLowWaterBytes:  memoryLowWaterBytes,
		fetching:             make(map[uint64]struct{}),
	}, nil
}

func (s *Store) AnnounceDiskBlocks(ctx context.Context) {
	if s == nil || s.disk == nil {
		return
	}
	for _, meta := range s.disk.ListMeta() {
		if err := s.announceLocalTier(ctx, meta.Seq, meta.ID, meta.Length, meta.Checksum, controlplane.StorageTier_STORAGE_TIER_DISK); err != nil {
			logger.Log.Warn("failed to announce restored disk block",
				"component", "distributed_store",
				"block_id", meta.ID,
				"error", err)
		}
	}
}

func (s *Store) AllocateBlock(ctx context.Context, seq uint64, req protocol.AllocateBlock) (protocol.BlockAllocation, error) {
	if s == nil {
		return protocol.BlockAllocation{}, fmt.Errorf("distributed store: nil store")
	}
	if req.BlockID == 0 {
		return protocol.BlockAllocation{}, fmt.Errorf("distributed store: zero block id")
	}
	if req.Length == 0 {
		return protocol.BlockAllocation{}, fmt.Errorf("distributed store: zero block length")
	}
	if err := s.ensureMemoryHeadroom(ctx, req.Length); err != nil {
		return protocol.BlockAllocation{}, err
	}
	allocator, ok := any(s.local).(localBlockAllocator)
	if !ok {
		return protocol.BlockAllocation{}, fmt.Errorf("distributed store: local handler does not support daemon shared memory allocation")
	}
	return allocator.AllocateBlock(ctx, seq, req)
}

// HandleBlockReady keeps the existing single-node write path intact, then makes
// the new local replica visible to the cluster control plane.
func (s *Store) HandleBlockReady(ctx context.Context, seq uint64, block protocol.BlockReady) error {
	if s == nil {
		return fmt.Errorf("distributed store: nil store")
	}
	if allocator, ok := any(s.local).(localBlockAllocator); !ok || !allocator.OwnsSharedMemory(block.ShmName) {
		if err := s.ensureMemoryHeadroom(ctx, block.Length); err != nil {
			return err
		}
	}
	if err := s.local.HandleBlockReady(ctx, seq, block); err != nil {
		return err
	}
	if meta, ok := s.local.Meta(block.BlockID); ok {
		s.touchMemory(meta)
	}
	if s.disk != nil {
		if err := s.mirrorBlockToDisk(ctx, seq, block.BlockID); err != nil {
			logger.Log.Warn("failed to mirror block to disk tier",
				"component", "distributed_store",
				"block_id", block.BlockID,
				"error", err)
		}
	}
	if err := s.announce(ctx, seq, block.BlockID, block.Length, block.Checksum); err != nil {
		return err
	}
	return s.enforceMemoryWatermark(ctx)
}

// OpenBlock serves in tier order: local offheap memory, local disk, then
// remote peer fetch recorded by the control plane.
func (s *Store) OpenBlock(blockID uint64) (io.ReadCloser, uint64, uint64, uint32, bool, error) {
	if s == nil {
		return nil, 0, 0, 0, false, fmt.Errorf("distributed store: nil store")
	}
	reader, id, length, checksum, ok, err := s.local.OpenBlock(blockID)
	if ok || err != nil {
		if ok {
			s.touchMemoryID(blockID)
		}
		return reader, id, length, checksum, ok, err
	}
	var localDiskErr error
	reader, id, length, checksum, ok, err = s.openDiskBlock(blockID)
	if ok {
		return reader, id, length, checksum, true, nil
	}
	if err != nil {
		localDiskErr = err
		logger.Log.Warn("failed to open block from disk tier",
			"component", "distributed_store",
			"block_id", blockID,
			"error", err)
	}
	if err := s.fetchAndCache(blockID); err != nil {
		if errors.Is(err, network.ErrBlockNotFound) {
			if localDiskErr != nil {
				return nil, 0, 0, 0, false, localDiskErr
			}
			return nil, 0, 0, 0, false, nil
		}
		return nil, 0, 0, 0, false, err
	}
	reader, id, length, checksum, ok, err = s.local.OpenBlock(blockID)
	if ok {
		s.touchMemoryID(blockID)
	}
	return reader, id, length, checksum, ok, err
}

// ComputeBlockNearData executes a request using hybrid placement:
// 1. local data computes locally;
// 2. remote holders compute when the placement policy allows it;
// 3. overloaded/failed remote holders fall back to fetch-then-compute locally.
func (s *Store) ComputeBlockNearData(ctx context.Context, req network.ComputeRequest) (network.ComputeResult, error) {
	if s == nil {
		return network.ComputeResult{}, fmt.Errorf("distributed store: nil store")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if req.BlockID == 0 {
		return network.ComputeResult{}, fmt.Errorf("distributed store: zero block id")
	}
	if req.Operator == "" {
		return network.ComputeResult{}, fmt.Errorf("distributed store: empty compute operator")
	}
	if result, found, err := s.ComputeBlock(ctx, req); err != nil {
		return network.ComputeResult{}, err
	} else if found {
		return result, nil
	}
	if s.client == nil {
		return network.ComputeResult{}, network.ErrBlockNotFound
	}

	locations, err := s.getLocations(ctx, req.BlockID)
	if err != nil {
		return network.ComputeResult{}, fmt.Errorf("distributed store: get locations for block %d: %w", req.BlockID, err)
	}
	locations = sortLocations(locations)

	placement := s.computePlacement
	var remoteErr error
	if placement != ComputePlacementFetchLocalOnly {
		result, attempted, err := s.computeOnRemoteHolders(ctx, req, locations)
		if err == nil {
			return result, nil
		}
		if attempted {
			remoteErr = err
		}
		if placement == ComputePlacementRemoteOnly {
			if remoteErr != nil {
				return network.ComputeResult{}, fmt.Errorf("distributed store: compute block %d near data: %w", req.BlockID, remoteErr)
			}
			return network.ComputeResult{}, network.ErrBlockNotFound
		}
	}

	result, err := s.fetchThenComputeLocal(ctx, req, locations)
	if err == nil {
		if remoteErr != nil {
			logger.Log.Warn("remote compute failed; fetched block and computed locally",
				"component", "distributed_store",
				"block_id", req.BlockID,
				"remote_error", remoteErr)
		}
		return result, nil
	}
	if remoteErr != nil {
		return network.ComputeResult{}, fmt.Errorf("distributed store: compute block %d hybrid failed: remote=%v fetch_local=%w", req.BlockID, remoteErr, err)
	}
	return network.ComputeResult{}, err
}

// ComputeBlock executes a built-in operator against a block stored on this
// daemon. It intentionally checks only local memory/disk so an inbound remote
// compute request cannot trigger another cross-node fetch.
func (s *Store) ComputeBlock(ctx context.Context, req network.ComputeRequest) (network.ComputeResult, bool, error) {
	if s == nil {
		return network.ComputeResult{}, false, fmt.Errorf("distributed store: nil store")
	}
	if err := ctx.Err(); err != nil {
		return network.ComputeResult{}, false, err
	}
	reader, blockID, _, _, ok, err := s.openLocalBlock(req.BlockID)
	if err != nil || !ok {
		return network.ComputeResult{}, ok, err
	}
	defer reader.Close()

	result, err := executeComputeOperator(reader, req)
	if err != nil {
		return network.ComputeResult{}, true, err
	}
	result.BlockID = blockID
	result.Operator = req.Operator
	return result, true, nil
}

func (s *Store) computeOnRemoteHolders(ctx context.Context, req network.ComputeRequest, locations []*controlplane.BlockLocation) (network.ComputeResult, bool, error) {
	var lastErr error
	attempted := false
	for _, loc := range locations {
		if s.skipLocation(loc) || !s.allowsRemoteCompute(loc) {
			continue
		}
		attempted = true
		result, err := s.p2p.ComputeBlock(ctx, loc.GetAddr(), req)
		if err != nil {
			lastErr = err
			continue
		}
		return result, true, nil
	}
	if lastErr != nil {
		return network.ComputeResult{}, attempted, lastErr
	}
	return network.ComputeResult{}, attempted, network.ErrBlockNotFound
}

func (s *Store) fetchThenComputeLocal(ctx context.Context, req network.ComputeRequest, locations []*controlplane.BlockLocation) (network.ComputeResult, error) {
	if err := s.fetchAndCacheFromLocations(ctx, req.BlockID, locations); err != nil {
		return network.ComputeResult{}, err
	}
	result, found, err := s.ComputeBlock(ctx, req)
	if err != nil {
		return network.ComputeResult{}, err
	}
	if !found {
		return network.ComputeResult{}, network.ErrBlockNotFound
	}
	return result, nil
}

func (s *Store) allowsRemoteCompute(loc *controlplane.BlockLocation) bool {
	if s == nil || loc == nil {
		return false
	}
	if s.remoteComputeAllowed == nil {
		return true
	}
	return s.remoteComputeAllowed(loc)
}

func (s *Store) announce(parent context.Context, seq uint64, blockID uint64, length uint64, checksum uint32) error {
	return s.announceTier(parent, seq, blockID, length, checksum, controlplane.StorageTier_STORAGE_TIER_MEMORY)
}

func (s *Store) announceTier(parent context.Context, seq uint64, blockID uint64, length uint64, checksum uint32, tier controlplane.StorageTier) error {
	if err := s.announceLocalTier(parent, seq, blockID, length, checksum, tier); err != nil {
		return err
	}
	ctx := parent
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, s.announceTimeout)
	defer cancel()

	for _, peer := range s.peers {
		if peer == nil {
			continue
		}
		_, err := peer.AnnounceBlock(ctx, &controlplane.AnnounceBlockRequest{
			NodeId: s.selfID,
			Meta: &controlplane.BlockMeta{
				BlockId:  blockID,
				Length:   length,
				Checksum: checksum,
				Seq:      seq,
			},
			Tier: tier,
		})
		if err != nil {
			logger.Log.Warn("failed to announce block to peer control plane",
				"component", "distributed_store",
				"block_id", blockID,
				"error", err)
		}
	}
	return nil
}

func (s *Store) announceLocalTier(parent context.Context, seq uint64, blockID uint64, length uint64, checksum uint32, tier controlplane.StorageTier) error {
	if s.client == nil {
		return nil
	}
	ctx := parent
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, s.announceTimeout)
	defer cancel()

	_, err := s.client.AnnounceBlock(ctx, &controlplane.AnnounceBlockRequest{
		NodeId: s.selfID,
		Meta: &controlplane.BlockMeta{
			BlockId:  blockID,
			Length:   length,
			Checksum: checksum,
			Seq:      seq,
		},
		Tier: tier,
	})
	if err != nil {
		return fmt.Errorf("distributed store: announce block %d: %w", blockID, err)
	}
	return nil
}

func (s *Store) forgetTier(parent context.Context, blockID uint64, tier controlplane.StorageTier, reason string) error {
	if err := s.forgetLocalTier(parent, blockID, tier, reason); err != nil {
		return err
	}
	ctx := parent
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, s.announceTimeout)
	defer cancel()

	for _, peer := range s.peers {
		if peer == nil {
			continue
		}
		_, err := peer.ForgetBlock(ctx, &controlplane.ForgetBlockRequest{
			NodeId:  s.selfID,
			BlockId: blockID,
			Tier:    tier,
			Reason:  reason,
		})
		if err != nil {
			logger.Log.Warn("failed to forget block on peer control plane",
				"component", "distributed_store",
				"block_id", blockID,
				"tier", tier.String(),
				"error", err)
		}
	}
	return nil
}

func (s *Store) forgetLocalTier(parent context.Context, blockID uint64, tier controlplane.StorageTier, reason string) error {
	if s.client == nil {
		return nil
	}
	ctx := parent
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, s.announceTimeout)
	defer cancel()

	_, err := s.client.ForgetBlock(ctx, &controlplane.ForgetBlockRequest{
		NodeId:  s.selfID,
		BlockId: blockID,
		Tier:    tier,
		Reason:  reason,
	})
	if err != nil {
		return fmt.Errorf("distributed store: forget block %d: %w", blockID, err)
	}
	return nil
}

func (s *Store) mirrorBlockToDisk(ctx context.Context, seq uint64, blockID uint64) error {
	if s.disk == nil {
		return nil
	}
	lease, ok := s.local.Acquire(blockID)
	if !ok {
		return fmt.Errorf("distributed store: block %d missing after memory commit", blockID)
	}
	defer lease.Release()

	block := lease.Block()
	if err := s.disk.Put(block); err != nil {
		return err
	}
	return s.announceTier(ctx, seq, block.ID, block.Length, block.Checksum, controlplane.StorageTier_STORAGE_TIER_DISK)
}

func (s *Store) openDiskBlock(blockID uint64) (io.ReadCloser, uint64, uint64, uint32, bool, error) {
	if s == nil || s.disk == nil || !s.disk.Has(blockID) {
		return nil, 0, 0, 0, false, nil
	}
	meta, hasMeta := s.disk.Meta(blockID)
	if hasMeta {
		if err := s.ensureMemoryHeadroom(context.Background(), meta.Length); err != nil {
			logger.Log.Warn("failed to reserve memory before disk promotion",
				"component", "distributed_store",
				"block_id", blockID,
				"error", err)
		}
	}
	if err := s.local.PromoteFromDisk(blockID, s.disk); err == nil {
		reader, id, length, checksum, ok, err := s.local.OpenBlock(blockID)
		if ok && hasMeta {
			if memoryMeta, exists := s.local.Meta(id); exists {
				s.touchMemory(memoryMeta)
			}
			if announceErr := s.announce(context.Background(), meta.Seq, id, length, checksum); announceErr != nil {
				logger.Log.Warn("failed to announce promoted memory block",
					"component", "distributed_store",
					"block_id", blockID,
					"error", announceErr)
			}
			if enforceErr := s.enforceMemoryWatermark(context.Background()); enforceErr != nil {
				logger.Log.Warn("failed to enforce memory watermark after disk promotion",
					"component", "distributed_store",
					"block_id", blockID,
					"error", enforceErr)
			}
		}
		return reader, id, length, checksum, ok, err
	} else {
		logger.Log.Warn("failed to promote disk block to memory; serving from disk",
			"component", "distributed_store",
			"block_id", blockID,
			"error", err)
	}
	return s.disk.OpenBlock(blockID)
}

func (s *Store) openLocalBlock(blockID uint64) (io.ReadCloser, uint64, uint64, uint32, bool, error) {
	reader, id, length, checksum, ok, err := s.local.OpenBlock(blockID)
	if ok || err != nil {
		if ok {
			s.touchMemoryID(blockID)
		}
		return reader, id, length, checksum, ok, err
	}
	if s.disk == nil {
		return nil, 0, 0, 0, false, nil
	}
	return s.openDiskBlock(blockID)
}

func executeComputeOperator(reader io.Reader, req network.ComputeRequest) (network.ComputeResult, error) {
	switch req.Operator {
	case network.ComputeOperatorByteSum:
		if len(req.Params) != 0 {
			return network.ComputeResult{}, fmt.Errorf("distributed store: byte_sum does not accept params")
		}
		sum, err := sumBytes(reader)
		if err != nil {
			return network.ComputeResult{}, err
		}
		payload := make([]byte, 8)
		binary.LittleEndian.PutUint64(payload, sum)
		return network.ComputeResult{Payload: payload}, nil
	default:
		return network.ComputeResult{}, fmt.Errorf("distributed store: unsupported compute operator %q", req.Operator)
	}
}

func sumBytes(reader io.Reader) (uint64, error) {
	var buf [32 << 10]byte
	var sum uint64
	for {
		n, err := reader.Read(buf[:])
		for _, b := range buf[:n] {
			sum += uint64(b)
		}
		if errors.Is(err, io.EOF) {
			return sum, nil
		}
		if err != nil {
			return 0, fmt.Errorf("distributed store: compute byte_sum: %w", err)
		}
	}
}

func (s *Store) fetchAndCache(blockID uint64) error {
	if s.client == nil {
		return network.ErrBlockNotFound
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.fetchTimeout)
	defer cancel()

	locations, err := s.getLocations(ctx, blockID)
	if err != nil {
		return fmt.Errorf("distributed store: get locations for block %d: %w", blockID, err)
	}
	return s.fetchAndCacheFromLocations(ctx, blockID, locations)
}

func (s *Store) fetchAndCacheFromLocations(ctx context.Context, blockID uint64, locations []*controlplane.BlockLocation) error {
	if s == nil {
		return fmt.Errorf("distributed store: nil store")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.beginFetch(blockID) {
		return network.ErrBlockNotFound
	}
	defer s.endFetch(blockID)

	locations = sortLocations(locations)
	var lastErr error
	for _, loc := range locations {
		if s.skipLocation(loc) {
			continue
		}
		meta := loc.GetMeta()
		if meta == nil || meta.GetLength() == 0 {
			continue
		}
		if err := s.ensureMemoryHeadroom(ctx, meta.GetLength()); err != nil {
			lastErr = err
			continue
		}
		writer, err := s.local.NewImportBlockWriter(blockID, meta.GetSeq(), meta.GetLength(), meta.GetChecksum())
		if err != nil {
			return err
		}
		remote, err := s.p2p.FetchBlockTo(ctx, loc.GetAddr(), blockID, writer)
		if err != nil {
			_ = writer.Rollback()
			lastErr = err
			continue
		}
		if remote.Length != meta.GetLength() || remote.Checksum != meta.GetChecksum() {
			_ = writer.Rollback()
			lastErr = fmt.Errorf("distributed store: remote block %d metadata mismatch", blockID)
			continue
		}
		block, err := writer.Commit()
		if err != nil {
			_ = writer.Rollback()
			lastErr = err
			continue
		}
		if err := s.announce(ctx, block.Seq, block.ID, block.Length, block.Checksum); err != nil {
			logger.Log.Warn("failed to announce fetched block",
				"component", "distributed_store",
				"block_id", block.ID,
				"error", err)
		}
		s.touchMemory(storage.BlockMeta{
			ID:        block.ID,
			Seq:       block.Seq,
			Length:    block.Length,
			Allocated: block.Allocated,
			Checksum:  block.Checksum,
		})
		if s.disk != nil {
			if err := s.mirrorBlockToDisk(ctx, block.Seq, block.ID); err != nil {
				logger.Log.Warn("failed to mirror fetched block to disk tier",
					"component", "distributed_store",
					"block_id", block.ID,
					"error", err)
			}
		}
		if err := s.enforceMemoryWatermark(ctx); err != nil {
			logger.Log.Warn("failed to enforce memory watermark after fetch",
				"component", "distributed_store",
				"block_id", block.ID,
				"error", err)
		}
		return nil
	}
	if lastErr != nil {
		return fmt.Errorf("distributed store: fetch block %d: %w", blockID, lastErr)
	}
	return network.ErrBlockNotFound
}

func normalizeComputePlacement(placement ComputePlacement) ComputePlacement {
	switch placement {
	case ComputePlacementHybrid, ComputePlacementRemoteOnly, ComputePlacementFetchLocalOnly:
		return placement
	default:
		return ComputePlacementHybrid
	}
}

func (s *Store) touchMemory(meta storage.BlockMeta) {
	if s == nil || s.localLRU == nil {
		return
	}
	if meta.ID == 0 || meta.Allocated == 0 {
		return
	}
	s.localLRU.touch(meta)
}

func (s *Store) touchMemoryID(blockID uint64) {
	if s == nil || s.localLRU == nil {
		return
	}
	s.localLRU.touchID(blockID)
}

func (s *Store) ensureMemoryHeadroom(ctx context.Context, incomingBytes uint64) error {
	if s == nil || s.localLRU == nil || s.disk == nil || s.memoryHighWaterBytes == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	stats := s.local.Stats()
	if stats.PoolUsedBytes+incomingBytes <= s.effectiveMemoryHighWater(stats) {
		return nil
	}
	return s.spillColdBlocks(ctx, incomingBytes)
}

func (s *Store) enforceMemoryWatermark(ctx context.Context) error {
	if s == nil || s.localLRU == nil || s.disk == nil || s.memoryHighWaterBytes == 0 {
		return nil
	}
	stats := s.local.Stats()
	high := s.effectiveMemoryHighWater(stats)
	if stats.AllocatedBytes <= high && stats.PoolUsedBytes <= high {
		return nil
	}
	return s.spillColdBlocks(ctx, 0)
}

func (s *Store) spillColdBlocks(ctx context.Context, incomingBytes uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	var lastErr error
	busyVictims := make(map[uint64]struct{})
	for attempts := 0; attempts < s.localLRU.count(); attempts++ {
		if s.memoryWithinWatermark(incomingBytes) {
			break
		}
		victim, ok := s.localLRU.coldest()
		if !ok {
			break
		}
		if _, busy := busyVictims[victim.ID]; busy {
			break
		}
		meta, found, err := s.local.TrySpillToDisk(victim.ID, s.disk)
		switch {
		case errors.Is(err, storage.ErrBlockBusy):
			busyVictims[victim.ID] = struct{}{}
			s.localLRU.touchID(victim.ID)
			lastErr = err
			continue
		case err != nil:
			s.localLRU.remove(victim.ID)
			lastErr = err
			logger.Log.Warn("failed to spill cold block to disk",
				"component", "distributed_store",
				"block_id", victim.ID,
				"error", err)
			continue
		case !found:
			s.localLRU.remove(victim.ID)
			continue
		}

		s.localLRU.remove(meta.ID)
		if err := s.announceTier(ctx, meta.Seq, meta.ID, meta.Length, meta.Checksum, controlplane.StorageTier_STORAGE_TIER_DISK); err != nil {
			logger.Log.Warn("failed to announce spilled disk block",
				"component", "distributed_store",
				"block_id", meta.ID,
				"error", err)
		}
		if err := s.forgetTier(ctx, meta.ID, controlplane.StorageTier_STORAGE_TIER_MEMORY, "local_lru_spill"); err != nil {
			logger.Log.Warn("failed to forget spilled memory block",
				"component", "distributed_store",
				"block_id", meta.ID,
				"error", err)
		}
	}

	keepIDs := s.localLRU.blockIDs()
	if result, compacted, err := s.local.TryCompact(storage.EvictPolicyFunc(func(block storage.Block) bool {
		_, keep := keepIDs[block.ID]
		return keep
	})); err != nil {
		return err
	} else if compacted && result.Evicted > 0 {
		logger.Log.Info("compacted local memory tier",
			"component", "distributed_store",
			"before_bytes", result.BeforeBytes,
			"after_bytes", result.AfterBytes,
			"evicted", result.Evicted)
	}

	if s.memoryWithinWatermark(incomingBytes) {
		return nil
	}
	if lastErr != nil {
		return fmt.Errorf("distributed store: local memory watermark still exceeded: %w", lastErr)
	}
	return nil
}

func (s *Store) memoryWithinWatermark(incomingBytes uint64) bool {
	stats := s.local.Stats()
	high := s.effectiveMemoryHighWater(stats)
	if incomingBytes > 0 {
		return stats.PoolUsedBytes+incomingBytes <= high
	}
	target := s.effectiveMemoryLowWater(stats)
	return stats.AllocatedBytes <= target
}

func (s *Store) effectiveMemoryHighWater(stats storage.HandlerStats) uint64 {
	if s.memoryHighWaterBytes == 0 {
		return 0
	}
	if stats.PoolSizeBytes > 0 && s.memoryHighWaterBytes > stats.PoolSizeBytes {
		return stats.PoolSizeBytes
	}
	return s.memoryHighWaterBytes
}

func (s *Store) effectiveMemoryLowWater(stats storage.HandlerStats) uint64 {
	high := s.effectiveMemoryHighWater(stats)
	low := s.memoryLowWaterBytes
	if low == 0 || low >= high {
		low = high * 8 / 10
	}
	if low == high && low > 0 {
		low--
	}
	return low
}

func (s *Store) getLocations(ctx context.Context, blockID uint64) ([]*controlplane.BlockLocation, error) {
	controls := make([]ControlPlane, 0, 1+len(s.peers))
	if s.client != nil {
		controls = append(controls, s.client)
	}
	controls = append(controls, s.peers...)

	var locations []*controlplane.BlockLocation
	var lastErr error
	for _, control := range controls {
		if control == nil {
			continue
		}
		resp, err := control.GetBlockLocations(ctx, &controlplane.GetBlockLocationsRequest{
			BlockId: blockID,
		})
		if err != nil {
			lastErr = err
			continue
		}
		locations = append(locations, resp.GetLocations()...)
	}
	if len(locations) == 0 && lastErr != nil {
		return nil, lastErr
	}
	return locations, nil
}

func (s *Store) beginFetch(blockID uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.fetching[blockID]; ok {
		return false
	}
	s.fetching[blockID] = struct{}{}
	return true
}

func (s *Store) endFetch(blockID uint64) {
	s.mu.Lock()
	delete(s.fetching, blockID)
	s.mu.Unlock()
}

func (s *Store) skipLocation(loc *controlplane.BlockLocation) bool {
	if loc == nil || loc.GetAddr() == "" {
		return true
	}
	if loc.GetNodeId() == s.selfID {
		return true
	}
	if s.selfAddr != "" && loc.GetAddr() == s.selfAddr {
		return true
	}
	return false
}

func sortLocations(locations []*controlplane.BlockLocation) []*controlplane.BlockLocation {
	out := append([]*controlplane.BlockLocation(nil), locations...)
	sort.SliceStable(out, func(i, j int) bool {
		left := out[i]
		right := out[j]
		if left.GetTier() != right.GetTier() {
			return left.GetTier() == controlplane.StorageTier_STORAGE_TIER_MEMORY
		}
		return left.GetVersion() > right.GetVersion()
	})
	return out
}
