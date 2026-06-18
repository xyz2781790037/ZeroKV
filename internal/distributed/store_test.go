package distributed

import (
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"kvcache/internal/network"
	"kvcache/internal/storage"
	"kvcache/internal/transport"
	"kvcache/pkg/kvcachekey"
	"kvcache/proto/controlplane"
)

type testControlPlane struct {
	mu            sync.Mutex
	locations     map[uint64][]*controlplane.BlockLocation
	announcements []*controlplane.AnnounceBlockRequest
}

func (c *testControlPlane) AnnounceBlock(_ context.Context, req *controlplane.AnnounceBlockRequest) (*controlplane.AnnounceBlockResponse, error) {
	c.mu.Lock()
	c.announcements = append(c.announcements, req)
	c.mu.Unlock()
	return &controlplane.AnnounceBlockResponse{}, nil
}

func (c *testControlPlane) ForgetBlock(context.Context, *controlplane.ForgetBlockRequest) (*controlplane.ForgetBlockResponse, error) {
	return &controlplane.ForgetBlockResponse{}, nil
}

func (c *testControlPlane) GetBlockLocations(_ context.Context, req *controlplane.GetBlockLocationsRequest) (*controlplane.GetBlockLocationsResponse, error) {
	c.mu.Lock()
	locations := append([]*controlplane.BlockLocation(nil), c.locations[req.GetBlockId()]...)
	c.mu.Unlock()
	return &controlplane.GetBlockLocationsResponse{
		BlockId:   req.GetBlockId(),
		Locations: locations,
	}, nil
}

func (c *testControlPlane) announcementCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.announcements)
}

type testBlockTransport struct {
	mu      sync.Mutex
	name    string
	payload []byte
	partial []byte
	err     error
	calls   int
	target  transport.Target

	started   chan struct{}
	startOnce sync.Once
	release   <-chan struct{}
}

func (t *testBlockTransport) Name() string {
	if t.name == "" {
		return "test_rdma"
	}
	return t.name
}

func (t *testBlockTransport) FetchBlockTo(ctx context.Context, target transport.Target, dst io.Writer) (transport.BlockMetadata, error) {
	t.mu.Lock()
	t.calls++
	t.target = target
	t.mu.Unlock()
	if t.started != nil {
		t.startOnce.Do(func() { close(t.started) })
	}
	if t.release != nil {
		select {
		case <-t.release:
		case <-ctx.Done():
			return transport.BlockMetadata{}, ctx.Err()
		}
	}
	if len(t.partial) > 0 {
		if _, err := dst.Write(t.partial); err != nil {
			return transport.BlockMetadata{}, err
		}
	}
	if t.err != nil {
		return transport.BlockMetadata{}, t.err
	}
	if _, err := dst.Write(t.payload); err != nil {
		return transport.BlockMetadata{}, err
	}
	return transport.BlockMetadata{
		ID:       target.BlockID,
		Length:   uint64(len(t.payload)),
		Checksum: crc32.ChecksumIEEE(t.payload),
	}, nil
}

func (t *testBlockTransport) callCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls
}

func (t *testBlockTransport) lastTarget() transport.Target {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.target
}

func newDistributedTestStore(t *testing.T, control ControlPlane, primary transport.BlockTransport) (*Store, *storage.Handler) {
	t.Helper()
	pool, err := storage.NewOffheapPool(8 << 20)
	if err != nil {
		t.Fatalf("storage.NewOffheapPool() error = %v", err)
	}
	handler, err := storage.NewHandler(pool)
	if err != nil {
		_ = pool.Release()
		t.Fatalf("storage.NewHandler() error = %v", err)
	}
	t.Cleanup(func() {
		if err := handler.Release(); err != nil {
			t.Errorf("Handler.Release() error = %v", err)
		}
	})
	store, err := NewStore(handler, control, StoreOptions{
		SelfID:           "node-a",
		SelfAddr:         "127.0.0.1:1",
		PrimaryTransport: primary,
		ComputePlacement: ComputePlacementFetchLocalOnly,
	})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	return store, handler
}

func remoteLocation(blockID uint64, address string, payload []byte) *controlplane.BlockLocation {
	return &controlplane.BlockLocation{
		BlockId: blockID,
		NodeId:  "node-b",
		Addr:    address,
		Tier:    controlplane.StorageTier_STORAGE_TIER_MEMORY,
		Meta: &controlplane.BlockMeta{
			BlockId:  blockID,
			Length:   uint64(len(payload)),
			Checksum: crc32.ChecksumIEEE(payload),
			Seq:      1,
		},
	}
}

func readStoreBlock(t *testing.T, store *Store, blockID uint64) []byte {
	t.Helper()
	reader, id, length, checksum, ok, err := store.OpenBlock(blockID)
	if err != nil || !ok {
		t.Fatalf("OpenBlock(%d) ok=%v error=%v", blockID, ok, err)
	}
	payload, readErr := io.ReadAll(reader)
	if closeErr := reader.Close(); readErr == nil {
		readErr = closeErr
	}
	if readErr != nil {
		t.Fatalf("read OpenBlock(%d) error = %v", blockID, readErr)
	}
	if id != blockID || length != uint64(len(payload)) || checksum != crc32.ChecksumIEEE(payload) {
		t.Fatalf("OpenBlock(%d) metadata id=%d length=%d checksum=%d", blockID, id, length, checksum)
	}
	return payload
}

func TestStoreRemoteFetchCachesReplica(t *testing.T) {
	const blockID = uint64(701)
	payload := bytes.Repeat([]byte("kv-cache"), 512)
	control := &testControlPlane{locations: map[uint64][]*controlplane.BlockLocation{
		blockID: {remoteLocation(blockID, "rdma://node-b", payload)},
	}}
	primary := &testBlockTransport{name: "rdma", payload: payload}
	store, handler := newDistributedTestStore(t, control, primary)

	if got := readStoreBlock(t, store, blockID); !bytes.Equal(got, payload) {
		t.Fatal("first OpenBlock() returned wrong payload")
	}
	if got := readStoreBlock(t, store, blockID); !bytes.Equal(got, payload) {
		t.Fatal("cached OpenBlock() returned wrong payload")
	}
	if primary.callCount() != 1 {
		t.Fatalf("remote transport calls = %d, want 1", primary.callCount())
	}
	if _, ok := handler.Meta(blockID); !ok {
		t.Fatal("remote block was not cached locally")
	}
	if control.announcementCount() != 1 {
		t.Fatalf("control-plane announcements = %d, want 1", control.announcementCount())
	}
}

func prefixTestChunks(t *testing.T, tokens []uint32) []kvcachekey.ChunkKey {
	t.Helper()
	chunks, err := kvcachekey.Build(kvcachekey.Scope{
		Version:       kvcachekey.SchemaVersion,
		Namespace:     "prefix-test",
		ModelID:       "test-model",
		ModelRevision: "revision-1",
		ChunkSize:     4,
		Layout: kvcachekey.Layout{
			Version:     1,
			DType:       "fp16",
			Layers:      2,
			Heads:       2,
			HeadDim:     4,
			TPWorldSize: 1,
		},
	}, tokens)
	if err != nil {
		t.Fatalf("kvcachekey.Build() error = %v", err)
	}
	return chunks
}

func prefixRequest(chunks []kvcachekey.ChunkKey) network.PrefixLookupRequest {
	req := network.PrefixLookupRequest{ScopeDigest: network.CacheDigest(chunks[0].Key.ScopeDigest)}
	for _, chunk := range chunks {
		req.Candidates = append(req.Candidates, network.PrefixCandidate{
			ObjectID: network.CacheDigest(chunk.ObjectID),
			TokenEnd: chunk.TokenEnd,
		})
	}
	return req
}

func commitPrefixChunk(t *testing.T, store *Store, chunk kvcachekey.ChunkKey, seq uint64, payload []byte) {
	t.Helper()
	checksum := crc32.ChecksumIEEE(payload)
	if err := store.IngestBlock(context.Background(), seq, chunk.BlockID, uint64(len(payload)), checksum, payload); err != nil {
		t.Fatalf("IngestBlock(%d) error = %v", chunk.BlockID, err)
	}
	if err := store.CommitCacheObject(context.Background(), network.CacheObjectCommit{
		ScopeDigest:  network.CacheDigest(chunk.Key.ScopeDigest),
		PrefixDigest: network.CacheDigest(chunk.Key.PrefixDigest),
		ObjectID:     network.CacheDigest(chunk.ObjectID),
		TokenCount:   chunk.TokenEnd,
		BlockID:      chunk.BlockID,
		Length:       uint64(len(payload)),
		Checksum:     checksum,
	}); err != nil {
		t.Fatalf("CommitCacheObject(%d) error = %v", chunk.BlockID, err)
	}
}

func TestStorePrefixLookupReturnsContiguousLease(t *testing.T) {
	store, _ := newDistributedTestStore(t, &testControlPlane{}, nil)
	chunks := prefixTestChunks(t, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	for index := 0; index < 2; index++ {
		commitPrefixChunk(t, store, chunks[index], uint64(index+1), bytes.Repeat([]byte{byte(index + 1)}, 256))
	}

	result, err := store.LookupPrefix(context.Background(), prefixRequest(chunks))
	if err != nil {
		t.Fatalf("LookupPrefix() error = %v", err)
	}
	if len(result.Entries) != 2 || result.MatchedTokens != 8 || result.LeaseID == 0 || result.StopReason != network.PrefixStopNotFound {
		t.Fatalf("LookupPrefix() = %+v, want two-block prefix ending at token 8", result)
	}
	if err := store.DeleteBlock(context.Background(), chunks[0].BlockID); !errors.Is(err, ErrCacheObjectBusy) {
		t.Fatalf("DeleteBlock() while leased error = %v, want ErrCacheObjectBusy", err)
	}
	if err := store.ReleasePrefixLease(context.Background(), result.LeaseID); err != nil {
		t.Fatalf("ReleasePrefixLease() error = %v", err)
	}
	if err := store.DeleteBlock(context.Background(), chunks[0].BlockID); err != nil {
		t.Fatalf("DeleteBlock() after release error = %v", err)
	}
}

func TestStorePrefixLookupFiltersRemoteObjectID(t *testing.T) {
	chunks := prefixTestChunks(t, []uint32{1, 2, 3, 4})
	chunk := chunks[0]
	payload := bytes.Repeat([]byte("remote-semantic-kv"), 32)
	correct := remoteLocation(chunk.BlockID, "rdma://correct", payload)
	correct.NodeId = "node-correct"
	correct.Meta.ObjectId = append([]byte(nil), chunk.ObjectID[:]...)
	wrong := remoteLocation(chunk.BlockID, "rdma://wrong", payload)
	wrong.NodeId = "node-wrong"
	wrong.Meta.ObjectId = bytes.Repeat([]byte{0xff}, 32)
	control := &testControlPlane{locations: map[uint64][]*controlplane.BlockLocation{
		chunk.BlockID: {wrong, correct},
	}}
	primary := &testBlockTransport{name: "rdma", payload: payload}
	store, _ := newDistributedTestStore(t, control, primary)

	result, err := store.LookupPrefix(context.Background(), prefixRequest(chunks))
	if err != nil {
		t.Fatalf("LookupPrefix() error = %v", err)
	}
	if len(result.Entries) != 1 || result.MatchedTokens != chunk.TokenEnd {
		t.Fatalf("LookupPrefix() = %+v", result)
	}
	if target := primary.lastTarget(); target.NodeID != "node-correct" || target.Address != "rdma://correct" {
		t.Fatalf("selected target = %+v, want full ObjectID match", target)
	}
	_ = store.ReleasePrefixLease(context.Background(), result.LeaseID)
}

func TestStoreRestoresCacheManifestFromDisk(t *testing.T) {
	root := t.TempDir()
	disk, err := storage.NewDiskTier(root)
	if err != nil {
		t.Fatalf("storage.NewDiskTier() error = %v", err)
	}
	newHandler := func() *storage.Handler {
		pool, poolErr := storage.NewOffheapPool(8 << 20)
		if poolErr != nil {
			t.Fatalf("storage.NewOffheapPool() error = %v", poolErr)
		}
		handler, handlerErr := storage.NewHandler(pool)
		if handlerErr != nil {
			_ = pool.Release()
			t.Fatalf("storage.NewHandler() error = %v", handlerErr)
		}
		return handler
	}
	firstHandler := newHandler()
	first, err := NewStore(firstHandler, nil, StoreOptions{SelfID: "node-a", SelfAddr: "127.0.0.1:1", DiskTier: disk})
	if err != nil {
		t.Fatalf("NewStore(first) error = %v", err)
	}
	chunks := prefixTestChunks(t, []uint32{1, 2, 3, 4})
	payload := bytes.Repeat([]byte("persistent-kv"), 32)
	commitPrefixChunk(t, first, chunks[0], 1, payload)
	if err := firstHandler.Release(); err != nil {
		t.Fatalf("first Handler.Release() error = %v", err)
	}

	reopenedDisk, err := storage.NewDiskTier(root)
	if err != nil {
		t.Fatalf("storage.NewDiskTier(reopen) error = %v", err)
	}
	secondHandler := newHandler()
	defer func() {
		if err := secondHandler.Release(); err != nil {
			t.Errorf("second Handler.Release() error = %v", err)
		}
	}()
	second, err := NewStore(secondHandler, nil, StoreOptions{SelfID: "node-a", SelfAddr: "127.0.0.1:1", DiskTier: reopenedDisk})
	if err != nil {
		t.Fatalf("NewStore(second) error = %v", err)
	}
	result, err := second.LookupPrefix(context.Background(), prefixRequest(chunks))
	if err != nil {
		t.Fatalf("LookupPrefix() after restart error = %v", err)
	}
	if len(result.Entries) != 1 || result.MatchedTokens != 4 || result.Entries[0].Tier != network.CacheTierDisk {
		t.Fatalf("LookupPrefix() after restart = %+v", result)
	}
	_ = second.ReleasePrefixLease(context.Background(), result.LeaseID)
}

type staticNetworkStore struct {
	blockID uint64
	payload []byte
}

func (s staticNetworkStore) OpenBlock(blockID uint64) (io.ReadCloser, uint64, uint64, uint32, bool, error) {
	if blockID != s.blockID {
		return nil, 0, 0, 0, false, nil
	}
	return io.NopCloser(bytes.NewReader(s.payload)), blockID, uint64(len(s.payload)), crc32.ChecksumIEEE(s.payload), true, nil
}

func startTestP2PServer(t *testing.T, blockID uint64, payload []byte) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	server := network.NewServer("", staticNetworkStore{blockID: blockID, payload: payload})
	server.HeaderTimeout = time.Second
	server.PayloadBaseTimeout = time.Second
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx, listener)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("network.Server.Serve() error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("network.Server.Serve() did not stop")
		}
	})
	return listener.Addr().String()
}

func TestStoreFallsBackFromRDMAToP2P(t *testing.T) {
	const blockID = uint64(702)
	payload := []byte("p2p-fallback-kv-payload")
	address := startTestP2PServer(t, blockID, payload)
	control := &testControlPlane{locations: map[uint64][]*controlplane.BlockLocation{
		blockID: {remoteLocation(blockID, address, payload)},
	}}
	primary := &testBlockTransport{
		name:    "rdma",
		partial: []byte("partial-rdma"),
		err:     errors.New("simulated RDMA failure"),
	}
	store, _ := newDistributedTestStore(t, control, primary)

	if got := readStoreBlock(t, store, blockID); !bytes.Equal(got, payload) {
		t.Fatalf("fallback payload = %q, want %q", got, payload)
	}
	if primary.callCount() != 1 {
		t.Fatalf("RDMA attempts = %d, want 1", primary.callCount())
	}
}

func TestStoreConcurrentMissUsesSingleRemoteFetch(t *testing.T) {
	const (
		blockID  = uint64(703)
		requests = 16
	)
	payload := bytes.Repeat([]byte{0x7a}, 64<<10)
	control := &testControlPlane{locations: map[uint64][]*controlplane.BlockLocation{
		blockID: {remoteLocation(blockID, "rdma://node-b", payload)},
	}}
	started := make(chan struct{})
	release := make(chan struct{})
	primary := &testBlockTransport{
		name:    "rdma",
		payload: payload,
		started: started,
		release: release,
	}
	store, _ := newDistributedTestStore(t, control, primary)

	start := make(chan struct{})
	errs := make(chan error, requests)
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			reader, _, _, _, ok, err := store.OpenBlock(blockID)
			if err != nil {
				errs <- err
				return
			}
			if !ok {
				errs <- network.ErrBlockNotFound
				return
			}
			got, err := io.ReadAll(reader)
			_ = reader.Close()
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, payload) {
				errs <- errors.New("concurrent reader received wrong payload")
			}
		}()
	}
	close(start)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("remote fetch did not start")
	}
	time.Sleep(25 * time.Millisecond)
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent OpenBlock() error = %v", err)
	}
	if primary.callCount() != 1 {
		t.Fatalf("remote transport calls = %d, want 1", primary.callCount())
	}
}
