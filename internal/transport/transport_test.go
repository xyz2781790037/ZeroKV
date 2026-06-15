package transport

import (
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"io"
	"net"
	"testing"
	"time"

	"kvcache/internal/network"
)

type scriptedTransport struct {
	name    string
	payload []byte
	meta    BlockMetadata
	err     error
	calls   int
}

func (t *scriptedTransport) Name() string {
	return t.name
}

func (t *scriptedTransport) FetchBlockTo(_ context.Context, _ Target, dst io.Writer) (BlockMetadata, error) {
	t.calls++
	if len(t.payload) > 0 {
		if _, err := dst.Write(t.payload); err != nil {
			return BlockMetadata{}, err
		}
	}
	return t.meta, t.err
}

type rollbackBuffer struct {
	bytes.Buffer
	rollbacks int
}

func (b *rollbackBuffer) Rollback() error {
	b.rollbacks++
	b.Reset()
	return nil
}

type writeOnlyBuffer struct {
	data []byte
}

func (b *writeOnlyBuffer) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

func TestFailoverPrimarySuccess(t *testing.T) {
	target := Target{NodeID: "node-b", Address: "unused", BlockID: 41}
	primary := &scriptedTransport{
		name:    "rdma",
		payload: []byte("primary-payload"),
		meta:    BlockMetadata{ID: target.BlockID, Length: 15},
	}
	fallback := &scriptedTransport{
		name: "p2p_tcp",
		err:  errors.New("fallback should not run"),
	}
	dst := &rollbackBuffer{}

	meta, err := NewFailover(primary, fallback).FetchBlockTo(context.Background(), target, dst)
	if err != nil {
		t.Fatalf("FetchBlockTo() error = %v", err)
	}
	if meta.ID != target.BlockID {
		t.Fatalf("FetchBlockTo() block id = %d, want %d", meta.ID, target.BlockID)
	}
	if got, want := dst.String(), "primary-payload"; got != want {
		t.Fatalf("destination = %q, want %q", got, want)
	}
	if primary.calls != 1 || fallback.calls != 0 {
		t.Fatalf("transport calls primary=%d fallback=%d, want 1/0", primary.calls, fallback.calls)
	}
	if dst.rollbacks != 0 {
		t.Fatalf("rollback count = %d, want 0", dst.rollbacks)
	}
}

func TestFailoverRollsBackPartialPrimaryWrite(t *testing.T) {
	target := Target{NodeID: "node-b", Address: "unused", BlockID: 42}
	primaryErr := errors.New("simulated RDMA completion failure")
	primary := &scriptedTransport{
		name:    "rdma",
		payload: []byte("partial-"),
		err:     primaryErr,
	}
	fallbackPayload := []byte("complete-p2p-payload")
	fallback := &scriptedTransport{
		name:    "p2p_tcp",
		payload: fallbackPayload,
		meta: BlockMetadata{
			ID:       target.BlockID,
			Length:   uint64(len(fallbackPayload)),
			Checksum: crc32.ChecksumIEEE(fallbackPayload),
		},
	}
	dst := &rollbackBuffer{}
	var fallbackTarget Target
	var fallbackErr error
	failover := NewFailoverWithHandler(primary, fallback, func(_ string, target Target, err error) {
		fallbackTarget = target
		fallbackErr = err
	})

	meta, err := failover.FetchBlockTo(context.Background(), target, dst)
	if err != nil {
		t.Fatalf("FetchBlockTo() error = %v", err)
	}
	if meta.ID != target.BlockID {
		t.Fatalf("FetchBlockTo() block id = %d, want %d", meta.ID, target.BlockID)
	}
	if !bytes.Equal(dst.Bytes(), fallbackPayload) {
		t.Fatalf("destination = %q, want %q", dst.Bytes(), fallbackPayload)
	}
	if dst.rollbacks != 1 {
		t.Fatalf("rollback count = %d, want 1", dst.rollbacks)
	}
	if fallbackTarget != target || !errors.Is(fallbackErr, primaryErr) {
		t.Fatalf("fallback callback target=%+v err=%v", fallbackTarget, fallbackErr)
	}
}

func TestFailoverRejectsWrongPrimaryBlock(t *testing.T) {
	target := Target{NodeID: "node-b", Address: "unused", BlockID: 43}
	primary := &scriptedTransport{
		name:    "rdma",
		payload: []byte("wrong-block"),
		meta:    BlockMetadata{ID: target.BlockID + 1},
	}
	fallback := &scriptedTransport{
		name:    "p2p_tcp",
		payload: []byte("correct-block"),
		meta:    BlockMetadata{ID: target.BlockID},
	}
	dst := &rollbackBuffer{}

	meta, err := NewFailover(primary, fallback).FetchBlockTo(context.Background(), target, dst)
	if err != nil {
		t.Fatalf("FetchBlockTo() error = %v", err)
	}
	if meta.ID != target.BlockID || dst.String() != "correct-block" {
		t.Fatalf("result meta=%+v destination=%q", meta, dst.String())
	}
	if dst.rollbacks != 1 || fallback.calls != 1 {
		t.Fatalf("rollback count=%d fallback calls=%d, want 1/1", dst.rollbacks, fallback.calls)
	}
}

func TestFailoverRequiresResettableDestination(t *testing.T) {
	target := Target{BlockID: 44}
	primary := &scriptedTransport{
		name:    "rdma",
		payload: []byte("partial"),
		err:     errors.New("simulated RDMA failure"),
	}
	fallback := &scriptedTransport{
		name: "p2p_tcp",
		meta: BlockMetadata{ID: target.BlockID},
	}
	dst := &writeOnlyBuffer{}

	_, err := NewFailover(primary, fallback).FetchBlockTo(context.Background(), target, dst)
	if !errors.Is(err, ErrDestinationNotResettable) {
		t.Fatalf("FetchBlockTo() error = %v, want ErrDestinationNotResettable", err)
	}
	if fallback.calls != 0 {
		t.Fatalf("fallback calls = %d, want 0", fallback.calls)
	}
}

func TestRDMATransportWithoutBackendIsUnavailable(t *testing.T) {
	_, err := NewRDMA(nil).FetchBlockTo(context.Background(), Target{BlockID: 45}, &bytes.Buffer{})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("FetchBlockTo() error = %v, want ErrUnavailable", err)
	}
}

type staticBlockStore struct {
	blockID  uint64
	payload  []byte
	checksum uint32
}

func (s staticBlockStore) OpenBlock(blockID uint64) (io.ReadCloser, uint64, uint64, uint32, bool, error) {
	if blockID != s.blockID {
		return nil, 0, 0, 0, false, nil
	}
	return io.NopCloser(bytes.NewReader(s.payload)), s.blockID, uint64(len(s.payload)), s.checksum, true, nil
}

func TestP2PTransportFetchesBlockOverLoopback(t *testing.T) {
	payload := []byte("local-loopback-kv-block")
	store := staticBlockStore{
		blockID:  9001,
		payload:  payload,
		checksum: crc32.ChecksumIEEE(payload),
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	server := network.NewServer("", store)
	server.HeaderTimeout = time.Second
	server.PayloadBaseTimeout = time.Second
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Serve(ctx, listener)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-serverDone:
			if err != nil {
				t.Errorf("server.Serve() error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("server.Serve() did not stop after context cancellation")
		}
	})

	client := network.NewClient()
	client.HeaderTimeout = time.Second
	client.PayloadBaseTimeout = time.Second
	var dst bytes.Buffer
	meta, err := NewP2P(client).FetchBlockTo(ctx, Target{
		NodeID:  "node-b",
		Address: listener.Addr().String(),
		BlockID: store.blockID,
	}, &dst)
	if err != nil {
		t.Fatalf("FetchBlockTo() error = %v", err)
	}
	if meta.ID != store.blockID || meta.Length != uint64(len(payload)) || meta.Checksum != store.checksum {
		t.Fatalf("FetchBlockTo() metadata = %+v", meta)
	}
	if !bytes.Equal(dst.Bytes(), payload) {
		t.Fatalf("destination = %q, want %q", dst.Bytes(), payload)
	}
}

func BenchmarkP2PTransportFetch(b *testing.B) {
	benchmarks := []struct {
		name string
		size int
	}{
		{name: "64KiB", size: 64 << 10},
		{name: "1MiB", size: 1 << 20},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			payload := bytes.Repeat([]byte{0x6b}, benchmark.size)
			store := staticBlockStore{
				blockID:  9101,
				payload:  payload,
				checksum: crc32.ChecksumIEEE(payload),
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatalf("net.Listen() error = %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			server := network.NewServer("", store)
			serverDone := make(chan error, 1)
			go func() {
				serverDone <- server.Serve(ctx, listener)
			}()
			b.Cleanup(func() {
				cancel()
				select {
				case err := <-serverDone:
					if err != nil {
						b.Errorf("server.Serve() error = %v", err)
					}
				case <-time.After(2 * time.Second):
					b.Error("server.Serve() did not stop")
				}
			})

			client := network.NewClient()
			blockTransport := NewP2P(client)
			target := Target{NodeID: "node-b", Address: listener.Addr().String(), BlockID: store.blockID}
			var dst bytes.Buffer
			dst.Grow(benchmark.size)
			b.ReportAllocs()
			b.SetBytes(int64(benchmark.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst.Reset()
				if _, err := blockTransport.FetchBlockTo(ctx, target, &dst); err != nil {
					b.Fatalf("FetchBlockTo() error = %v", err)
				}
			}
		})
	}
}
