package storage

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
	"testing"
)

func newTestHandler(tb testing.TB, size uint64) *Handler {
	tb.Helper()
	pool, err := NewOffheapPool(size)
	if err != nil {
		tb.Fatalf("NewOffheapPool() error = %v", err)
	}
	handler, err := NewHandler(pool)
	if err != nil {
		_ = pool.Release()
		tb.Fatalf("NewHandler() error = %v", err)
	}
	tb.Cleanup(func() {
		if err := handler.Release(); err != nil {
			tb.Errorf("Handler.Release() error = %v", err)
		}
	})
	return handler
}

func importTestBlock(t *testing.T, handler *Handler, blockID, seq uint64, payload []byte) Block {
	t.Helper()
	block, err := handler.ImportBlock(blockID, seq, uint64(len(payload)), crc32.ChecksumIEEE(payload), payload)
	if err != nil {
		t.Fatalf("ImportBlock(%d) error = %v", blockID, err)
	}
	return block
}

func TestHandlerBlockLifecycle(t *testing.T) {
	handler := newTestHandler(t, 1<<20)
	payload := []byte("kv-cache-block-payload")
	checksum := crc32.ChecksumIEEE(payload)
	block := importTestBlock(t, handler, 101, 7, payload)

	if block.ID != 101 || block.Seq != 7 || block.Length != uint64(len(payload)) || block.Checksum != checksum {
		t.Fatalf("ImportBlock() block = %+v", block)
	}
	stats := handler.Stats()
	if stats.Blocks != 1 || stats.LogicalBytes != uint64(len(payload)) {
		t.Fatalf("Stats() = %+v", stats)
	}

	got, ok := handler.Get(block.ID)
	if !ok || !bytes.Equal(got, payload) {
		t.Fatalf("Get(%d) = %q, %v", block.ID, got, ok)
	}
	got[0] ^= 0xff
	again, ok := handler.Get(block.ID)
	if !ok || !bytes.Equal(again, payload) {
		t.Fatal("Get() exposed mutable offheap storage")
	}

	reader, id, length, gotChecksum, ok, err := handler.OpenBlock(block.ID)
	if err != nil || !ok {
		t.Fatalf("OpenBlock(%d) ok=%v error=%v", block.ID, ok, err)
	}
	streamed, err := io.ReadAll(reader)
	if closeErr := reader.Close(); err == nil {
		err = closeErr
	}
	if err != nil || id != block.ID || length != block.Length || gotChecksum != checksum || !bytes.Equal(streamed, payload) {
		t.Fatalf("OpenBlock() id=%d length=%d checksum=%d payload=%q error=%v", id, length, gotChecksum, streamed, err)
	}

	usedBeforeDuplicate := handler.Stats().PoolUsedBytes
	duplicate, err := handler.ImportBlock(block.ID, block.Seq+1, block.Length, block.Checksum, payload)
	if err != nil {
		t.Fatalf("duplicate ImportBlock() error = %v", err)
	}
	if duplicate.ID != block.ID || handler.Stats().PoolUsedBytes != usedBeforeDuplicate {
		t.Fatal("duplicate import allocated a second physical block")
	}

	meta, found, err := handler.DeleteBlock(block.ID)
	if err != nil || !found || meta.ID != block.ID {
		t.Fatalf("DeleteBlock() meta=%+v found=%v error=%v", meta, found, err)
	}
	if _, ok := handler.Get(block.ID); ok || handler.Len() != 0 {
		t.Fatal("deleted block is still visible")
	}
}

func TestHandlerRejectsInvalidImport(t *testing.T) {
	handler := newTestHandler(t, 1<<20)
	payload := []byte("checksum-protected-block")

	if _, err := handler.ImportBlock(201, 1, uint64(len(payload)+1), crc32.ChecksumIEEE(payload), payload); err == nil {
		t.Fatal("ImportBlock() accepted mismatched length")
	}
	if _, err := handler.ImportBlock(202, 1, uint64(len(payload)), crc32.ChecksumIEEE(payload)+1, payload); err == nil {
		t.Fatal("ImportBlock() accepted mismatched checksum")
	}
	stats := handler.Stats()
	if stats.Blocks != 0 || stats.PoolUsedBytes != 0 {
		t.Fatalf("invalid imports changed storage stats: %+v", stats)
	}
}

func TestHandlerLeaseBlocksDeleteAndCompaction(t *testing.T) {
	handler := newTestHandler(t, 1<<20)
	block := importTestBlock(t, handler, 301, 1, []byte("leased-kv-block"))
	lease, ok := handler.Acquire(block.ID)
	if !ok {
		t.Fatal("Acquire() did not find imported block")
	}

	if _, found, err := handler.DeleteBlock(block.ID); !found || !errors.Is(err, ErrBlockBusy) {
		t.Fatalf("DeleteBlock() found=%v error=%v, want ErrBlockBusy", found, err)
	}
	if _, compacted, err := handler.TryCompact(NewEvictBlockIDsPolicy(block.ID)); err != nil || compacted {
		t.Fatalf("TryCompact() compacted=%v error=%v while lease active", compacted, err)
	}

	lease.Release()
	if _, found, err := handler.DeleteBlock(block.ID); err != nil || !found {
		t.Fatalf("DeleteBlock() after release found=%v error=%v", found, err)
	}
}

func TestHandlerCompactReclaimsEvictedBlocks(t *testing.T) {
	handler := newTestHandler(t, 1<<20)
	first := importTestBlock(t, handler, 401, 1, bytes.Repeat([]byte{0x11}, 4097))
	secondPayload := bytes.Repeat([]byte{0x22}, 8193)
	second := importTestBlock(t, handler, 402, 2, secondPayload)
	usedBefore := handler.Stats().PoolUsedBytes

	result, err := handler.Compact(NewEvictBlockIDsPolicy(first.ID))
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if result.BeforeBlocks != 2 || result.AfterBlocks != 1 || result.Evicted != 1 {
		t.Fatalf("Compact() result = %+v", result)
	}
	if _, ok := handler.Get(first.ID); ok {
		t.Fatal("compacted block is still visible")
	}
	got, ok := handler.Get(second.ID)
	if !ok || !bytes.Equal(got, secondPayload) {
		t.Fatal("Compact() corrupted retained block")
	}
	if usedAfter := handler.Stats().PoolUsedBytes; usedAfter >= usedBefore {
		t.Fatalf("Compact() pool used bytes = %d, want less than %d", usedAfter, usedBefore)
	}
}

var benchmarkByte byte

func BenchmarkHandlerLeaseAcquire(b *testing.B) {
	for _, size := range []int{16 << 10, 1 << 20} {
		b.Run(byteSizeLabel(size), func(b *testing.B) {
			handler := newTestHandler(b, uint64(size*2))
			payload := bytes.Repeat([]byte{0x5a}, size)
			if _, err := handler.ImportBlock(501, 1, uint64(size), crc32.ChecksumIEEE(payload), payload); err != nil {
				b.Fatalf("ImportBlock() error = %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				lease, ok := handler.Acquire(501)
				if !ok {
					b.Fatal("Acquire() missed benchmark block")
				}
				data := lease.Data()
				benchmarkByte ^= data[i%len(data)]
				lease.Release()
			}
		})
	}
}

func BenchmarkHandlerStreamRead(b *testing.B) {
	for _, size := range []int{16 << 10, 1 << 20} {
		b.Run(byteSizeLabel(size), func(b *testing.B) {
			handler := newTestHandler(b, uint64(size*2))
			payload := bytes.Repeat([]byte{0x5a}, size)
			if _, err := handler.ImportBlock(502, 1, uint64(size), crc32.ChecksumIEEE(payload), payload); err != nil {
				b.Fatalf("ImportBlock() error = %v", err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader, _, _, _, ok, err := handler.OpenBlock(502)
				if err != nil || !ok {
					b.Fatalf("OpenBlock() ok=%v error=%v", ok, err)
				}
				if _, err := io.Copy(io.Discard, reader); err != nil {
					_ = reader.Close()
					b.Fatalf("io.Copy() error = %v", err)
				}
				if err := reader.Close(); err != nil {
					b.Fatalf("reader.Close() error = %v", err)
				}
			}
		})
	}
}

func byteSizeLabel(size int) string {
	if size%(1<<20) == 0 {
		return "1MiB"
	}
	return "16KiB"
}
