package storage

import (
	"bytes"
	"hash/crc32"
	"os"
	"testing"
)

func TestDiskTierPersistsAcrossRestart(t *testing.T) {
	root := t.TempDir()
	payload := bytes.Repeat([]byte("kv"), 4096)
	block := Block{
		ID:       601,
		Seq:      9,
		Length:   uint64(len(payload)),
		Checksum: crc32.ChecksumIEEE(payload),
		Data:     payload,
	}

	tier, err := NewDiskTier(root)
	if err != nil {
		t.Fatalf("NewDiskTier() error = %v", err)
	}
	if err := tier.Put(block); err != nil {
		t.Fatalf("DiskTier.Put() error = %v", err)
	}
	got, ok, err := tier.Get(block.ID)
	if err != nil || !ok || !bytes.Equal(got, payload) {
		t.Fatalf("DiskTier.Get() ok=%v payload=%d bytes error=%v", ok, len(got), err)
	}

	reopened, err := NewDiskTier(root)
	if err != nil {
		t.Fatalf("NewDiskTier() after restart error = %v", err)
	}
	got, ok, err = reopened.Get(block.ID)
	if err != nil || !ok || !bytes.Equal(got, payload) {
		t.Fatalf("reopened DiskTier.Get() ok=%v payload=%d bytes error=%v", ok, len(got), err)
	}
	if meta, ok := reopened.Meta(block.ID); !ok || meta.Seq != block.Seq || meta.Checksum != block.Checksum {
		t.Fatalf("reopened DiskTier.Meta() = %+v, %v", meta, ok)
	}

	if err := reopened.Delete(block.ID); err != nil {
		t.Fatalf("DiskTier.Delete() error = %v", err)
	}
	if reopened.Has(block.ID) {
		t.Fatal("deleted disk block is still indexed")
	}
}

func TestDiskTierDetectsPayloadCorruption(t *testing.T) {
	tier, err := NewDiskTier(t.TempDir())
	if err != nil {
		t.Fatalf("NewDiskTier() error = %v", err)
	}
	payload := []byte("disk-checksum-protected-kv-block")
	block := Block{
		ID:       602,
		Seq:      1,
		Length:   uint64(len(payload)),
		Checksum: crc32.ChecksumIEEE(payload),
		Data:     payload,
	}
	if err := tier.Put(block); err != nil {
		t.Fatalf("DiskTier.Put() error = %v", err)
	}
	meta, ok := tier.Meta(block.ID)
	if !ok {
		t.Fatal("DiskTier.Meta() did not find block")
	}
	file, err := os.OpenFile(meta.Path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	if _, err := file.WriteAt([]byte{payload[0] ^ 0xff}, int64(diskBlockHeader)); err != nil {
		_ = file.Close()
		t.Fatalf("WriteAt() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, ok, err := tier.Get(block.ID); err == nil || ok {
		t.Fatalf("DiskTier.Get() ok=%v error=%v, want corruption error", ok, err)
	}
}
