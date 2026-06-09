package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	cacheManifestMagic      uint32 = 0x4d564b5a // "ZKVM" in little endian.
	cacheManifestVersion    uint16 = 1
	cacheManifestHeaderSize        = 140
)

// CacheObjectManifest is the durable semantic-to-physical commit record. The
// current storage object contains one complete, block-aligned KV chunk; the
// manifest boundary allows a future object to contain multiple physical shards.
type CacheObjectManifest struct {
	ScopeDigest  [32]byte
	PrefixDigest [32]byte
	ObjectID     [32]byte
	TokenCount   uint64
	BlockID      uint64
	Length       uint64
	Checksum     uint32
}

func (d *DiskTier) PutCacheManifest(manifest CacheObjectManifest) error {
	if d == nil {
		return fmt.Errorf("disk tier: nil tier")
	}
	if err := validateCacheManifest(manifest); err != nil {
		return err
	}
	d.manifestMu.Lock()
	defer d.manifestMu.Unlock()
	if meta, ok := d.Meta(manifest.BlockID); !ok {
		return fmt.Errorf("disk tier: manifest block %d is not present", manifest.BlockID)
	} else if meta.Length != manifest.Length || meta.Checksum != manifest.Checksum {
		return fmt.Errorf("disk tier: manifest block %d metadata mismatch", manifest.BlockID)
	}

	d.mu.RLock()
	if existing, ok := d.manifests[manifest.ObjectID]; ok {
		d.mu.RUnlock()
		if existing == manifest {
			return nil
		}
		return fmt.Errorf("disk tier: conflicting manifest for object %x", manifest.ObjectID)
	}
	if existingObject, ok := d.manifestByBlock[manifest.BlockID]; ok && existingObject != manifest.ObjectID {
		d.mu.RUnlock()
		return fmt.Errorf("disk tier: block %d already belongs to object %x", manifest.BlockID, existingObject)
	}
	d.mu.RUnlock()

	encoded := encodeCacheManifest(manifest)
	tmp, err := os.CreateTemp(d.root, fmt.Sprintf("%x-*.kvmeta.tmp", manifest.ObjectID))
	if err != nil {
		return fmt.Errorf("disk tier: create manifest temp file: %w", err)
	}
	tmpPath := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := tmp.Write(encoded); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("disk tier: write manifest: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("disk tier: sync manifest: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("disk tier: close manifest: %w", err)
	}
	path := d.cacheManifestPath(manifest.ObjectID)
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("disk tier: commit manifest: %w", err)
	}
	committed = true
	if err := syncDir(d.root); err != nil {
		return fmt.Errorf("disk tier: sync root after manifest commit: %w", err)
	}
	d.mu.Lock()
	d.manifests[manifest.ObjectID] = manifest
	d.manifestByBlock[manifest.BlockID] = manifest.ObjectID
	d.mu.Unlock()
	return nil
}

func (d *DiskTier) ListCacheManifests() []CacheObjectManifest {
	if d == nil {
		return nil
	}
	d.mu.RLock()
	result := make([]CacheObjectManifest, 0, len(d.manifests))
	for _, manifest := range d.manifests {
		result = append(result, manifest)
	}
	d.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].TokenCount < result[j].TokenCount })
	return result
}

func (d *DiskTier) DeleteCacheManifestByBlock(blockID uint64) error {
	if d == nil {
		return fmt.Errorf("disk tier: nil tier")
	}
	d.manifestMu.Lock()
	defer d.manifestMu.Unlock()
	d.mu.Lock()
	objectID, ok := d.manifestByBlock[blockID]
	if ok {
		delete(d.manifestByBlock, blockID)
		delete(d.manifests, objectID)
	}
	d.mu.Unlock()
	if !ok {
		return nil
	}
	if err := os.Remove(d.cacheManifestPath(objectID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("disk tier: delete manifest for block %d: %w", blockID, err)
	}
	return syncDir(d.root)
}

func (d *DiskTier) loadCacheManifests() error {
	entries, err := os.ReadDir(d.root)
	if err != nil {
		return fmt.Errorf("disk tier: scan manifests: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".kvmeta") {
			continue
		}
		payload, err := os.ReadFile(filepath.Join(d.root, entry.Name()))
		if err != nil {
			return fmt.Errorf("disk tier: read manifest %s: %w", entry.Name(), err)
		}
		manifest, err := decodeCacheManifest(payload)
		if err != nil {
			d.invalidFiles++
			continue
		}
		meta, ok := d.blocks[manifest.BlockID]
		if !ok || meta.Length != manifest.Length || meta.Checksum != manifest.Checksum {
			d.invalidFiles++
			continue
		}
		if other, exists := d.manifestByBlock[manifest.BlockID]; exists && other != manifest.ObjectID {
			d.invalidFiles++
			continue
		}
		d.manifests[manifest.ObjectID] = manifest
		d.manifestByBlock[manifest.BlockID] = manifest.ObjectID
	}
	return nil
}

func (d *DiskTier) cacheManifestPath(objectID [32]byte) string {
	return filepath.Join(d.root, fmt.Sprintf("%x.kvmeta", objectID))
}

func validateCacheManifest(manifest CacheObjectManifest) error {
	if manifest.ObjectID == ([32]byte{}) || manifest.ScopeDigest == ([32]byte{}) || manifest.PrefixDigest == ([32]byte{}) {
		return fmt.Errorf("disk tier: manifest contains zero digest")
	}
	if manifest.BlockID == 0 || manifest.TokenCount == 0 || manifest.Length == 0 {
		return fmt.Errorf("disk tier: manifest contains zero numeric field")
	}
	return nil
}

func encodeCacheManifest(manifest CacheObjectManifest) []byte {
	payload := make([]byte, cacheManifestHeaderSize)
	binary.LittleEndian.PutUint32(payload[0:4], cacheManifestMagic)
	binary.LittleEndian.PutUint16(payload[4:6], cacheManifestVersion)
	binary.LittleEndian.PutUint16(payload[6:8], cacheManifestHeaderSize)
	binary.LittleEndian.PutUint64(payload[8:16], manifest.BlockID)
	binary.LittleEndian.PutUint64(payload[16:24], manifest.TokenCount)
	binary.LittleEndian.PutUint64(payload[24:32], manifest.Length)
	binary.LittleEndian.PutUint32(payload[32:36], manifest.Checksum)
	copy(payload[40:72], manifest.ScopeDigest[:])
	copy(payload[72:104], manifest.PrefixDigest[:])
	copy(payload[104:136], manifest.ObjectID[:])
	binary.LittleEndian.PutUint32(payload[136:140], crc32.ChecksumIEEE(payload[:136]))
	return payload
}

func decodeCacheManifest(payload []byte) (CacheObjectManifest, error) {
	if len(payload) != cacheManifestHeaderSize || binary.LittleEndian.Uint32(payload[0:4]) != cacheManifestMagic ||
		binary.LittleEndian.Uint16(payload[4:6]) != cacheManifestVersion || binary.LittleEndian.Uint16(payload[6:8]) != cacheManifestHeaderSize {
		return CacheObjectManifest{}, fmt.Errorf("disk tier: invalid cache manifest header")
	}
	if binary.LittleEndian.Uint32(payload[136:140]) != crc32.ChecksumIEEE(payload[:136]) {
		return CacheObjectManifest{}, fmt.Errorf("disk tier: cache manifest checksum mismatch")
	}
	manifest := CacheObjectManifest{
		BlockID:    binary.LittleEndian.Uint64(payload[8:16]),
		TokenCount: binary.LittleEndian.Uint64(payload[16:24]),
		Length:     binary.LittleEndian.Uint64(payload[24:32]),
		Checksum:   binary.LittleEndian.Uint32(payload[32:36]),
	}
	copy(manifest.ScopeDigest[:], payload[40:72])
	copy(manifest.PrefixDigest[:], payload[72:104])
	copy(manifest.ObjectID[:], payload[104:136])
	return manifest, validateCacheManifest(manifest)
}
