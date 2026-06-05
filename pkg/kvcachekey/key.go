// Package kvcachekey defines the semantic identity of reusable KV-cache chunks.
//
// Storage and transport code should continue to treat BlockID as an opaque
// physical handle. Inference-side connectors use this package's canonical key
// algorithm to derive that handle from model, layout, and token-prefix identity.
package kvcachekey

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"math"
)

const (
	// SchemaVersion is part of every scope digest. Changing the canonical
	// encoding or the meaning of a field requires a new version.
	SchemaVersion uint16 = 1

	DefaultNamespace = "default"
)

var (
	scopeDomain  = []byte("zerokv.kvcache.scope.v1\x00")
	prefixDomain = []byte("zerokv.kvcache.prefix.v1\x00")
	objectDomain = []byte("zerokv.kvcache.object.v1\x00")
)

// Digest is the stable 256-bit identity used by the semantic KV-cache layer.
type Digest [sha256.Size]byte

// Layout identifies the byte representation of one cached KV chunk. The MVP
// stores K and V for all configured layers in one object, so individual layer
// IDs are intentionally not part of the key yet.
type Layout struct {
	Version     uint16
	DType       string
	Layers      uint32
	Heads       uint32
	HeadDim     uint32
	TPWorldSize uint32
	TPRank      uint32
}

// Scope contains every input, other than tokens, that can change the KV bytes.
// Placement, storage tier, replicas, checksum, and transport must not appear
// here because moving an object must not change its semantic identity.
type Scope struct {
	Version       uint16
	Namespace     string
	ModelID       string
	ModelRevision string
	AdapterID     string
	ChunkSize     uint32
	Layout        Layout
}

// Key identifies one token-prefix chunk within a validated Scope.
type Key struct {
	ScopeDigest  Digest
	PrefixDigest Digest
	TokenCount   uint64
}

// ChunkKey adds the physical handle and token range used by existing ZeroKV
// storage and transport APIs.
type ChunkKey struct {
	Key        Key
	ObjectID   Digest
	BlockID    uint64
	TokenBegin uint64
	TokenEnd   uint64
}

// Validate rejects ambiguous scopes before they can enter the cache namespace.
func (s Scope) Validate() error {
	if s.Version != SchemaVersion {
		return fmt.Errorf("kvcache key: unsupported schema version %d", s.Version)
	}
	if s.Namespace == "" {
		return fmt.Errorf("kvcache key: empty namespace")
	}
	if s.ModelID == "" {
		return fmt.Errorf("kvcache key: empty model id")
	}
	if s.ModelRevision == "" {
		return fmt.Errorf("kvcache key: empty model revision")
	}
	if s.ChunkSize == 0 {
		return fmt.Errorf("kvcache key: zero chunk size")
	}
	if s.Layout.Version == 0 {
		return fmt.Errorf("kvcache key: zero layout version")
	}
	if s.Layout.DType == "" {
		return fmt.Errorf("kvcache key: empty dtype")
	}
	if s.Layout.Layers == 0 || s.Layout.Heads == 0 || s.Layout.HeadDim == 0 {
		return fmt.Errorf("kvcache key: layers, heads, and head dim must be positive")
	}
	if s.Layout.TPWorldSize == 0 {
		return fmt.Errorf("kvcache key: zero tensor-parallel world size")
	}
	if s.Layout.TPRank >= s.Layout.TPWorldSize {
		return fmt.Errorf("kvcache key: tensor-parallel rank %d outside world size %d", s.Layout.TPRank, s.Layout.TPWorldSize)
	}
	for name, value := range map[string]string{
		"namespace":      s.Namespace,
		"model id":       s.ModelID,
		"model revision": s.ModelRevision,
		"adapter id":     s.AdapterID,
		"dtype":          s.Layout.DType,
	} {
		if uint64(len(value)) > math.MaxUint32 {
			return fmt.Errorf("kvcache key: %s is too long", name)
		}
	}
	return nil
}

// ScopeDigest returns the canonical digest shared by every prefix in a scope.
func ScopeDigest(s Scope) (Digest, error) {
	if err := s.Validate(); err != nil {
		return Digest{}, err
	}
	h := sha256.New()
	_, _ = h.Write(scopeDomain)
	writeUint16(h, s.Version)
	writeString(h, s.Namespace)
	writeString(h, s.ModelID)
	writeString(h, s.ModelRevision)
	writeString(h, s.AdapterID)
	writeUint32(h, s.ChunkSize)
	writeUint16(h, s.Layout.Version)
	writeString(h, s.Layout.DType)
	writeUint32(h, s.Layout.Layers)
	writeUint32(h, s.Layout.Heads)
	writeUint32(h, s.Layout.HeadDim)
	writeUint32(h, s.Layout.TPWorldSize)
	writeUint32(h, s.Layout.TPRank)
	return digestSum(h), nil
}

// Build splits tokens by Scope.ChunkSize and derives a chained prefix digest.
// A later chunk therefore cannot match when any earlier token differs.
func Build(s Scope, tokens []uint32) ([]ChunkKey, error) {
	if len(tokens) == 0 {
		return nil, fmt.Errorf("kvcache key: empty token sequence")
	}
	scope, err := ScopeDigest(s)
	if err != nil {
		return nil, err
	}

	chunkSize := uint64(s.ChunkSize)
	result := make([]ChunkKey, 0, (uint64(len(tokens))+chunkSize-1)/chunkSize)
	var parent Digest
	for begin := uint64(0); begin < uint64(len(tokens)); begin += chunkSize {
		end := begin + chunkSize
		if end > uint64(len(tokens)) {
			end = uint64(len(tokens))
		}
		prefix := nextPrefixDigest(scope, parent, tokens[begin:end])
		key := Key{
			ScopeDigest:  scope,
			PrefixDigest: prefix,
			TokenCount:   end,
		}
		objectID := key.Digest()
		blockID, err := blockIDFromDigest(objectID)
		if err != nil {
			return nil, err
		}
		result = append(result, ChunkKey{
			Key:        key,
			ObjectID:   objectID,
			BlockID:    blockID,
			TokenBegin: begin,
			TokenEnd:   end,
		})
		parent = prefix
	}
	return result, nil
}

// Digest returns the full semantic object identity. The existing uint64
// BlockID is a temporary physical projection of this digest.
func (k Key) Digest() Digest {
	h := sha256.New()
	_, _ = h.Write(objectDomain)
	_, _ = h.Write(k.ScopeDigest[:])
	_, _ = h.Write(k.PrefixDigest[:])
	writeUint64(h, k.TokenCount)
	return digestSum(h)
}

// Hex returns the lowercase hexadecimal representation used in logs and tests.
func (d Digest) Hex() string {
	return fmt.Sprintf("%x", d[:])
}

// BlockID returns the current physical uint64 projection of a semantic object
// digest. Callers must still compare the full Digest before accepting a hit.
func (d Digest) BlockID() (uint64, error) {
	return blockIDFromDigest(d)
}

func nextPrefixDigest(scope Digest, parent Digest, tokens []uint32) Digest {
	h := sha256.New()
	_, _ = h.Write(prefixDomain)
	_, _ = h.Write(scope[:])
	_, _ = h.Write(parent[:])
	writeUint32(h, uint32(len(tokens)))
	for _, token := range tokens {
		writeUint32(h, token)
	}
	return digestSum(h)
}

func blockIDFromDigest(d Digest) (uint64, error) {
	id := binary.LittleEndian.Uint64(d[:8])
	if id == 0 {
		return 0, fmt.Errorf("kvcache key: derived zero block id")
	}
	return id, nil
}

func digestSum(h hash.Hash) Digest {
	var digest Digest
	copy(digest[:], h.Sum(nil))
	return digest
}

func writeString(h hash.Hash, value string) {
	writeUint32(h, uint32(len(value)))
	_, _ = h.Write([]byte(value))
}

func writeUint16(h hash.Hash, value uint16) {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], value)
	_, _ = h.Write(buf[:])
}

func writeUint32(h hash.Hash, value uint32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], value)
	_, _ = h.Write(buf[:])
}

func writeUint64(h hash.Hash, value uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	_, _ = h.Write(buf[:])
}
