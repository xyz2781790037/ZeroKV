package kvcachekey

import (
	"errors"
	"fmt"
	"sync"
)

var ErrConflict = errors.New("kvcache key: conflicting immutable object")

// Entry binds a semantic key to the physical block metadata validated by the
// storage layer. The binding is immutable and duplicate writes are idempotent.
type Entry struct {
	Key      Key
	ObjectID Digest
	BlockID  uint64
	Length   uint64
	Checksum uint32
}

// PrefixMatch is the longest consecutive prefix present in an Index.
type PrefixMatch struct {
	Entries       []Entry
	MatchedTokens uint64
}

// Index owns only semantic key bindings. It does not own payload memory,
// placement, replicas, leases, or transport connections.
type Index struct {
	mu      sync.RWMutex
	entries map[Digest]Entry
	byBlock map[uint64]Digest
}

func NewIndex() *Index {
	return &Index{
		entries: make(map[Digest]Entry),
		byBlock: make(map[uint64]Digest),
	}
}

// Bind publishes an immutable key-to-block mapping. Repeating the same binding
// succeeds, while reusing the key for different physical metadata is rejected.
func (i *Index) Bind(key Key, blockID uint64, length uint64, checksum uint32) error {
	if i == nil {
		return fmt.Errorf("kvcache key: nil index")
	}
	if blockID == 0 || length == 0 {
		return fmt.Errorf("kvcache key: block id and length must be positive")
	}
	objectID := key.Digest()
	wantBlockID, err := blockIDFromDigest(objectID)
	if err != nil {
		return err
	}
	if blockID != wantBlockID {
		return fmt.Errorf("kvcache key: block id %d does not match derived id %d", blockID, wantBlockID)
	}
	incoming := Entry{
		Key:      key,
		ObjectID: objectID,
		BlockID:  blockID,
		Length:   length,
		Checksum: checksum,
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	if i.entries == nil {
		i.entries = make(map[Digest]Entry)
	}
	if i.byBlock == nil {
		i.byBlock = make(map[uint64]Digest)
	}
	if existing, ok := i.entries[objectID]; ok {
		if existing.BlockID == incoming.BlockID && existing.Length == incoming.Length && existing.Checksum == incoming.Checksum {
			return nil
		}
		return fmt.Errorf("%w: object=%s existing block=%d length=%d checksum=%d incoming block=%d length=%d checksum=%d",
			ErrConflict, objectID.Hex(), existing.BlockID, existing.Length, existing.Checksum,
			incoming.BlockID, incoming.Length, incoming.Checksum)
	}
	if existingObject, ok := i.byBlock[blockID]; ok && existingObject != objectID {
		return fmt.Errorf("%w: block id %d is already bound to object %s", ErrConflict, blockID, existingObject.Hex())
	}
	i.entries[objectID] = incoming
	i.byBlock[blockID] = objectID
	return nil
}

func (i *Index) Lookup(key Key) (Entry, bool) {
	return i.LookupObject(key.Digest())
}

// LookupObject resolves a full 256-bit object identity. BlockID alone is never
// sufficient because it is only a compact physical projection.
func (i *Index) LookupObject(objectID Digest) (Entry, bool) {
	if i == nil {
		return Entry{}, false
	}
	i.mu.RLock()
	entry, ok := i.entries[objectID]
	i.mu.RUnlock()
	return entry, ok
}

// LookupBlock is used only to recover semantic metadata for an already-known
// physical block. The returned entry still carries the full ObjectID so callers
// can perform the required collision check.
func (i *Index) LookupBlock(blockID uint64) (Entry, bool) {
	if i == nil {
		return Entry{}, false
	}
	i.mu.RLock()
	objectID, ok := i.byBlock[blockID]
	entry := i.entries[objectID]
	i.mu.RUnlock()
	return entry, ok
}

// LongestPrefix stops at the first missing chunk because causal KV reuse must
// be contiguous from the beginning of the request.
func (i *Index) LongestPrefix(keys []Key) PrefixMatch {
	if i == nil || len(keys) == 0 {
		return PrefixMatch{}
	}
	i.mu.RLock()
	defer i.mu.RUnlock()

	match := PrefixMatch{Entries: make([]Entry, 0, len(keys))}
	for _, key := range keys {
		entry, ok := i.entries[key.Digest()]
		if !ok {
			break
		}
		match.Entries = append(match.Entries, entry)
		match.MatchedTokens = key.TokenCount
	}
	return match
}

func (i *Index) Forget(key Key) bool {
	if i == nil {
		return false
	}
	objectID := key.Digest()
	i.mu.Lock()
	_, ok := i.entries[objectID]
	if ok {
		delete(i.byBlock, i.entries[objectID].BlockID)
		delete(i.entries, objectID)
	}
	i.mu.Unlock()
	return ok
}

func (i *Index) ForgetBlock(blockID uint64) bool {
	if i == nil {
		return false
	}
	i.mu.Lock()
	objectID, ok := i.byBlock[blockID]
	if ok {
		delete(i.byBlock, blockID)
		delete(i.entries, objectID)
	}
	i.mu.Unlock()
	return ok
}

func (i *Index) Len() int {
	if i == nil {
		return 0
	}
	i.mu.RLock()
	n := len(i.entries)
	i.mu.RUnlock()
	return n
}
