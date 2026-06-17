package distributed

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"kvcache/internal/network"
	"kvcache/internal/storage"
	"kvcache/pkg/kvcachekey"
	"kvcache/proto/controlplane"
)

func (s *Store) restoreCacheManifests() error {
	if s == nil || s.disk == nil {
		return nil
	}
	for _, manifest := range s.disk.ListCacheManifests() {
		key := kvcachekey.Key{
			ScopeDigest:  kvcachekey.Digest(manifest.ScopeDigest),
			PrefixDigest: kvcachekey.Digest(manifest.PrefixDigest),
			TokenCount:   manifest.TokenCount,
		}
		if key.Digest() != kvcachekey.Digest(manifest.ObjectID) {
			return fmt.Errorf("distributed store: invalid persisted cache object %x", manifest.ObjectID)
		}
		if err := s.semantic.Bind(key, manifest.BlockID, manifest.Length, manifest.Checksum); err != nil {
			return fmt.Errorf("distributed store: restore cache object %x: %w", manifest.ObjectID, err)
		}
	}
	return nil
}

// CommitCacheObject is the visibility boundary between an opaque physical
// block and a reusable KV-cache object. A failed commit leaves at worst an
// unindexed physical block, which is safe because Prefix Lookup cannot see it.
func (s *Store) CommitCacheObject(ctx context.Context, commit network.CacheObjectCommit) error {
	if s == nil {
		return fmt.Errorf("distributed store: nil store")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	key := kvcachekey.Key{
		ScopeDigest:  kvcachekey.Digest(commit.ScopeDigest),
		PrefixDigest: kvcachekey.Digest(commit.PrefixDigest),
		TokenCount:   commit.TokenCount,
	}
	objectID := key.Digest()
	if objectID != kvcachekey.Digest(commit.ObjectID) {
		return fmt.Errorf("distributed store: cache object digest mismatch")
	}
	blockID, err := objectID.BlockID()
	if err != nil || blockID != commit.BlockID {
		return fmt.Errorf("distributed store: cache object block id mismatch")
	}
	meta, tier, ok := s.localCacheBlockMeta(commit.BlockID)
	if !ok {
		return network.ErrBlockNotFound
	}
	if meta.Length != commit.Length || meta.Checksum != commit.Checksum {
		return fmt.Errorf("distributed store: cache object block metadata mismatch")
	}
	existingCommit := false
	if existing, found := s.semantic.LookupObject(objectID); found {
		if existing.Key != key || existing.BlockID != commit.BlockID || existing.Length != commit.Length || existing.Checksum != commit.Checksum {
			return fmt.Errorf("%w: object=%s", kvcachekey.ErrConflict, objectID.Hex())
		}
		existingCommit = true
	}
	manifest := storage.CacheObjectManifest{
		ScopeDigest:  [32]byte(commit.ScopeDigest),
		PrefixDigest: [32]byte(commit.PrefixDigest),
		ObjectID:     [32]byte(commit.ObjectID),
		TokenCount:   commit.TokenCount,
		BlockID:      commit.BlockID,
		Length:       commit.Length,
		Checksum:     commit.Checksum,
	}
	if s.disk != nil && !existingCommit {
		if err := s.disk.PutCacheManifest(manifest); err != nil {
			return fmt.Errorf("distributed store: persist cache object: %w", err)
		}
	}
	if !existingCommit {
		if err := s.semantic.Bind(key, commit.BlockID, commit.Length, commit.Checksum); err != nil {
			return err
		}
	}
	objectBytes := append([]byte(nil), commit.ObjectID[:]...)
	if memoryMeta, found := s.local.Meta(commit.BlockID); found {
		if err := s.announceTierObject(ctx, memoryMeta.Seq, memoryMeta.ID, memoryMeta.Length, memoryMeta.Checksum, objectBytes, controlplane.StorageTier_STORAGE_TIER_MEMORY); err != nil {
			return err
		}
	}
	if tier == network.CacheTierDisk || (s.disk != nil && s.disk.Has(commit.BlockID)) {
		diskMeta, _ := s.disk.Meta(commit.BlockID)
		if err := s.announceTierObject(ctx, diskMeta.Seq, diskMeta.ID, diskMeta.Length, diskMeta.Checksum, objectBytes, controlplane.StorageTier_STORAGE_TIER_DISK); err != nil {
			return err
		}
	}
	return nil
}

// LookupPrefix returns one contiguous batch and a short lease. Operational
// cache failures are represented as stop reasons so inference can recompute the
// suffix without treating cache availability as request failure.
func (s *Store) LookupPrefix(ctx context.Context, req network.PrefixLookupRequest) (network.PrefixLookupResult, error) {
	if s == nil {
		return network.PrefixLookupResult{}, fmt.Errorf("distributed store: nil store")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if req.ScopeDigest == (network.CacheDigest{}) || len(req.Candidates) == 0 || len(req.Candidates) > network.DefaultMaxPrefixEntries {
		return network.PrefixLookupResult{}, fmt.Errorf("distributed store: invalid prefix lookup request")
	}
	result := network.PrefixLookupResult{StopReason: network.PrefixStopFullMatch}
	held := make([]uint64, 0, len(req.Candidates))
	var previousTokenEnd uint64
	for _, candidate := range req.Candidates {
		if candidate.ObjectID == (network.CacheDigest{}) || candidate.TokenEnd <= previousTokenEnd {
			for _, blockID := range held {
				s.unpinLookupBlock(blockID)
			}
			return network.PrefixLookupResult{}, fmt.Errorf("distributed store: invalid prefix candidate order")
		}
		previousTokenEnd = candidate.TokenEnd
		objectID := kvcachekey.Digest(candidate.ObjectID)
		blockID, err := objectID.BlockID()
		if err != nil {
			result.StopReason = network.PrefixStopUnavailable
			break
		}
		if !s.pinBlockForLookup(blockID) {
			result.StopReason = network.PrefixStopBusy
			break
		}
		location, found, unavailable := s.resolvePrefixLocation(ctx, req.ScopeDigest, candidate, blockID)
		if !found {
			s.unpinLookupBlock(blockID)
			if unavailable {
				result.StopReason = network.PrefixStopUnavailable
			} else {
				result.StopReason = network.PrefixStopNotFound
			}
			break
		}
		held = append(held, blockID)
		result.Entries = append(result.Entries, location)
		result.MatchedTokens = candidate.TokenEnd
	}
	if len(held) == 0 {
		return result, nil
	}
	leaseID, expiresAt := s.publishPrefixLease(held)
	result.LeaseID = leaseID
	result.ExpiresUnixNano = expiresAt.UnixNano()
	return result, nil
}

func (s *Store) resolvePrefixLocation(ctx context.Context, scope network.CacheDigest, candidate network.PrefixCandidate, blockID uint64) (network.PrefixLocation, bool, bool) {
	objectID := kvcachekey.Digest(candidate.ObjectID)
	if entry, ok := s.semantic.LookupObject(objectID); ok && entry.Key.ScopeDigest == kvcachekey.Digest(scope) && entry.Key.TokenCount == candidate.TokenEnd {
		if meta, tier, found := s.localCacheBlockMeta(blockID); found && meta.Length == entry.Length && meta.Checksum == entry.Checksum {
			return s.prefixLocation(candidate, blockID, meta.Length, meta.Checksum, tier), true, false
		}
	}

	locations, err := s.getLocations(ctx, blockID)
	if err != nil {
		return network.PrefixLocation{}, false, true
	}
	selected := s.selectSemanticLocation(locations, candidate.ObjectID)
	if selected == nil {
		return network.PrefixLocation{}, false, false
	}
	if err := s.fetchAndCacheFromLocations(ctx, blockID, []*controlplane.BlockLocation{selected}); err != nil {
		return network.PrefixLocation{}, false, true
	}
	meta, _, found := s.localCacheBlockMeta(blockID)
	if !found || meta.Length != selected.GetMeta().GetLength() || meta.Checksum != selected.GetMeta().GetChecksum() {
		return network.PrefixLocation{}, false, true
	}
	return s.prefixLocation(candidate, blockID, meta.Length, meta.Checksum, network.CacheTierMemory), true, false
}

func (s *Store) selectSemanticLocation(locations []*controlplane.BlockLocation, objectID network.CacheDigest) *controlplane.BlockLocation {
	candidates := make([]*controlplane.BlockLocation, 0, len(locations))
	for _, location := range locations {
		if s.skipLocation(location) || location.GetMeta() == nil {
			continue
		}
		if !bytes.Equal(location.GetMeta().GetObjectId(), objectID[:]) {
			continue
		}
		candidates = append(candidates, location)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left, right := candidates[i], candidates[j]
		if left.GetTier() != right.GetTier() {
			return prefixTierRank(left.GetTier()) < prefixTierRank(right.GetTier())
		}
		if left.GetNodeId() != right.GetNodeId() {
			return left.GetNodeId() < right.GetNodeId()
		}
		if left.GetAddr() != right.GetAddr() {
			return left.GetAddr() < right.GetAddr()
		}
		return left.GetVersion() > right.GetVersion()
	})
	if len(candidates) == 0 {
		return nil
	}
	return candidates[0]
}

func prefixTierRank(tier controlplane.StorageTier) int {
	switch tier {
	case controlplane.StorageTier_STORAGE_TIER_MEMORY:
		return 0
	case controlplane.StorageTier_STORAGE_TIER_DISK:
		return 1
	default:
		return 2
	}
}

func (s *Store) localCacheBlockMeta(blockID uint64) (storage.BlockMeta, network.CacheTier, bool) {
	if meta, ok := s.local.Meta(blockID); ok {
		return meta, network.CacheTierMemory, true
	}
	if s.disk != nil {
		if meta, ok := s.disk.Meta(blockID); ok {
			return storage.BlockMeta{ID: meta.ID, Seq: meta.Seq, Length: meta.Length, Checksum: meta.Checksum}, network.CacheTierDisk, true
		}
	}
	return storage.BlockMeta{}, network.CacheTierUnknown, false
}

func (s *Store) prefixLocation(candidate network.PrefixCandidate, blockID uint64, length uint64, checksum uint32, tier network.CacheTier) network.PrefixLocation {
	return network.PrefixLocation{
		ObjectID:  candidate.ObjectID,
		BlockID:   blockID,
		TokenEnd:  candidate.TokenEnd,
		Length:    length,
		Checksum:  checksum,
		Tier:      tier,
		Transport: network.CacheTransportTCP,
		NodeID:    s.selfID,
		Address:   s.selfAddr,
	}
}

func (s *Store) objectIDForBlock(blockID uint64) []byte {
	if s == nil || s.semantic == nil {
		return nil
	}
	entry, ok := s.semantic.LookupBlock(blockID)
	if !ok {
		return nil
	}
	return append([]byte(nil), entry.ObjectID[:]...)
}

func (s *Store) pinBlockForLookup(blockID uint64) bool {
	s.lookupMu.Lock()
	defer s.lookupMu.Unlock()
	if _, mutating := s.lookupMutating[blockID]; mutating {
		return false
	}
	s.lookupPins[blockID]++
	return true
}

func (s *Store) unpinLookupBlock(blockID uint64) {
	s.lookupMu.Lock()
	if count := s.lookupPins[blockID]; count <= 1 {
		delete(s.lookupPins, blockID)
	} else {
		s.lookupPins[blockID] = count - 1
	}
	s.lookupMu.Unlock()
}

func (s *Store) publishPrefixLease(blockIDs []uint64) (uint64, time.Time) {
	leaseID := s.nextLeaseID.Add(1) ^ uint64(time.Now().UnixNano())
	if leaseID == 0 {
		leaseID = s.nextLeaseID.Add(1)
	}
	expiresAt := time.Now().Add(s.leaseTTL)
	s.lookupMu.Lock()
	s.lookupLeases[leaseID] = prefixLease{blockIDs: append([]uint64(nil), blockIDs...), expiresAt: expiresAt}
	s.lookupMu.Unlock()
	time.AfterFunc(s.leaseTTL, func() { _ = s.ReleasePrefixLease(context.Background(), leaseID) })
	return leaseID, expiresAt
}

func (s *Store) ReleasePrefixLease(ctx context.Context, leaseID uint64) error {
	if s == nil || leaseID == 0 {
		return nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	s.lookupMu.Lock()
	lease, ok := s.lookupLeases[leaseID]
	if ok {
		delete(s.lookupLeases, leaseID)
		for _, blockID := range lease.blockIDs {
			if count := s.lookupPins[blockID]; count <= 1 {
				delete(s.lookupPins, blockID)
			} else {
				s.lookupPins[blockID] = count - 1
			}
		}
	}
	s.lookupMu.Unlock()
	return nil
}

func (s *Store) beginCacheMutation(blockID uint64) bool {
	s.lookupMu.Lock()
	defer s.lookupMu.Unlock()
	if s.lookupPins[blockID] != 0 {
		return false
	}
	if _, exists := s.lookupMutating[blockID]; exists {
		return false
	}
	s.lookupMutating[blockID] = struct{}{}
	return true
}

func (s *Store) endCacheMutation(blockID uint64) {
	s.lookupMu.Lock()
	delete(s.lookupMutating, blockID)
	s.lookupMu.Unlock()
}
