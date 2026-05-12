package storage

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"kvcache/pkg/protocol"
)

var ErrBlockBusy = errors.New("storage handler: block is busy")

// EvictPolicy 决定哪些 block 保留在索引中。
// 对 bump allocator 而言，逻辑淘汰只删除索引，不回收 OffheapPool 物理空间；
// 物理回收必须通过 Reset，在没有活跃 lease 时整池复用。
type EvictPolicy interface {
	Keep(block Block) bool
}
type EvictPolicyFunc func(Block) bool

func (f EvictPolicyFunc) Keep(block Block) bool {
	return f(block)
}

// 空结构体占用 0 字节内存。当底层调用 KeepAllPolicy{} 时，系统不会发生任何实际的内存分配。它纯粹是为了实现 EvictPolicy 接口而存在的“幽灵接收者”，专门用来向 Compact 函数注入一段纯粹的逻辑（永远返回 true），性能开销为绝对的零。
type KeepAllPolicy struct{}

func (KeepAllPolicy) Keep(Block) bool {
	return true
}

type KeepGenerationPolicy struct {
	Generation uint64
}

func (p KeepGenerationPolicy) Keep(block Block) bool {
	return block.Generation == p.Generation
}

// EvictBlockIDsPolicy 淘汰指定 block_id，保留其他 block。
type EvictBlockIDsPolicy map[uint64]struct{}

func NewEvictBlockIDsPolicy(blockIDs ...uint64) EvictBlockIDsPolicy {
	policy := make(EvictBlockIDsPolicy, len(blockIDs))
	for _, blockID := range blockIDs {
		policy[blockID] = struct{}{}
	}
	return policy
}
func (p EvictBlockIDsPolicy) Keep(block Block) bool {
	_, evict := p[block.ID]
	return !evict
}

// EvictResult 记录了一次内存淘汰/压缩操作前后的核心统计指标与状态快照。
type EvictResult struct {
	BeforeBlocks uint64 // 【压缩前块数】: 执行淘汰/压缩操作前，缓存池中存在的总数据块数量
	AfterBlocks  uint64 // 【压缩后块数】: 执行操作后，成功保留在内存池中的有效数据块数量
	Evicted      uint64 // 【已淘汰块数】: 本次策略命中并被物理清理掉的数据块总数 (等于 BeforeBlocks - AfterBlocks)
	BeforeBytes  uint64 // 【压缩前内存量】: 执行操作前，所有数据块在堆外内存池(OffheapPool)占用的总物理字节数
	AfterBytes   uint64 // 【压缩后内存量】: 执行操作后，剩余保留数据块在堆外占用、重新紧凑打包后的总物理字节数
	Generation   uint64 // 【最新分代纪元】: 压缩完成后存储引擎更新的全局分代计数器，用于防幻读和拦截旧缓存拦截
}

// Evict 删除不满足 policy 的 block 索引。
// 它不会移动或覆写 OffheapPool 中的数据，因此不会破坏已经发出去的 lease。
func (h *Handler) Evict(policy EvictPolicy) (EvictResult, error) {
	if h == nil {
		return EvictResult{}, fmt.Errorf("storage handler: nil handler")
	}
	if policy == nil {
		policy = KeepAllPolicy{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	result := EvictResult{
		BeforeBlocks: uint64(len(h.blocks)),
		Generation:   h.generation,
	}
	for blockID, block := range h.blocks {
		result.BeforeBytes += block.Allocated
		if policy.Keep(block) {
			result.AfterBlocks++
			result.AfterBytes += block.Allocated
			continue
		}
		delete(h.blocks, blockID)
		result.Evicted++
	}
	return result, nil
}

// ReclaimAll 是物理回收入口，本质上等价于 Reset。
// 它会等待所有通过 Acquire 发出的 lease 释放后，再清空索引并复用 OffheapPool。
func (h *Handler) ReclaimAll() {
	h.Reset()
}

// Compact rebuilds the offheap pool with only blocks accepted by policy.
// It waits for all active zero-copy leases to drain, then copies live blocks
// into a fresh mmap region and releases the old one. This is the physical
// reclaim path for the bump allocator.
func (h *Handler) Compact(policy EvictPolicy) (EvictResult, error) {
	if h == nil {
		return EvictResult{}, fmt.Errorf("storage handler: nil handler")
	}
	if policy == nil {
		policy = KeepAllPolicy{}
	}

	h.lifecycle.Lock()
	defer h.lifecycle.Unlock()

	return h.compactLocked(policy)
}

// TryCompact performs the same physical reclaim as Compact, but never waits for
// active readers. It returns compacted=false when any lease/load is in progress.
func (h *Handler) TryCompact(policy EvictPolicy) (EvictResult, bool, error) {
	if h == nil {
		return EvictResult{}, false, fmt.Errorf("storage handler: nil handler")
	}
	if policy == nil {
		policy = KeepAllPolicy{}
	}
	if !h.lifecycle.TryLock() {
		return EvictResult{}, false, nil
	}
	defer h.lifecycle.Unlock()

	result, err := h.compactLocked(policy)
	return result, true, err
}

func (h *Handler) compactLocked(policy EvictPolicy) (EvictResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	result := EvictResult{
		BeforeBlocks: uint64(len(h.blocks)),
		Generation:   h.generation,
	}
	for _, block := range h.blocks {
		result.BeforeBytes += block.Allocated
	}

	newPool, err := NewOffheapPool(h.pool.Size())
	if err != nil {
		return result, err
	}
	newBlocks := make(map[uint64]Block, len(h.blocks))
	for _, block := range h.blocks {
		if !policy.Keep(block) {
			result.Evicted++
			continue
		}
		if h.shm != nil && h.shm.Owns(block.ShmName) {
			newBlocks[block.ID] = block
			result.AfterBlocks++
			result.AfterBytes += block.Allocated
			continue
		}
		dst, err := newPool.Alloc(block.Length)
		if err != nil {
			_ = newPool.Release()
			return result, fmt.Errorf("storage handler: compact block %d length=%d: %w", block.ID, block.Length, err)
		}
		copy(dst, block.Data)
		block.Data = dst
		block.Allocated = uint64(cap(dst))
		newBlocks[block.ID] = block
		result.AfterBlocks++
		result.AfterBytes += block.Allocated
	}

	oldPool := h.pool
	h.pool = newPool
	h.blocks = newBlocks
	h.inflight = make(map[uint64]*blockLoad)
	h.leases = make(map[uint64]int)
	h.evicting = make(map[uint64]struct{})
	if err := oldPool.Release(); err != nil {
		return result, fmt.Errorf("storage handler: release old pool after compact: %w", err)
	}
	return result, nil
}

// SpillToDisk writes an in-memory block to disk and then removes its memory index.
// The offheap bytes are not physically reclaimed until Reset/ReclaimAll.
func (h *Handler) SpillToDisk(blockID uint64, disk *DiskTier) error {
	if h == nil {
		return fmt.Errorf("storage handler: nil handler")
	}
	if disk == nil {
		return fmt.Errorf("storage handler: nil disk tier")
	}
	_, ok, err := h.TrySpillToDisk(blockID, disk)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("storage handler: block %d not found in memory", blockID)
	}
	return nil
}

// TrySpillToDisk writes a cold in-memory block to disk and removes only its
// memory index. If the block has active zero-copy readers, the caller gets
// ErrBlockBusy and should pick another LRU victim.
func (h *Handler) TrySpillToDisk(blockID uint64, disk *DiskTier) (BlockMeta, bool, error) {
	if h == nil {
		return BlockMeta{}, false, fmt.Errorf("storage handler: nil handler")
	}
	if disk == nil {
		return BlockMeta{}, false, fmt.Errorf("storage handler: nil disk tier")
	}

	h.lifecycle.RLock()
	defer h.lifecycle.RUnlock()

	h.mu.Lock()
	block, ok := h.blocks[blockID]
	if !ok {
		h.mu.Unlock()
		return BlockMeta{}, false, nil
	}
	if h.leases[blockID] > 0 {
		h.mu.Unlock()
		return blockMeta(block), true, ErrBlockBusy
	}
	if _, busy := h.evicting[blockID]; busy {
		h.mu.Unlock()
		return blockMeta(block), true, ErrBlockBusy
	}
	h.evicting[blockID] = struct{}{}
	h.leases[blockID]++
	h.mu.Unlock()

	meta := blockMeta(block)
	err := disk.Put(block)

	h.mu.Lock()
	if count := h.leases[blockID]; count <= 1 {
		delete(h.leases, blockID)
	} else {
		h.leases[blockID] = count - 1
	}
	delete(h.evicting, blockID)
	if err == nil {
		current, exists := h.blocks[blockID]
		if exists && current.Seq == block.Seq && current.Length == block.Length && current.Checksum == block.Checksum {
			delete(h.blocks, blockID)
		}
	}
	h.mu.Unlock()

	if err != nil {
		return meta, true, err
	}
	return meta, true, nil
}

// PromoteFromDisk loads a disk block back into OffheapPool and indexes it in memory.
func (h *Handler) PromoteFromDisk(blockID uint64, disk *DiskTier) error {
	if h == nil {
		return fmt.Errorf("storage handler: nil handler")
	}
	if disk == nil {
		return fmt.Errorf("storage handler: nil disk tier")
	}

	reader, ok, err := disk.Open(blockID)
	if err != nil || !ok {
		if !ok {
			return fmt.Errorf("storage handler: block %d not found on disk", blockID)
		}
		return err
	}
	defer reader.Close()

	meta := reader.Meta()
	h.lifecycle.RLock()
	defer h.lifecycle.RUnlock()

	generation, load, owner, err := h.beginPromote(meta)
	if err != nil {
		return err
	}
	if !owner {
		if load == nil {
			return nil
		}
		return waitLoadDone(load)
	}
	defer h.finishLoad(blockID, load, &err)

	dst, err := h.pool.Alloc(meta.Length)
	if err != nil {
		return fmt.Errorf("storage handler: promote block %d length=%d: %w", blockID, meta.Length, err)
	}
	written, checksum, err := copyAndChecksum(dst, reader)
	if err != nil {
		return err
	}
	if written != meta.Length {
		return fmt.Errorf("storage handler: promoted block %d short read: expected=%d got=%d", blockID, meta.Length, written)
	}
	if checksum != meta.Checksum {
		return fmt.Errorf("storage handler: promoted block %d checksum mismatch: expected=%d actual=%d", blockID, meta.Checksum, checksum)
	}

	return h.commitPromotedBlock(generation, Block{
		ID:         meta.ID,
		Seq:        meta.Seq,
		Generation: generation,
		Length:     meta.Length,
		Allocated:  uint64(cap(dst)),
		Checksum:   meta.Checksum,
		Data:       dst,
	})
}

func (h *Handler) beginPromote(meta DiskBlockMeta) (uint64, *blockLoad, bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if existing, exists := h.blocks[meta.ID]; exists {
		return h.generation, nil, false, validatePromotedBlock(existing, meta)
	}
	if load, exists := h.inflight[meta.ID]; exists {
		if err := validateReadyMetadata(load.block, blockReadyFromMeta(meta)); err != nil {
			return h.generation, nil, false, err
		}
		return h.generation, load, false, nil
	}

	load := &blockLoad{
		block: blockReadyFromMeta(meta),
		done:  make(chan struct{}),
	}
	h.inflight[meta.ID] = load
	return h.generation, load, true, nil
}

func (h *Handler) commitPromotedBlock(generation uint64, block Block) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if generation != h.generation {
		return fmt.Errorf("storage handler: generation changed while promoting block %d", block.ID)
	}
	if existing, exists := h.blocks[block.ID]; exists {
		return validatePromotedBlock(existing, DiskBlockMeta{
			ID:       block.ID,
			Seq:      block.Seq,
			Length:   block.Length,
			Checksum: block.Checksum,
		})
	}
	h.blocks[block.ID] = block
	return nil
}

func copyAndChecksum(dst []byte, src io.Reader) (uint64, uint32, error) {
	hash := crc32.NewIEEE()
	bufWriter := &sliceWriter{buf: dst}
	writer := io.MultiWriter(bufWriter, hash)
	written, err := io.Copy(writer, src)
	if err != nil {
		return uint64(written), 0, fmt.Errorf("storage handler: copy promoted block: %w", err)
	}
	return uint64(written), hash.Sum32(), nil
}

type sliceWriter struct {
	buf []byte
}

func (w *sliceWriter) Write(p []byte) (int, error) {
	if len(p) > len(w.buf) {
		copy(w.buf, p[:len(w.buf)])
		return len(w.buf), io.ErrShortWrite
	}
	copy(w.buf, p)
	w.buf = w.buf[len(p):]
	return len(p), nil
}

func blockReadyFromMeta(meta DiskBlockMeta) protocol.BlockReady {
	return protocol.BlockReady{
		BlockID:  meta.ID,
		Length:   meta.Length,
		Checksum: meta.Checksum,
	}
}

func waitLoadDone(load *blockLoad) error {
	<-load.done
	return load.err
}

func validatePromotedBlock(existing Block, meta DiskBlockMeta) error {
	if existing.ID == meta.ID && existing.Length == meta.Length && existing.Checksum == meta.Checksum {
		return nil
	}
	return fmt.Errorf(
		"storage handler: conflicting promoted block metadata for block %d: existing length=%d checksum=%d incoming length=%d checksum=%d",
		meta.ID,
		existing.Length,
		existing.Checksum,
		meta.Length,
		meta.Checksum,
	)
}
