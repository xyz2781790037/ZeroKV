package storage

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"kvcache/internal/ipc"
	"kvcache/pkg/protocol"
	"sync"
)

// Block 是已经从共享内存校验并拷贝到本地堆外池中的缓存块。
type Block struct {
	ID           uint64 // 【唯一标识符】: 数据的身份证，用于 radix 前缀路由和缓存查找
	Seq          uint64 // 【序列号】: 用于多版本并发控制(MVCC)或防乱序，确保处理的是最新消息
	Generation   uint64 // 【分代计数器】: 绑定当前内存池生命周期，在 Reset 后拦截并拒绝写入旧数据
	ShmName      string // 【共享内存文件名】: POSIX 共享内存的名称，指向 /dev/shm 中的源文件
	SourceOffset uint64 // 【源文件偏移量】: 标记该数据块在共享内存文件中的绝对起始物理位置
	Length       uint64 // 【有效数据长度】: 业务层真正需要的、未填充对齐的张量数据真实大小
	Allocated    uint64 // 【物理对齐长度】: 考虑 8 字节或页对齐后，在 OffheapPool 中实际占用的物理空间
	Checksum     uint32 // 【安全校验和】: C++ 侧计算的 CRC32 值，用于在落盘前进行端到端完整性防踩坏校验
	Data         []byte // 【堆外内存切片视图】: 指向 OffheapPool 的 RAM 地址，脱离 Go GC 控管，属于严格只读负载
}

// Handler 处理 UDS 层解码后的 BlockReady 消息，并把块内容落到 OffheapPool。
type Handler struct {
	pool *OffheapPool
	shm  *SharedMemoryPool

	lifecycle  sync.RWMutex // 用于控制整个 Handler 生命周期的锁（如 Reset 操作）
	mu         sync.RWMutex // 用于保护 blocks 映射表和 inflight 状态的细粒度锁
	generation uint64       // 分代计数器，用于防止老旧缓存块在 Reset 后被意外加载
	blocks     map[uint64]Block
	inflight   map[uint64]*blockLoad // 记录正在异步加载中的块，用于实现 SingleFlight 去重
	pending    map[uint64]protocol.BlockAllocation
	leases     map[uint64]int      // 每个 block 当前活跃的零拷贝 reader/内部 pin 数量
	evicting   map[uint64]struct{} // 正在被 spill/evict 的 block，阻止新的 reader 进入
}

// blockLoad 结构体用于封装正在加载中的任务信息，支持多协程等待同一数据块。
type blockLoad struct {
	block protocol.BlockReady
	done  chan struct{} // 加载完成通知通道
	err   error         // 记录加载过程中的错误
}

// BlockLease 是对堆外 block 的显式零拷贝借用。
// 持有 lease 期间 Reset/Evict 会等待，调用方必须在读完后调用 Release。
type BlockLease struct {
	handler *Handler
	block   Block
	once    sync.Once
}

type BlockMeta struct {
	ID         uint64
	Seq        uint64
	Generation uint64
	Length     uint64
	Allocated  uint64
	Checksum   uint32
}

type HandlerStats struct {
	Blocks         uint64
	LogicalBytes   uint64
	AllocatedBytes uint64
	PoolUsedBytes  uint64
	PoolSizeBytes  uint64
}

// BlockLeaseReader 把堆外 block lease 暴露成流式 reader。
// Close 会释放底层 lifecycle 读锁；网络层或磁盘层调用方必须及时关闭。
type BlockLeaseReader struct {
	lease  *BlockLease
	block  Block
	reader *bytes.Reader
}

// ImportBlockWriter buffers a remotely fetched block before atomically publishing it
// into the handler. The temporary buffer keeps partially received or corrupt network
// payloads out of the visible in-memory index.
type ImportBlockWriter struct {
	handler  *Handler
	blockID  uint64
	seq      uint64
	expected uint64
	checksum uint32

	buf       bytes.Buffer
	committed bool
}

func (l *BlockLease) Data() []byte {
	if l == nil {
		return nil
	}
	return l.block.Data
}

func (l *BlockLease) Block() Block {
	if l == nil {
		return Block{}
	}
	return l.block
}

func (l *BlockLease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		if l.handler != nil {
			l.handler.releaseLease(l.block.ID)
			l.handler = nil
		}
	})
}

func (l *BlockLease) Close() error {
	l.Release()
	return nil
}

func (r *BlockLeaseReader) Read(p []byte) (int, error) {
	if r == nil || r.reader == nil {
		return 0, fmt.Errorf("storage handler: closed block reader")
	}
	return r.reader.Read(p)
}

func (r *BlockLeaseReader) Close() error {
	if r == nil {
		return nil
	}
	if r.lease != nil {
		r.lease.Release()
		r.lease = nil
	}
	r.reader = nil
	return nil
}

func (r *BlockLeaseReader) BlockID() uint64 {
	if r == nil {
		return 0
	}
	return r.block.ID
}

func (r *BlockLeaseReader) BlockLength() uint64 {
	if r == nil {
		return 0
	}
	return r.block.Length
}

func (r *BlockLeaseReader) BlockChecksum() uint32 {
	if r == nil {
		return 0
	}
	return r.block.Checksum
}

func (w *ImportBlockWriter) Write(p []byte) (int, error) {
	if w == nil {
		return 0, fmt.Errorf("storage handler: nil import writer")
	}
	if w.committed {
		return 0, fmt.Errorf("storage handler: import writer already committed")
	}
	if uint64(w.buf.Len())+uint64(len(p)) > w.expected {
		return 0, fmt.Errorf("storage handler: imported block %d exceeds expected length %d", w.blockID, w.expected)
	}
	return w.buf.Write(p)
}

func (w *ImportBlockWriter) Grow(n int) {
	if w != nil {
		w.buf.Grow(n)
	}
}

func (w *ImportBlockWriter) Commit() (Block, error) {
	if w == nil {
		return Block{}, fmt.Errorf("storage handler: nil import writer")
	}
	if w.committed {
		return Block{}, fmt.Errorf("storage handler: import writer already committed")
	}
	if uint64(w.buf.Len()) != w.expected {
		return Block{}, fmt.Errorf("storage handler: imported block %d short write: expected=%d got=%d", w.blockID, w.expected, w.buf.Len())
	}
	if actual := crc32.ChecksumIEEE(w.buf.Bytes()); actual != w.checksum {
		return Block{}, fmt.Errorf("storage handler: imported block %d checksum mismatch: expected=%d actual=%d", w.blockID, w.checksum, actual)
	}
	block, err := w.handler.importBlock(w.blockID, w.seq, w.expected, w.checksum, w.buf.Bytes())
	if err != nil {
		return Block{}, err
	}
	w.committed = true
	return block, nil
}

func (w *ImportBlockWriter) Rollback() error {
	if w == nil {
		return nil
	}
	w.buf.Reset()
	return nil
}

func NewHandler(pool *OffheapPool) (*Handler, error) {
	if pool == nil {
		return nil, fmt.Errorf("storage handler: nil offheap pool")
	}
	return newHandler(pool, nil)
}

func NewHandlerWithSharedMemory(pool *OffheapPool, shm *SharedMemoryPool) (*Handler, error) {
	if pool == nil {
		return nil, fmt.Errorf("storage handler: nil offheap pool")
	}
	if shm == nil {
		return nil, fmt.Errorf("storage handler: nil shared memory pool")
	}
	return newHandler(pool, shm)
}

func newHandler(pool *OffheapPool, shm *SharedMemoryPool) (*Handler, error) {
	return &Handler{
		pool:     pool,
		shm:      shm,
		blocks:   make(map[uint64]Block),
		inflight: make(map[uint64]*blockLoad),
		pending:  make(map[uint64]protocol.BlockAllocation),
		leases:   make(map[uint64]int),
		evicting: make(map[uint64]struct{}),
	}, nil
}

func (h *Handler) Release() error {
	if h == nil {
		return nil
	}
	var firstErr error
	if h.pool != nil {
		if err := h.pool.Release(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if h.shm != nil {
		if err := h.shm.Release(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (h *Handler) AllocateBlock(ctx context.Context, seq uint64, req protocol.AllocateBlock) (protocol.BlockAllocation, error) {
	if h == nil {
		return protocol.BlockAllocation{}, fmt.Errorf("storage handler: nil handler")
	}
	if h.shm == nil {
		return protocol.BlockAllocation{}, fmt.Errorf("storage handler: daemon-owned shared memory is not configured")
	}
	if err := ctx.Err(); err != nil {
		return protocol.BlockAllocation{}, err
	}
	if req.BlockID == 0 {
		return protocol.BlockAllocation{}, fmt.Errorf("storage handler: zero block id")
	}
	if req.Length == 0 {
		return protocol.BlockAllocation{}, fmt.Errorf("storage handler: zero block length")
	}
	h.lifecycle.RLock()
	defer h.lifecycle.RUnlock()

	h.mu.Lock()
	if _, exists := h.blocks[req.BlockID]; exists {
		h.mu.Unlock()
		return protocol.BlockAllocation{}, fmt.Errorf("storage handler: block %d already exists or is loading", req.BlockID)
	}
	if _, loading := h.inflight[req.BlockID]; loading {
		h.mu.Unlock()
		return protocol.BlockAllocation{}, fmt.Errorf("storage handler: block %d already exists or is loading", req.BlockID)
	}
	if pending, exists := h.pending[req.BlockID]; exists {
		h.mu.Unlock()
		if pending.Length == req.Length {
			return pending, nil
		}
		return protocol.BlockAllocation{}, fmt.Errorf("storage handler: conflicting pending allocation for block %d: existing length=%d requested length=%d", req.BlockID, pending.Length, req.Length)
	}
	h.mu.Unlock()

	allocation, err := h.shm.Allocate(req.Length)
	if err != nil {
		return protocol.BlockAllocation{}, fmt.Errorf("storage handler: allocate daemon shared block %d length=%d: %w", req.BlockID, req.Length, err)
	}
	ready := protocol.BlockAllocation{
		BlockID: req.BlockID,
		ShmName: allocation.ShmName,
		Offset:  allocation.Offset,
		Length:  allocation.Length,
	}

	h.mu.Lock()
	if pending, exists := h.pending[req.BlockID]; exists {
		h.mu.Unlock()
		if pending.Length == req.Length {
			return pending, nil
		}
		return protocol.BlockAllocation{}, fmt.Errorf("storage handler: conflicting pending allocation for block %d: existing length=%d requested length=%d", req.BlockID, pending.Length, req.Length)
	}
	if _, exists := h.blocks[req.BlockID]; exists {
		h.mu.Unlock()
		return protocol.BlockAllocation{}, fmt.Errorf("storage handler: block %d already exists", req.BlockID)
	}
	if _, loading := h.inflight[req.BlockID]; loading {
		h.mu.Unlock()
		return protocol.BlockAllocation{}, fmt.Errorf("storage handler: block %d is loading", req.BlockID)
	}
	h.pending[req.BlockID] = ready
	h.mu.Unlock()
	return ready, nil
}

func (h *Handler) OwnsSharedMemory(shmName string) bool {
	if h == nil || h.shm == nil {
		return false
	}
	return h.shm.Owns(shmName)
}

// HandleBlockReady 实现 ipc.BlockReadyHandler。
// 防并发冲突 -> 挂载外部数据 -> 分配私人空间 -> 极速转移数据 -> 登记入册 -> 释放外部资源 -> 广播唤醒同伴。
func (h *Handler) HandleBlockReady(ctx context.Context, seq uint64, block protocol.BlockReady) (err error) {
	if h == nil {
		return fmt.Errorf("storage handler:nil handler")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// 获取生命周期读锁，确保在此期间不会发生 Reset
	h.lifecycle.RLock()
	defer h.lifecycle.RUnlock()
	// 尝试开始加载，如果是重复块或已有任务则直接获取引用
	generation, load, owner, err := h.beginLoad(block)
	if err != nil {
		return err
	}
	// 如果不是本次任务的负责人，则等待已有任务完成
	if !owner {
		if load == nil {
			return nil
		}
		return h.waitLoad(ctx, load)
	}
	// 确保清理正在进行的加载任务状态
	defer h.finishLoad(block.BlockID, load, &err)

	if err := ctx.Err(); err != nil {
		return err
	}
	loaded, err := h.loadBlockReadyZeroCopy(generation, seq, block)
	if err != nil {
		return err
	}
	return h.commitBlock(generation, block, loaded)
}

func (h *Handler) loadBlockReadyZeroCopy(generation uint64, seq uint64, block protocol.BlockReady) (Block, error) {
	if h.shm != nil && h.shm.Owns(block.ShmName) {
		data, err := h.shm.Slice(block.Offset, block.Length)
		if err != nil {
			return Block{}, err
		}
		if actual := crc32.ChecksumIEEE(data); actual != block.Checksum {
			return Block{}, fmt.Errorf("storage handler: daemon shared block %d checksum mismatch: expected=%d actual=%d", block.BlockID, block.Checksum, actual)
		}
		allocated, _ := alignUp(block.Length, sharedMemoryAlignment)
		return Block{
			ID:           block.BlockID,
			Seq:          seq,
			Generation:   generation,
			ShmName:      block.ShmName,
			SourceOffset: block.Offset,
			Length:       block.Length,
			Allocated:    allocated,
			Checksum:     block.Checksum,
			Data:         data,
		}, nil
	}

	// Backward-compatible path for old clients that still own their POSIX shm.
	// This path copies into daemon offheap because the client may unlink/reset
	// its shm immediately after ACK.
	shmBlock, err := ipc.MapBlockReady(block)
	if err != nil {
		return Block{}, err
	}
	defer shmBlock.Unmap()
	dst, err := h.pool.Alloc(uint64(len(shmBlock.Data)))
	if err != nil {
		return Block{}, fmt.Errorf("storage handler: allocate block %d length=%d: %w", block.BlockID, block.Length, err)
	}
	copy(dst, shmBlock.Data)
	loaded := Block{
		ID:           block.BlockID,
		Seq:          seq,
		Generation:   generation,
		ShmName:      block.ShmName,
		SourceOffset: block.Offset,
		Length:       block.Length,
		Allocated:    uint64(cap(dst)),
		Checksum:     block.Checksum,
		Data:         dst,
	}
	return loaded, nil
}

// Acquire 返回指定 block 的零拷贝 lease。调用方必须在读完后调用 Release。
func (h *Handler) Acquire(blockID uint64) (*BlockLease, bool) {
	if h == nil {
		return nil, false
	}
	h.lifecycle.RLock()
	h.mu.Lock()
	block, ok := h.blocks[blockID]
	if _, evicting := h.evicting[blockID]; !ok || evicting {
		h.mu.Unlock()
		h.lifecycle.RUnlock()
		return nil, false
	}
	h.leases[blockID]++
	h.mu.Unlock()
	return &BlockLease{
		handler: h,
		block:   block,
	}, true
}

func (h *Handler) releaseLease(blockID uint64) {
	h.mu.Lock()
	if count := h.leases[blockID]; count <= 1 {
		delete(h.leases, blockID)
	} else {
		h.leases[blockID] = count - 1
	}
	h.mu.Unlock()
	h.lifecycle.RUnlock()
}

func (h *Handler) Meta(blockID uint64) (BlockMeta, bool) {
	if h == nil {
		return BlockMeta{}, false
	}
	h.mu.RLock()
	block, ok := h.blocks[blockID]
	h.mu.RUnlock()
	if !ok {
		return BlockMeta{}, false
	}
	return blockMeta(block), true
}

func (h *Handler) Stats() HandlerStats {
	if h == nil {
		return HandlerStats{}
	}
	h.mu.RLock()
	defer h.mu.RUnlock()

	stats := HandlerStats{
		Blocks: uint64(len(h.blocks)),
	}
	if h.pool != nil {
		stats.PoolUsedBytes += h.pool.Used()
		stats.PoolSizeBytes += h.pool.Size()
	}
	if h.shm != nil {
		stats.PoolUsedBytes += h.shm.Used()
		stats.PoolSizeBytes += h.shm.Size()
	}
	for _, block := range h.blocks {
		stats.LogicalBytes += block.Length
		stats.AllocatedBytes += block.Allocated
	}
	return stats
}

// Get 返回指定 block 的安全副本。需要零拷贝读时使用 Acquire。
func (h *Handler) Get(blockID uint64) ([]byte, bool) {
	lease, ok := h.Acquire(blockID)
	if !ok {
		return nil, false
	}
	defer lease.Release()

	data := lease.Data()
	copied := make([]byte, len(data))
	copy(copied, data)
	return copied, true
}

// GetBlock 返回指定 block 的元信息和安全数据副本。需要零拷贝读时使用 Acquire。
func (h *Handler) GetBlock(blockID uint64) (Block, bool) {
	lease, ok := h.Acquire(blockID)
	if !ok {
		return Block{}, false
	}
	defer lease.Release()

	block := lease.Block()
	copied := make([]byte, len(block.Data))
	copy(copied, block.Data)
	block.Data = copied
	return block, true
}

// OpenBlock 返回一个零拷贝流式 reader，供网络层发送大块数据。
// reader 持有底层 lease；调用方必须 Close，否则 Reset/Evict 会等待。
func (h *Handler) OpenBlock(blockID uint64) (io.ReadCloser, uint64, uint64, uint32, bool, error) {
	lease, ok := h.Acquire(blockID)
	if !ok {
		return nil, 0, 0, 0, false, nil
	}
	block := lease.Block()
	return &BlockLeaseReader{
		lease:  lease,
		block:  block,
		reader: bytes.NewReader(block.Data),
	}, block.ID, block.Length, block.Checksum, true, nil
}

// NewImportBlockWriter creates a staging writer for a block fetched from another
// daemon over P2P. Call Commit after the network copy succeeds.
func (h *Handler) NewImportBlockWriter(blockID uint64, seq uint64, length uint64, checksum uint32) (*ImportBlockWriter, error) {
	if h == nil {
		return nil, fmt.Errorf("storage handler: nil handler")
	}
	if blockID == 0 {
		return nil, fmt.Errorf("storage handler: zero block id")
	}
	if length == 0 {
		return nil, fmt.Errorf("storage handler: zero block length")
	}
	return &ImportBlockWriter{
		handler:  h,
		blockID:  blockID,
		seq:      seq,
		expected: length,
		checksum: checksum,
	}, nil
}

// Len 返回当前缓存中存储的块数量。
func (h *Handler) Len() int {
	if h == nil {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.blocks)
}

// Reset 清空当前索引并复用底层 OffheapPool。
// 调用方必须保证没有 Goroutine 正在持有或读写旧的 Block.Data。
func (h *Handler) Reset() {
	if h == nil {
		return
	}
	// 使用生命周期写锁确保没有任何加载任务在运行
	h.lifecycle.Lock()
	defer h.lifecycle.Unlock()

	h.mu.Lock()
	defer h.mu.Unlock()

	h.resetLocked(0)
}

// waitLoad 等待正在进行的加载任务。
func (h *Handler) waitLoad(ctx context.Context, load *blockLoad) error {
	select {
	case <-load.done:
		return load.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// finishLoad 完成加载流程并通知所有等待者。
func (h *Handler) finishLoad(blockID uint64, load *blockLoad, errp *error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 设置错误信息，删除 inflight 状态并广播通知
	if current := h.inflight[blockID]; current == load {
		load.err = *errp
		delete(h.inflight, blockID)
		close(load.done)
	}
}

func (h *Handler) resetLocked(blockHint int) {
	h.generation++
	h.blocks = make(map[uint64]Block, blockHint)
	h.inflight = make(map[uint64]*blockLoad)
	h.pending = make(map[uint64]protocol.BlockAllocation)
	h.leases = make(map[uint64]int)
	h.evicting = make(map[uint64]struct{})
	if h.pool != nil {
		h.pool.Reset()
	}
	if h.shm != nil {
		h.shm.Reset()
	}
}

// beginLoad 检查当前块状态，如果需要加载则注册到 inflight 表中。
func (h *Handler) beginLoad(block protocol.BlockReady) (uint64, *blockLoad, bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 检查是否已经加载完成
	if existing, exists := h.blocks[block.BlockID]; exists {
		return h.generation, nil, false, validateExistingBlock(existing, block)
	}
	// 检查是否正在加载中
	if load, exists := h.inflight[block.BlockID]; exists {
		if err := validateReadyMetadata(load.block, block); err != nil {
			return h.generation, nil, false, err
		}
		return h.generation, load, false, nil
	}
	if h.shm != nil && h.shm.Owns(block.ShmName) {
		pending, exists := h.pending[block.BlockID]
		if !exists {
			return h.generation, nil, false, fmt.Errorf("storage handler: missing pending daemon shared allocation for block %d", block.BlockID)
		}
		if err := validatePendingAllocation(pending, block); err != nil {
			return h.generation, nil, false, err
		}
	}

	// 当前协程负责加载
	load := &blockLoad{
		block: block,
		done:  make(chan struct{}),
	}
	h.inflight[block.BlockID] = load
	return h.generation, load, true, nil
}

// commitBlock 将加载完成的块写入索引。
func (h *Handler) commitBlock(generation uint64, incoming protocol.BlockReady, loaded Block) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 确保生成代没有改变，防止 Reset 导致的数据不一致
	if generation != h.generation {
		return fmt.Errorf("storage handler: generation changed while loading block %d", incoming.BlockID)
	}
	if existing, exists := h.blocks[incoming.BlockID]; exists {
		return validateExistingBlock(existing, incoming)
	}

	h.blocks[incoming.BlockID] = loaded
	delete(h.pending, incoming.BlockID)
	return nil
}

func (h *Handler) importBlock(blockID uint64, seq uint64, length uint64, checksum uint32, data []byte) (Block, error) {
	h.lifecycle.RLock()
	defer h.lifecycle.RUnlock()

	generation, load, owner, err := h.beginLoad(protocol.BlockReady{
		BlockID:  blockID,
		Length:   length,
		Checksum: checksum,
	})
	if err != nil {
		return Block{}, err
	}
	if !owner {
		if load == nil {
			block, ok := h.GetBlock(blockID)
			if !ok {
				return Block{}, fmt.Errorf("storage handler: block %d disappeared during import", blockID)
			}
			return block, nil
		}
		if err := h.waitLoad(context.Background(), load); err != nil {
			return Block{}, err
		}
		block, ok := h.GetBlock(blockID)
		if !ok {
			return Block{}, fmt.Errorf("storage handler: block %d missing after concurrent import", blockID)
		}
		return block, nil
	}
	defer h.finishLoad(blockID, load, &err)

	dst, err := h.pool.Alloc(uint64(len(data)))
	if err != nil {
		return Block{}, fmt.Errorf("storage handler: import block %d length=%d: %w", blockID, length, err)
	}
	copy(dst, data)
	loaded := Block{
		ID:         blockID,
		Seq:        seq,
		Generation: generation,
		Length:     length,
		Allocated:  uint64(cap(dst)),
		Checksum:   checksum,
		Data:       dst,
	}
	err = h.commitBlock(generation, protocol.BlockReady{
		BlockID:  blockID,
		Length:   length,
		Checksum: checksum,
	}, loaded)
	if err != nil {
		return Block{}, err
	}
	return loaded, nil
}

func blockMeta(block Block) BlockMeta {
	return BlockMeta{
		ID:         block.ID,
		Seq:        block.Seq,
		Generation: block.Generation,
		Length:     block.Length,
		Allocated:  block.Allocated,
		Checksum:   block.Checksum,
	}
}

// validateExistingBlock 检查已存储块与传入块的元数据一致性。
func validateExistingBlock(existing Block, block protocol.BlockReady) error {
	if existing.Length == block.Length && existing.Checksum == block.Checksum {
		return nil
	}
	return fmt.Errorf(
		"storage handler: conflicting block metadata for block %d: existing length=%d checksum=%d incoming length=%d checksum=%d",
		block.BlockID,
		existing.Length,
		existing.Checksum,
		block.Length,
		block.Checksum,
	)
}

// validateReadyMetadata 检查 inflight 加载任务与传入块的元数据一致性。
func validateReadyMetadata(existing protocol.BlockReady, incoming protocol.BlockReady) error {
	if existing.Length == incoming.Length && existing.Checksum == incoming.Checksum {
		return nil
	}
	return fmt.Errorf(
		"storage handler: conflicting in-flight block metadata for block %d: existing length=%d checksum=%d incoming length=%d checksum=%d",
		incoming.BlockID,
		existing.Length,
		existing.Checksum,
		incoming.Length,
		incoming.Checksum,
	)
}

func validatePendingAllocation(existing protocol.BlockAllocation, incoming protocol.BlockReady) error {
	if existing.BlockID == incoming.BlockID && existing.ShmName == incoming.ShmName && existing.Offset == incoming.Offset && existing.Length == incoming.Length {
		return nil
	}
	return fmt.Errorf(
		"storage handler: block-ready does not match pending allocation for block %d: pending shm=%s offset=%d length=%d incoming shm=%s offset=%d length=%d",
		incoming.BlockID,
		existing.ShmName,
		existing.Offset,
		existing.Length,
		incoming.ShmName,
		incoming.Offset,
		incoming.Length,
	)
}
