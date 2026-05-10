package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
)

const (
	diskBlockMagic   uint32 = 0x4b564344 // "DCVK" in little endian bytes.
	diskBlockVersion uint16 = 1
	diskBlockHeader  int    = 48
)

var ErrDiskBlockNotFound = errors.New("disk tier: block not found")

// 它是整个落盘模块的核心入口。大模型推理时内存极度宝贵，装不下的 KV Cache 会被驱逐（Evict）到本地磁盘上，DiskTier 就是管这些落在磁盘上的文件的。
type DiskTier struct {
	root         string                   // 【根目录路径】: 磁盘层存储数据的绝对或相对根路径，所有 .kvblk 文件存放于此。
	mu           sync.RWMutex             // 【并发控制锁】: 保护 blocks 路由表和 invalidFiles 计数器的读写安全，确保并发访问不引发 data race。
	blocks       map[uint64]DiskBlockMeta // 【内存索引表】: O(1) 路由字典，BlockID 映射到磁盘文件的元信息。启动时重建，内存中只存元数据不存实体。
	invalidFiles uint64                   // 【损坏文件计数】: 记录在扫盘加载或运行期间发现的损坏、截断或魔数错误的脏文件总数，用于监控告警(Metrics)。
}

// 它是 DiskTier 路由表里的 Value。

// 核心价值：控制面与数据面分离。一个大模型张量块可能有 500MB，但这个 Meta 结构体只占几十个字节。我们把 500MB 的实体放在磁盘，把这几十个字节的 Meta 留在内存里。里面包含了精准的文件路径（Path）用于打开文件，以及极其关键的 Checksum 用于在文件系统发生“静默损坏（Bit Rot）”时进行拦截验证。
type DiskBlockMeta struct {
	ID       uint64 // 【全局唯一标识】: Block 的 ID，也是生成的本地临时文件和持久化文件名的哈希依据。
	Seq      uint64 // 【数据序列号】: 用于多版本并发控制(MVCC)或数据更新的先后防乱序校验。
	Length   uint64 // 【有效载荷长度】: 实际张量数据(Data)的字节长度，不包含 48 字节文件 Header 的大小。
	Checksum uint32 // 【端到端校验和】: 写入前生成的 CRC32 哈希，读取时通过严格校验防文件系统静默损坏(Bit Rot)。
	Path     string // 【物理绝对路径】: 该块数据在操作系统文件系统中的完整文件路径，用于 os.Open 快速定位。
}

type DiskTierStats struct {
	Blocks       uint64 // 【有效块总数】: 当前磁盘层正常管理的有效 KV 块的数量 (对应 len(blocks))。
	Bytes        uint64 // 【落盘总容量】: 当前磁盘层存放的所有有效块有效载荷(不含 Header)的总物理字节数。
	InvalidFiles uint64 // 【失效/脏文件数】: 监控指标透出，反映底层磁盘可靠性或进程崩溃造成的遗留脏文件数量。
}

type DiskBlockReader struct {
	meta DiskBlockMeta // 【块元数据快照】: 记录当前正在读取文件的预期校验和与长度，用于流式读取结束后的防伪比对。
	file *os.File      // 【底层文件句柄】: 操作系统级别的文件描述符(FD)，持有磁盘 I/O 上下文，调用方用完后必须显式 Close。
}

func (r *DiskBlockReader) Meta() DiskBlockMeta {
	if r == nil {
		return DiskBlockMeta{}
	}
	return r.meta
}
func (r *DiskBlockReader) BlockID() uint64 {
	if r == nil {
		return 0
	}
	return r.meta.ID
}
func (r *DiskBlockReader) BlockLength() uint64 {
	if r == nil {
		return 0
	}
	return r.meta.Length
}
func (r *DiskBlockReader) BlockChecksum() uint32 {
	if r == nil {
		return 0
	}
	return r.meta.Checksum
}
func (r *DiskBlockReader) Read(p []byte) (int, error) {
	if r == nil || r.file == nil {
		return 0, os.ErrClosed
	}
	return r.file.Read(p)
}
func (r *DiskBlockReader) Close() error {
	if r == nil || r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}
func NewDiskTier(root string) (*DiskTier, error) {
	if root == "" {
		return nil, fmt.Errorf("disk tier: empty root")
	}
	cleanRoot := filepath.Clean(root)
	if err := os.MkdirAll(cleanRoot, 0750); err != nil {
		return nil, fmt.Errorf("disk tier: create root %s: %w", cleanRoot, err)
	}
	tier := &DiskTier{
		root:   cleanRoot,
		blocks: make(map[uint64]DiskBlockMeta),
	}
	if err := tier.loadIndex(); err != nil {
		return nil, err
	}
	return tier, nil
}
func (d *DiskTier) Put(block Block) error {
	if d == nil {
		return fmt.Errorf("disk tier: nil tier")
	}
	if block.ID == 0 {
		return fmt.Errorf("disk tier: zero block id")
	}
	if uint64(len(block.Data)) != block.Length {
		return fmt.Errorf("disk tier: block %d length mismatch: meta=%d data=%d", block.ID, block.Length, len(block.Data))
	}
	if actual := crc32.ChecksumIEEE(block.Data); actual != block.Checksum {
		return fmt.Errorf("disk tier: block %d checksum mismatch: expected=%d actual=%d", block.ID, block.Checksum, actual)
	}
	path := d.blockPath(block.ID)
	tmp, err := os.CreateTemp(d.root, fmt.Sprintf("%016x-*.tmp", block.ID))
	if err != nil {
		return fmt.Errorf("disk tier: create temp block %d: %w", block.ID, err)
	}
	tmpPath := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := writeDiskBlock(tmp, block); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("disk tier: sync temp block %d: %w", block.ID, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("disk tier: close temp block %d: %w", block.ID, err)
	}
	// os.Rename(tmpPath, path)：原子可见（单机事务提交）
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("disk tier: commit block %d: %w", block.ID, err)
	}
	committed = true
	// syncDir(d.root)：目录刷盘（硬核防掉电保障）
	if err := syncDir(d.root); err != nil {
		return fmt.Errorf("disk tier: sync root after commit block %d: %w", block.ID, err)
	}
	d.mu.Lock()
	d.blocks[block.ID] = DiskBlockMeta{
		ID:       block.ID,
		Seq:      block.Seq,
		Length:   block.Length,
		Checksum: block.Checksum,
		Path:     path,
	}
	d.mu.Unlock()

	return nil
}
func (d *DiskTier) Get(blockID uint64) ([]byte, bool, error) {
	reader, ok, err := d.Open(blockID)
	if !ok || err != nil {
		return nil, ok, err
	}
	defer reader.Close()
	meta := reader.Meta()
	if meta.Length > uint64(maxInt()) {
		return nil, false, fmt.Errorf("disk tier: block %d too large to read: %d", blockID, meta.Length)
	}
	data := make([]byte, int(meta.Length))
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, false, fmt.Errorf("disk tier: read data block %d: %w", blockID, err)
	}
	if actual := crc32.ChecksumIEEE(data); actual != meta.Checksum {
		return nil, false, fmt.Errorf("disk tier: block %d checksum mismatch: expected=%d actual=%d", blockID, meta.Checksum, actual)
	}
	return data, true, nil
}
func (d *DiskTier) Open(blockID uint64) (*DiskBlockReader, bool, error) {
	meta, ok := d.Meta(blockID)
	if !ok {
		return nil, false, nil
	}
	file, err := os.Open(meta.Path)
	if errors.Is(err, os.ErrNotExist) {
		d.dropMeta(blockID)
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("disk tier: open block %d: %w", blockID, err)
	}
	fileMeta, err := readDiskBlockHeader(file, meta.Path)
	if err != nil {
		_ = file.Close()
		return nil, false, err
	}
	if err := validateMeta(meta, fileMeta); err != nil {
		_ = file.Close()
		return nil, false, err
	}
	return &DiskBlockReader{meta: fileMeta, file: file}, true, nil
}
func (d *DiskTier) OpenBlock(blockID uint64) (io.ReadCloser, uint64, uint64, uint32, bool, error) {
	reader, ok, err := d.Open(blockID)
	if !ok || err != nil {
		return nil, 0, 0, 0, ok, err
	}
	meta := reader.Meta()
	return reader, meta.ID, meta.Length, meta.Checksum, true, nil
}
func (d *DiskTier) Delete(blockID uint64) error {
	if d == nil {
		return fmt.Errorf("disk tier: nil tier")
	}
	d.mu.Lock()
	meta, ok := d.blocks[blockID]
	if ok {
		delete(d.blocks, blockID)
	}
	d.mu.Unlock()
	if !ok {
		return nil
	}
	if err := os.Remove(meta.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("disk tier: delete block %d: %w", blockID, err)
	}
	if err := syncDir(d.root); err != nil {
		return fmt.Errorf("disk tier: sync root after delete block %d: %w", blockID, err)
	}
	return nil
}
func (d *DiskTier) Stats() DiskTierStats {
	if d == nil {
		return DiskTierStats{}
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	stats := DiskTierStats{
		Blocks:       uint64(len(d.blocks)),
		InvalidFiles: d.invalidFiles,
	}
	for _, meta := range d.blocks {
		stats.Bytes += meta.Length
	}
	return stats
}

func (d *DiskTier) ListMeta() []DiskBlockMeta {
	if d == nil {
		return nil
	}
	d.mu.RLock()
	metas := make([]DiskBlockMeta, 0, len(d.blocks))
	for _, meta := range d.blocks {
		metas = append(metas, meta)
	}
	d.mu.RUnlock()
	sort.Slice(metas, func(i, j int) bool {
		return metas[i].ID < metas[j].ID
	})
	return metas
}

func (d *DiskTier) loadIndex() error {
	entries, err := os.ReadDir(d.root)
	if err != nil {
		return fmt.Errorf("disk tier: scan root %s: %w", d.root, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".kvblk") {
			continue
		}
		path := filepath.Join(d.root, entry.Name())
		meta, err := readDiskBlockHeaderFile(path)
		if err != nil {
			d.invalidFiles++
			continue
		}
		d.blocks[meta.ID] = meta
	}
	return nil
}
func readDiskBlockHeaderFile(path string) (DiskBlockMeta, error) {
	file, err := os.Open(path)
	if err != nil {
		return DiskBlockMeta{}, fmt.Errorf("disk tier: open header %s: %w", path, err)
	}
	defer file.Close()
	return readDiskBlockHeader(file, path)
}

func (d *DiskTier) Has(blockID uint64) bool {
	if d == nil {
		return false
	}
	d.mu.RLock()
	_, ok := d.blocks[blockID]
	d.mu.RUnlock()
	return ok
}

func (d *DiskTier) Meta(blockID uint64) (DiskBlockMeta, bool) {
	if d == nil {
		return DiskBlockMeta{}, false
	}
	d.mu.RLock()
	meta, ok := d.blocks[blockID]
	d.mu.RUnlock()
	return meta, ok
}
func maxInt() int {
	return int(^uint(0) >> 1)
}

func (d *DiskTier) blockPath(blockID uint64) string {
	return filepath.Join(d.root, fmt.Sprintf("%016x.kvblk", blockID))
}

func (d *DiskTier) dropMeta(blockID uint64) {
	d.mu.Lock()
	delete(d.blocks, blockID)
	d.mu.Unlock()
}
func writeDiskBlock(w io.Writer, block Block) error {
	header := make([]byte, diskBlockHeader)
	binary.LittleEndian.PutUint32(header[0:4], diskBlockMagic)
	binary.LittleEndian.PutUint16(header[4:6], diskBlockVersion)
	binary.LittleEndian.PutUint16(header[6:8], uint16(diskBlockHeader))
	binary.LittleEndian.PutUint64(header[8:16], block.ID)
	binary.LittleEndian.PutUint64(header[16:24], block.Seq)
	binary.LittleEndian.PutUint64(header[24:32], block.Length)
	binary.LittleEndian.PutUint32(header[32:36], block.Checksum)

	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("disk tier: write header block %d: %w", block.ID, err)
	}
	if _, err := w.Write(block.Data); err != nil {
		return fmt.Errorf("disk tier: write data block %d: %w", block.ID, err)
	}
	return nil
}
func readDiskBlockHeader(r io.Reader, path string) (DiskBlockMeta, error) {
	header := make([]byte, diskBlockHeader)
	if _, err := io.ReadFull(r, header); err != nil {
		return DiskBlockMeta{}, fmt.Errorf("disk tier: read header %s: %w", path, err)
	}
	// 校验魔数 (Magic Number)
	if got := binary.LittleEndian.Uint32(header[0:4]); got != diskBlockMagic {
		return DiskBlockMeta{}, fmt.Errorf("disk tier: invalid magic in %s: 0x%08x", path, got)
	}
	// 校验版本号 (Version)
	if got := binary.LittleEndian.Uint16(header[4:6]); got != diskBlockVersion {
		return DiskBlockMeta{}, fmt.Errorf("disk tier: unsupported version in %s: %d", path, got)
	}
	// 校验 Header 长度
	if got := binary.LittleEndian.Uint16(header[6:8]); int(got) != diskBlockHeader {
		return DiskBlockMeta{}, fmt.Errorf("disk tier: invalid header size in %s: %d", path, got)
	}
	return DiskBlockMeta{
		ID:       binary.LittleEndian.Uint64(header[8:16]),
		Seq:      binary.LittleEndian.Uint64(header[16:24]),
		Length:   binary.LittleEndian.Uint64(header[24:32]),
		Checksum: binary.LittleEndian.Uint32(header[32:36]),
		Path:     path,
	}, nil
}

// 防御性编程和“元数据强一致性校验”机制。
func validateMeta(expected DiskBlockMeta, actual DiskBlockMeta) error {
	if expected.ID != actual.ID ||
		expected.Seq != actual.Seq ||
		expected.Length != actual.Length ||
		expected.Checksum != actual.Checksum {
		return fmt.Errorf(
			"disk tier: block metadata changed while reading block %d: expected seq=%d length=%d checksum=%d got id=%d seq=%d length=%d checksum=%d",
			expected.ID,
			expected.Seq,
			expected.Length,
			expected.Checksum,
			actual.ID,
			actual.Seq,
			actual.Length,
			actual.Checksum,
		)
	}
	return nil
}
func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
			return nil
		}
		return err
	}
	return nil
}
