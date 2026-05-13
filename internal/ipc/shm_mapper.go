package ipc

import (
	"fmt"
	"hash/crc32"
	"kvcache/pkg/protocol"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const devShmDir = "/dev/shm"

// ShmBlock 拥有一个共享内存区域的 mmap 视图。
// Data 切片指向请求的精准块范围，而 mapping 保留了必须传递给 Munmap 的
// 完整的、页对齐的 mmap 原始切片。
type ShmBlock struct {
	// Name 是共享内存的名称（例如："mock_kv_block_1"），通常与 C++ 侧约定好，不含路径。
	Name string
	// Path 是共享内存文件在操作系统中的完整绝对路径（例如："/dev/shm/mock_kv_block_1"）。
	Path string
	// Data 是暴露给业务层直接使用的核心数据切片（零拷贝的精髓）。
	// 它是对底层 mapping 的精准二次切片，去除了页对齐产生的前置多余字节，长度严格等于 Size。
	Data []byte
	// Offset 是业务层请求的逻辑偏移量（对应整个共享内存文件的起始读取位置）。
	Offset uint64
	// Size 是业务层请求的真实数据块大小（字节数）。
	Size uint64
	// Checksum 是 C++ 侧计算并发送过来的 CRC32 校验和，用于验证这块物理内存中的数据完整性。
	Checksum uint32
	// mapping (私有字段) 是底层 syscall.Mmap 实际返回的完整、严格按系统页对齐的原始字节切片。
	// 极其重要：由于系统调用的限制，执行 syscall.Munmap 释放内存时，必须且只能传入这个原始切片。
	mapping []byte
	// mapOffset (私有字段) 是底层 mmap 系统调用实际使用的、经过向下取整（页对齐）后的真实物理偏移量。
	// Offset - mapOffset 就是 Data 在 mapping 切片中的起始索引。
	mapOffset uint64
}

// MapBlockReady 映射由 BlockReady 控制消息描述的内存块，
// 并在将其返回给调用者之前验证其 CRC32 校验和。
func MapBlockReady(block protocol.BlockReady) (*ShmBlock, error) {
	shmBlock, err := mapSharedMemoryRange(block.ShmName, block.Offset, block.Length, block.Checksum, true)
	if err != nil {
		return nil, err
	}
	return shmBlock, nil
}

// MapSharedMemory 映射现有 POSIX 共享内存对象的前 size 个字节。
// 在处理网络协议消息时应优先使用 MapBlockReady，因为它会尊重 offset/length 并验证校验和。
func MapSharedMemory(shmName string, size uint64) (*ShmBlock, error) {
	return mapSharedMemoryRange(shmName, 0, size, 0, false)
}
func mapSharedMemoryRange(shmName string, offset uint64, length uint64, checksum uint32, verifyChecksum bool) (*ShmBlock, error) {
	if length == 0 {
		return nil, fmt.Errorf("shared memory length cannot be zero") // 共享内存长度不能为零
	}
	name, shmPath, err := normalizeShmName(shmName)
	if err != nil {
		return nil, err
	}
	end, err := checkedAdd(offset, length)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(shmPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open shared memory file %s: %w", shmPath, err) // 无法打开共享内存文件
	}
	defer file.Close()
	if err := ensureFileCovers(file, shmPath, end); err != nil {
		return nil, err
	}
	// 获取系统的物理页大小
	pageSize := uint64(os.Getpagesize())
	//最近的合理物理页
	mapOffset := offset / pageSize * pageSize
	// 实际调用点，前边是为了对齐
	dataStart := offset - mapOffset
	//重新修正要申请的内存总长度（需要加上对齐的）
	mapLength := dataStart + length
	mmapLength, err := checkedMmapLength(mapLength)
	if err != nil {
		return nil, err
	}
	if mapOffset > math.MaxInt64 {
		return nil, fmt.Errorf("shared memory offset too large to mmap: %d", mapOffset) // 共享内存偏移量过大，无法进行 mmap
	}
	// 4是硬件级别的只读保护。5意思是这块内存映射是共享的。
	mapping, err := syscall.Mmap(int(file.Fd()), int64(mapOffset), mmapLength, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap shared memory %s offset=%d length=%d: %w", shmPath, mapOffset, mapLength, err) // mmap 共享内存失败
	}
	dataEnd := dataStart + length
	shmBlock := &ShmBlock{
		Name:      name,
		Path:      shmPath,
		Data:      mapping[int(dataStart):int(dataEnd)],
		Offset:    offset,
		Size:      length,
		Checksum:  checksum,
		mapping:   mapping,
		mapOffset: mapOffset,
	}
	if verifyChecksum {
		if err := shmBlock.VerifyChecksum(); err != nil {
			_ = shmBlock.Unmap() // 校验失败时立即清理现场
			return nil, err
		}
	}
	return shmBlock, nil
}

// VerifyChecksum 将映射的块字节与 C++ 端发送的 CRC32 校验和进行对比验证。
func (b *ShmBlock) VerifyChecksum() error {
	if b == nil || b.Data == nil {
		return fmt.Errorf("shared memory block is not mapped") // 共享内存块未映射
	}
	// 把任意长度的二进制字节流（b.Data），压缩并计算成一个固定的、独一无二的 32 位无符号整数（4 字节，uint32）。
	actual := crc32.ChecksumIEEE(b.Data)
	if actual != b.Checksum {
		return fmt.Errorf("shared memory checksum mismatch: expected=%d actual=%d", b.Checksum, actual) // 共享内存校验和不匹配
	}
	return nil
}

// Unmap 释放 mmap 视图。多次调用是安全的（但非并发安全）。
func (b *ShmBlock) Unmap() error {
	if b == nil || b.mapping == nil {
		return nil
	}
	err := syscall.Munmap(b.mapping) // 必须传入原始的、页对齐的 mapping 切片
	b.mapping = nil
	b.Data = nil
	return err
}
func normalizeShmName(shmName string) (string, string, error) {
	if shmName == "" {
		return "", "", fmt.Errorf("shared memory name cannot be empty") // 共享内存名称不能为空
	}
	// 致命的 NUL 字节截断防御
	if strings.ContainsRune(shmName, '\x00') {
		return "", "", fmt.Errorf("shared memory name contains NUL byte") // 共享内存名称包含 NUL 字节
	}
	name := strings.TrimPrefix(shmName, "/")
	if name == "" || name == "." || name == ".." {
		return "", "", fmt.Errorf("invalid shared memory name %q", shmName) // 无效的共享内存名称
	}
	if len(name) > protocol.MaxShmNameLen {
		return "", "", fmt.Errorf("shared memory name too long: %d", len(name)) // 共享内存名称过长
	}
	if strings.Contains(name, "/") {
		return "", "", fmt.Errorf("shared memory name must not contain path separators: %q", shmName) // 共享内存名称不能包含路径分隔符
	}
	// 防止诸如 a/../b 这样的绕过
	if filepath.Clean(name) != name {
		return "", "", fmt.Errorf("invalid shared memory name %q", shmName) // 无效的共享内存名称
	}

	return name, filepath.Join(devShmDir, name), nil
}

// 检查是否超出范围
func checkedAdd(a uint64, b uint64) (uint64, error) {
	if a > math.MaxUint64-b {
		return 0, fmt.Errorf("shared memory range overflows uint64: offset=%d length=%d", a, b) // 共享内存范围 uint64 溢出
	}
	return a + b, nil
}
func ensureFileCovers(file *os.File, path string, requiredEnd uint64) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat shared memory file %s: %w", path, err) // 获取共享内存文件状态失败
	}
	if info.Size() < 0 {
		return fmt.Errorf("shared memory file %s has invalid size %d", path, info.Size()) // 共享内存文件大小无效
	}
	if requiredEnd > uint64(info.Size()) {
		return fmt.Errorf("shared memory range exceeds file size: path=%s required_end=%d file_size=%d", path, requiredEnd, info.Size()) // 共享内存范围超出文件大小
	}
	return nil
}

// 判断在当前运行的 CPU 架构下，能不能安全地转换成标准的 int 类型。
func checkedMmapLength(length uint64) (int, error) {
	// 获取当前架构下 int 类型的最大值
	// uint(0)：创建一个无符号整型 0。在 32 位系统下，它的二进制是 32 个 0
	// ^按位区反再右1位，就是当前系统最大值
	maxInt := uint64(^uint(0) >> 1)
	if length > maxInt {
		return 0, fmt.Errorf("shared memory mmap length too large: %d", length) // 共享内存 mmap 长度过大
	}
	return int(length), nil
}

// 它用于验证块是否被填满了 0xAB。生产代码应当使用 VerifyChecksum。
func (b *ShmBlock) VerifyMockData() bool {
	if b == nil || len(b.Data) == 0 {
		return false
	}
	head := b.Data[0]
	mid := b.Data[len(b.Data)/2]
	tail := b.Data[len(b.Data)-1]
	return head == 0xAB && mid == 0xAB && tail == 0xAB
}
