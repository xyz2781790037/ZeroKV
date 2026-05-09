package storage

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"syscall"
)

var (
	// ErrOOM 表示堆外内存池已耗尽
	ErrOOM = errors.New("offheap pool: out of memory")
)

const allocationAlignment uint64 = 8

// OffheapPool 实现了一个基于 mmap 匿名映射的无锁 Bump-Pointer (撞针) 内存池。
// 它完全脱离 Go Runtime 的 GC 管辖，专为极高吞吐的零拷贝和元数据分配设计。
type OffheapPool struct {
	data   []byte
	size   uint64
	offset uint64 // 原子操作游标，记录当前分配位置
}

// NewOffheapPool 向操作系统直接申请一块连续的堆外物理内存。
func NewOffheapPool(size uint64) (*OffheapPool, error) {
	if size == 0 {
		return nil, fmt.Errorf("pool size cannot be zero")
	}

	// 1. 强制按操作系统页大小进行向上对齐取整
	pageSize := uint64(os.Getpagesize())
	alignedSize, ok := alignUp(size, pageSize)
	if !ok {
		return nil, fmt.Errorf("pool size overflows after page alignment: %d", size)
	}
	if alignedSize > maxMmapLen() {
		return nil, fmt.Errorf("pool size exceeds maximum mmap length: %d", alignedSize)
	}

	// 2. 申请匿名物理内存 (MAP_ANON | MAP_PRIVATE)
	// - MAP_ANON: 不关联磁盘文件，纯粹的 RAM。
	// - MAP_PRIVATE: 进程私有，不与其他进程共享。
	data, err := syscall.Mmap(-1, 0, int(alignedSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap anonymous memory for pool: %w", err)
	}

	return &OffheapPool{
		data:   data,
		size:   alignedSize,
		offset: 0,
	}, nil
}

// Alloc 从堆外内存池中无锁分配指定大小的内存切片。
func (p *OffheapPool) Alloc(n uint64) ([]byte, error) {
	if n == 0 {
		return nil, nil
	}

	// 1. 强制 8 字节内存对齐 (Bitwise Alignment)
	// 防止由于 CPU 缓存行未对齐或 ARM 等精简指令集架构导致的非法内存访问宕机。
	// 例如：请求 13 字节，(13+7) &^ 7 = 16 字节
	allocSize, ok := alignUp(n, allocationAlignment)
	if !ok {
		return nil, ErrOOM
	}

	// 2. 自旋锁与 CAS (Compare-And-Swap) 无锁抢占
	for {
		currentOffset := atomic.LoadUint64(&p.offset)

		// 边界检查：防止溢出
		if currentOffset > p.size || allocSize > p.size-currentOffset {
			return nil, ErrOOM
		}
		newOffset := currentOffset + allocSize

		// 原子级竞争：如果 currentOffset 没被其他 Goroutine 改掉，就更新为 newOffset
		// 向 CPU 下达了一个绝对原子的不可分割的任务
		if atomic.CompareAndSwapUint64(&p.offset, currentOffset, newOffset) {
			// 截取成功。返回实际需要的长度 n 给上层业务，但底层物理指针已向前推进了 allocSize
			start := int(currentOffset)
			end := int(newOffset)
			requestedEnd := int(currentOffset + n)
			return p.data[start:requestedEnd:end], nil
		}
		// 竞争失败，继续下一次循环 (Spin)
	}
}

// Reset 瞬间清空整个内存池。
// 警告：调用此方法时，必须从生命周期层面保证没有任何 Goroutine 正在持有或读写之前分配的内存。
func (p *OffheapPool) Reset() {
	atomic.StoreUint64(&p.offset, 0)
}

func (p *OffheapPool) Size() uint64 {
	if p == nil {
		return 0
	}
	return p.size
}

func (p *OffheapPool) Used() uint64 {
	if p == nil {
		return 0
	}
	return atomic.LoadUint64(&p.offset)
}

// Release 将整块物理内存归还给操作系统。
// 警告：调用此方法时，必须保证没有任何 Goroutine 正在调用 Alloc/Reset 或持有/读写之前分配的内存。
func (p *OffheapPool) Release() error {
	if p.data == nil {
		return nil
	}
	// 彻底断开页表映射，将内存归还给内核
	if err := syscall.Munmap(p.data); err != nil {
		return err
	}
	p.data = nil
	p.size = 0
	atomic.StoreUint64(&p.offset, 0)
	return nil
}

func alignUp(n, align uint64) (uint64, bool) {
	if align == 0 {
		return 0, false
	}
	remainder := n % align
	if remainder == 0 {
		return n, true
	}
	padding := align - remainder
	if n > ^uint64(0)-padding {
		return 0, false
	}
	return n + padding, true
}

// func alignUp(n, align uint64) (uint64, bool) {
// 	if align == 0 {
// 		return 0, false
// 	}
// 	// 验证 align 是否为 2 的幂次方 (例如 8, 4096)
// 	// 二进制特性：一个数如果是 2 的幂，它与自己减一的按位与结果必然为 0
// 	if align&(align-1) != 0 {
// 		return 0, false
// 	}

// 	// 溢出检查：防止 n + (align - 1) 导致 uint64 绕回
// 	if n > ^uint64(0)-(align-1) {
// 		return 0, false
// 	}

//		// 核心位运算：
//		// (n + align - 1) &^ (align - 1)
//		// &^ 是 Go 的位清零操作符 (Bit Clear)。这等价于将低位全部抹零，实现极速的向上对齐。
//		return (n + align - 1) &^ (align - 1), true
//	}
func maxMmapLen() uint64 {
	return uint64(int(^uint(0) >> 1))
}
