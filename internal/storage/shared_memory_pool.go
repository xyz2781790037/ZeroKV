package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

const (
	sharedMemoryAlignment  uint64 = 64
	sharedMemoryRoot              = "/dev/shm"
	maxSharedMemoryNameLen        = 255
)

type SharedMemoryAllocation struct {
	ShmName   string
	Offset    uint64
	Length    uint64
	Allocated uint64
	Data      []byte
}

// SharedMemoryPool is a daemon-owned POSIX shm arena. C++ clients ask the
// daemon for slots, mmap the returned coordinates, write bytes there, and then
// submit BlockReady. The daemon stores the same mapped slice directly.
type SharedMemoryPool struct {
	name string
	path string
	file *os.File
	data []byte
	size uint64

	mu     sync.Mutex
	offset uint64
}

func NewSharedMemoryPool(name string, size uint64) (*SharedMemoryPool, error) {
	if size == 0 {
		return nil, fmt.Errorf("shared memory pool: size cannot be zero")
	}
	cleanName, err := normalizeSharedMemoryName(name)
	if err != nil {
		return nil, err
	}
	alignedSize, ok := alignUp(size, uint64(os.Getpagesize()))
	if !ok || alignedSize > maxMmapLen() {
		return nil, fmt.Errorf("shared memory pool: size too large: %d", size)
	}

	path := filepath.Join(sharedMemoryRoot, cleanName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0660)
	if err != nil {
		return nil, fmt.Errorf("shared memory pool: open %s: %w", path, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = file.Close()
			_ = os.Remove(path)
		}
	}()

	if err := file.Truncate(int64(alignedSize)); err != nil {
		return nil, fmt.Errorf("shared memory pool: truncate %s: %w", path, err)
	}
	data, err := syscall.Mmap(int(file.Fd()), 0, int(alignedSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("shared memory pool: mmap %s: %w", path, err)
	}
	committed = true
	return &SharedMemoryPool{
		name: cleanName,
		path: path,
		file: file,
		data: data,
		size: alignedSize,
	}, nil
}

func (p *SharedMemoryPool) Allocate(length uint64) (SharedMemoryAllocation, error) {
	if p == nil {
		return SharedMemoryAllocation{}, fmt.Errorf("shared memory pool: nil pool")
	}
	if length == 0 {
		return SharedMemoryAllocation{}, fmt.Errorf("shared memory pool: zero allocation length")
	}
	allocated, ok := alignUp(length, sharedMemoryAlignment)
	if !ok {
		return SharedMemoryAllocation{}, ErrOOM
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.data == nil {
		return SharedMemoryAllocation{}, fmt.Errorf("shared memory pool: released")
	}
	offset, ok := alignUp(p.offset, sharedMemoryAlignment)
	if !ok || offset > p.size || allocated > p.size-offset {
		return SharedMemoryAllocation{}, ErrOOM
	}
	p.offset = offset + allocated
	return SharedMemoryAllocation{
		ShmName:   p.name,
		Offset:    offset,
		Length:    length,
		Allocated: allocated,
		Data:      p.data[int(offset):int(offset+length):int(offset+allocated)],
	}, nil
}

func (p *SharedMemoryPool) Slice(offset uint64, length uint64) ([]byte, error) {
	if p == nil {
		return nil, fmt.Errorf("shared memory pool: nil pool")
	}
	if length == 0 {
		return nil, fmt.Errorf("shared memory pool: zero slice length")
	}
	end, err := checkedSharedAdd(offset, length)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.data == nil {
		return nil, fmt.Errorf("shared memory pool: released")
	}
	if end > p.size {
		return nil, fmt.Errorf("shared memory pool: range exceeds pool size: offset=%d length=%d size=%d", offset, length, p.size)
	}
	return p.data[int(offset):int(end)], nil
}

func (p *SharedMemoryPool) Owns(name string) bool {
	if p == nil {
		return false
	}
	cleanName := strings.TrimPrefix(name, "/")
	return cleanName == p.name
}

func (p *SharedMemoryPool) Name() string {
	if p == nil {
		return ""
	}
	return p.name
}

func (p *SharedMemoryPool) Size() uint64 {
	if p == nil {
		return 0
	}
	return p.size
}

func (p *SharedMemoryPool) Used() uint64 {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.offset
}

func (p *SharedMemoryPool) Reset() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.offset = 0
	p.mu.Unlock()
}

func (p *SharedMemoryPool) Release() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	data := p.data
	p.data = nil
	file := p.file
	p.file = nil
	path := p.path
	p.offset = 0
	p.size = 0
	p.mu.Unlock()

	var firstErr error
	if data != nil {
		if err := syscall.Munmap(data); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if file != nil {
		if err := file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if path != "" {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func normalizeSharedMemoryName(name string) (string, error) {
	if name == "" {
		name = fmt.Sprintf("kvcache_daemon_%d", os.Getpid())
	}
	if strings.ContainsRune(name, '\x00') {
		return "", fmt.Errorf("shared memory pool: name contains NUL byte")
	}
	name = strings.TrimPrefix(name, "/")
	if name == "" || name == "." || name == ".." || len(name) > maxSharedMemoryNameLen || strings.Contains(name, "/") || filepath.Clean(name) != name {
		return "", fmt.Errorf("shared memory pool: invalid name %q", name)
	}
	return name, nil
}

func checkedSharedAdd(a uint64, b uint64) (uint64, error) {
	if a > ^uint64(0)-b {
		return 0, fmt.Errorf("shared memory pool: range overflows uint64: offset=%d length=%d", a, b)
	}
	return a + b, nil
}
