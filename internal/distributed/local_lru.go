package distributed

import (
	"sync"

	"kvcache/internal/storage"
)

type localMemoryEntry struct {
	meta storage.BlockMeta
	prev *localMemoryEntry
	next *localMemoryEntry
}

// localMemoryLRU tracks only this daemon's memory-tier blocks. The global
// radix router still decides blockID -> node; this list only decides which
// local memory replica should spill to the node's disk tier first.
type localMemoryLRU struct {
	mu      sync.Mutex
	entries map[uint64]*localMemoryEntry
	head    *localMemoryEntry
	tail    *localMemoryEntry
	bytes   uint64
}

func newLocalMemoryLRU() *localMemoryLRU {
	return &localMemoryLRU{
		entries: make(map[uint64]*localMemoryEntry),
	}
}

func (l *localMemoryLRU) touch(meta storage.BlockMeta) {
	if l == nil || meta.ID == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	if entry := l.entries[meta.ID]; entry != nil {
		l.bytes -= entry.meta.Allocated
		entry.meta = meta
		l.bytes += meta.Allocated
		l.moveToFront(entry)
		return
	}
	entry := &localMemoryEntry{meta: meta}
	l.entries[meta.ID] = entry
	l.bytes += meta.Allocated
	l.pushFront(entry)
}

func (l *localMemoryLRU) touchID(blockID uint64) {
	if l == nil || blockID == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	if entry := l.entries[blockID]; entry != nil {
		l.moveToFront(entry)
	}
}

func (l *localMemoryLRU) remove(blockID uint64) {
	if l == nil || blockID == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := l.entries[blockID]
	if entry == nil {
		return
	}
	l.unlink(entry)
	delete(l.entries, blockID)
	l.bytes -= entry.meta.Allocated
}

func (l *localMemoryLRU) coldest() (storage.BlockMeta, bool) {
	if l == nil {
		return storage.BlockMeta{}, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.tail == nil {
		return storage.BlockMeta{}, false
	}
	return l.tail.meta, true
}

func (l *localMemoryLRU) bytesUsed() uint64 {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.bytes
}

func (l *localMemoryLRU) count() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

func (l *localMemoryLRU) blockIDs() map[uint64]struct{} {
	ids := make(map[uint64]struct{})
	if l == nil {
		return ids
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for blockID := range l.entries {
		ids[blockID] = struct{}{}
	}
	return ids
}

func (l *localMemoryLRU) pushFront(entry *localMemoryEntry) {
	entry.prev = nil
	entry.next = l.head
	if l.head != nil {
		l.head.prev = entry
	}
	l.head = entry
	if l.tail == nil {
		l.tail = entry
	}
}

func (l *localMemoryLRU) moveToFront(entry *localMemoryEntry) {
	if entry == l.head {
		return
	}
	l.unlink(entry)
	l.pushFront(entry)
}

func (l *localMemoryLRU) unlink(entry *localMemoryEntry) {
	if entry.prev != nil {
		entry.prev.next = entry.next
	} else {
		l.head = entry.next
	}
	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		l.tail = entry.prev
	}
	entry.prev = nil
	entry.next = nil
}

func normalizeMemoryWatermarks(high, low uint64) (uint64, uint64) {
	if high == 0 {
		return 0, 0
	}
	if low == 0 || low >= high {
		low = high * 8 / 10
	}
	if low == high {
		low = high - 1
	}
	return high, low
}
