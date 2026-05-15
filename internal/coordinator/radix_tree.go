package coordinator

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	// ErrNoAliveNodes 触发于客户端请求路由时。
	// 物理意义：整个集群已全部宕机，或所有存活节点都被标记为 Tombstone/Down。
	ErrNoAliveNodes = errors.New("radix router: no alive nodes")

	// ErrInvalidLocateN 触发于执行高可用路由查询 LocateN 时。
	// 物理意义：调用方请求的容灾节点数量非法（如请求获取 0 个或负数个备选节点）。
	ErrInvalidLocateN = errors.New("radix router: locate count must be positive")
)

const blockIDBits = 64

// blockRange 表示 blockID 空间中的一个闭区间 [start, end]。
// radix tree 构建阶段会把每个节点负责的连续区间拆成最少数量的二进制前缀。
type blockRange struct {
	start uint64
	end   uint64
	node  Node
}

// radixRouteEntry 是一个已经压缩后的前缀路由项。
// prefix 只在前 prefixLen 位有效，剩余位必须为 0。
type radixRouteEntry struct {
	prefix    uint64
	prefixLen uint8
	start     uint64
	end       uint64
	node      Node
}

type radixNode struct {
	prefix    uint64
	prefixLen uint8
	entry     *radixRouteEntry
	children  [2]*radixNode
}

// RadixTree 用压缩二进制前缀树管理 blockID -> node 的路由。
// 和一致性哈希不同，它保留 blockID 的前缀局部性：相邻 ID 段会稳定落到同一节点。
type RadixTree struct {
	mu      sync.RWMutex
	version uint64
	nodes   map[NodeID]Node
	ranges  []blockRange
	root    *radixNode
}

func NewRadixTree() *RadixTree {
	return &RadixTree{
		nodes: make(map[NodeID]Node),
		root:  &radixNode{},
	}
}

func NewRadixTreeFromMembership(m *Membership) (*RadixTree, error) {
	tree := NewRadixTree()
	if err := tree.UpdateMembership(m); err != nil {
		return nil, err
	}
	return tree, nil
}

func (t *RadixTree) UpdateMembership(m *Membership) error {
	if m == nil {
		return fmt.Errorf("radix router: nil membership")
	}
	return t.Update(m.AliveNodes())
}

func (t *RadixTree) Update(nodes []Node) error {
	if t == nil {
		return fmt.Errorf("radix router: nil tree")
	}
	unique := make(map[NodeID]Node, len(nodes))
	for _, node := range nodes {
		if node.State != NodeStateAlive {
			continue
		}
		if err := validateNode(node); err != nil {
			return err
		}
		existing, exists := unique[node.ID]
		if !exists || node.Version > existing.Version {
			unique[node.ID] = node
		}
	}
	ordered := sortedNodeValues(unique)
	ranges := assignBlockRanges(ordered)
	root := buildRadixTree(ranges)

	t.mu.Lock()
	t.nodes = unique
	t.ranges = ranges
	t.root = root
	t.version++
	t.mu.Unlock()
	return nil
}

func (t *RadixTree) Len() int {
	if t == nil {
		return 0
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.nodes)
}

func (t *RadixTree) Version() uint64 {
	if t == nil {
		return 0
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.version
}

func (t *RadixTree) Nodes() []Node {
	if t == nil {
		return nil
	}
	t.mu.RLock()
	nodes := sortedNodeValues(t.nodes)
	t.mu.RUnlock()
	return nodes
}

func (t *RadixTree) Locate(blockID uint64) (Node, error) {
	nodes, _, err := t.LocateNWithVersion(blockID, 1)
	if err != nil {
		return Node{}, err
	}
	return nodes[0], nil
}

func (t *RadixTree) LocateN(blockID uint64, n int) ([]Node, error) {
	nodes, _, err := t.LocateNWithVersion(blockID, n)
	return nodes, err
}

func (t *RadixTree) LocateNWithVersion(blockID uint64, n int) ([]Node, uint64, error) {
	if t == nil {
		return nil, 0, fmt.Errorf("radix router: nil tree")
	}
	if n <= 0 {
		return nil, 0, ErrInvalidLocateN
	}
	t.mu.RLock()
	defer t.mu.RUnlock()

	version := t.version
	if len(t.nodes) == 0 || t.root == nil {
		return nil, version, ErrNoAliveNodes
	}
	if n > len(t.nodes) {
		n = len(t.nodes)
	}
	primary, ok := t.lookupLocked(blockID)
	if !ok {
		return nil, version, ErrNoAliveNodes
	}
	out := make([]Node, 0, n)
	seen := make(map[NodeID]struct{}, n)
	appendUniqueNode := func(node Node) {
		if len(out) >= n || node.ID == "" {
			return
		}
		if _, exists := seen[node.ID]; exists {
			return
		}
		seen[node.ID] = struct{}{}
		out = append(out, node)
	}
	appendUniqueNode(primary)

	start := searchRange(t.ranges, blockID)
	for offset := 1; len(out) < n && offset < len(t.ranges); offset++ {
		appendUniqueNode(t.ranges[(start+offset)%len(t.ranges)].node)
	}
	return out, version, nil
}

func (t *RadixTree) lookupLocked(blockID uint64) (Node, bool) {
	node := t.root
	for node != nil {
		if node.prefixLen > 0 && maskPrefix(blockID, node.prefixLen) != node.prefix {
			return Node{}, false
		}
		if node.entry != nil {
			return node.entry.node, true
		}
		if node.prefixLen >= blockIDBits {
			break
		}
		bit := bitAt(blockID, node.prefixLen)
		node = node.children[bit]
	}
	return Node{}, false
}

func buildRadixTree(ranges []blockRange) *radixNode {
	root := &radixNode{}
	for _, routeRange := range ranges {
		entries := coverRangeWithPrefixes(routeRange.start, routeRange.end, routeRange.node)
		for i := range entries {
			insertRadixEntry(root, &entries[i])
		}
	}
	compressRadix(root)
	return root
}

func insertRadixEntry(root *radixNode, entry *radixRouteEntry) {
	current := root
	for depth := uint8(0); depth < entry.prefixLen; depth++ {
		bit := bitAt(entry.prefix, depth)
		if current.children[bit] == nil {
			current.children[bit] = &radixNode{
				prefix:    maskPrefix(entry.prefix, depth+1),
				prefixLen: depth + 1,
			}
		}
		current = current.children[bit]
	}
	current.entry = entry
}

func compressRadix(node *radixNode) {
	if node == nil {
		return
	}
	for bit := range node.children {
		child := node.children[bit]
		if child == nil {
			continue
		}
		compressRadix(child)
		for child.entry == nil && child.singleChild() != nil {
			child = child.singleChild()
		}
		node.children[bit] = child
	}
}

func (n *radixNode) singleChild() *radixNode {
	if n == nil {
		return nil
	}
	if n.children[0] != nil && n.children[1] == nil {
		return n.children[0]
	}
	if n.children[1] != nil && n.children[0] == nil {
		return n.children[1]
	}
	return nil
}

func assignBlockRanges(nodes []Node) []blockRange {
	if len(nodes) == 0 {
		return nil
	}
	ranges := make([]blockRange, 0, len(nodes))
	step := ^uint64(0)/uint64(len(nodes)) + 1
	for i, node := range nodes {
		start := uint64(i) * step
		end := ^uint64(0)
		if i < len(nodes)-1 {
			end = uint64(i+1)*step - 1
		}
		ranges = append(ranges, blockRange{
			start: start,
			end:   end,
			node:  node,
		})
	}
	return ranges
}

func coverRangeWithPrefixes(start, end uint64, node Node) []radixRouteEntry {
	entries := make([]radixRouteEntry, 0, 2*blockIDBits)
	for start <= end {
		size := start & -start
		if size == 0 {
			size = uint64(1) << 63
		}
		for !rangeCanFit(start, end, size) {
			size >>= 1
		}
		prefixLen := uint8(blockIDBits - trailingZeros64(size))
		blockEnd := start + size - 1
		entries = append(entries, radixRouteEntry{
			prefix:    maskPrefix(start, prefixLen),
			prefixLen: prefixLen,
			start:     start,
			end:       blockEnd,
			node:      node,
		})
		if blockEnd == end {
			break
		}
		start = blockEnd + 1
	}
	return entries
}

func rangeCanFit(start, end, size uint64) bool {
	if size == 0 {
		return false
	}
	blockEnd := start + size - 1
	if blockEnd < start {
		return false
	}
	return blockEnd <= end
}

func sortedNodeValues(nodes map[NodeID]Node) []Node {
	out := make([]Node, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, node)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out
}

func searchRange(ranges []blockRange, blockID uint64) int {
	idx := sort.Search(len(ranges), func(i int) bool {
		return ranges[i].end >= blockID
	})
	if idx == len(ranges) {
		return 0
	}
	return idx
}

func bitAt(value uint64, depth uint8) uint8 {
	return uint8((value >> (blockIDBits - 1 - depth)) & 1)
}

func maskPrefix(value uint64, prefixLen uint8) uint64 {
	if prefixLen == 0 {
		return 0
	}
	return value & (^uint64(0) << (blockIDBits - prefixLen))
}

func trailingZeros64(value uint64) int {
	if value == 0 {
		return blockIDBits
	}
	n := 0
	for value&1 == 0 {
		n++
		value >>= 1
	}
	return n
}
