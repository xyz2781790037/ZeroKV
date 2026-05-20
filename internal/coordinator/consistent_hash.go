//go:build legacy_consistent_hash
// +build legacy_consistent_hash

package coordinator

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"sync"
)

var (
	// ErrNoAliveNodes 触发于客户端请求路由时。
	// 物理意义：整个集群已全部宕机，或所有存活节点都被标记为 Tombstone/Down。
	ErrNoAliveNodes = errors.New("consistent hash: no alive nodes")

	// ErrInvalidReplicas 触发于一致性哈希环初始化时。
	// 物理意义：传入的虚拟节点放大倍数（replicas）非法。
	ErrInvalidReplicas = errors.New("consistent hash: replicas must be positive")

	// ErrInvalidLocateN 触发于执行高可用路由查询 LocateN 时。
	// 物理意义：调用方请求的容灾节点数量非法（如请求获取 0 个或负数个备选节点）。
	ErrInvalidLocateN = errors.New("consistent hash: locate count must be positive")
)

// ringPoint 表示一致性哈希环上的一个虚拟节点（Virtual Node / Slot）。
// 在大规模分布式缓存中，一个物理节点会被虚拟化为多个 ringPoint 散落到环的不同位置，
// 从而通过数学上的随机分布，彻底消除由于物理节点位置不均带来的数据倾斜（Data Skew）问题。
type ringPoint struct {
	hash uint64 // 虚拟节点的全局哈希值（由 NodeID + 副本索引 共同计算得出）
	node Node   // 当前虚拟节点所对应的真实物理节点元数据
}

// ConsistentHash 实现了基于一致性哈希的集群无状态路由管理器。
// 它负责建立“数据块 ID (BlockID) 到 物理节点 (Node)”的稳定映射关系。
// 核心优势在于：当集群发生扩容（新增机器）或缩容（机器宕机）时，只有极少部分缓存键（约 1/N）发生路由变更。
type ConsistentHash struct {
	mu sync.RWMutex // 保护哈希环的高并发读写安全。因为路由查找（Locate）属于极致高频的读，
	// 而拓扑变更（Update）是极低频的写，因此采用读写分离锁（RWMutex）最大化吞吐量。
	replicas int    // 副本因子（虚拟节点倍数）。决定每个物理节点虚拟出多少个坑位，通常配置在 150~300 之间。
	version  uint64 // 拓扑版本号。每次调用 Update 成功重建哈希环后单调递增。
	// 外部客户端（Client）或 API 网关可通过轻量级比对该版本号，判定本地缓存的路由表是否已过时。
	nodes map[NodeID]Node // 当前正参与一致性哈希环分片的活跃物理节点映射表，用于 O(1) 快速检索和状态核对。
	ring  []ringPoint     // 严格按照 hash 值从小到大升序排列的虚拟节点切片。
	// 必须保持严格有序，因为高频路由 Locate 函数底层强依赖该切片执行 O(log N) 的二分查找。
}

func NewConsistentHash(replicas int) (*ConsistentHash, error) {
	if replicas <= 0 {
		return nil, ErrInvalidReplicas
	}
	return &ConsistentHash{
		replicas: replicas,
		nodes:    make(map[NodeID]Node),
	}, nil
}

// NewConsistentHashFromMembership 是一个便捷的构造工厂。
// 它直接读取当前拓扑管理（Membership）中的活跃节点，并以此原子化地初始化一个一致性哈希环。
func NewConsistentHashFromMembership(m *Membership, replicas int) (*ConsistentHash, error) {
	ring, err := NewConsistentHash(replicas)
	if err != nil {
		return nil, err
	}
	if err := ring.UpdateMembership(m); err != nil {
		return nil, err
	}
	return ring, nil
}

// UpdateMembership 提取 Membership 中的当前存活节点并刷新哈希环拓扑。
func (c *ConsistentHash) UpdateMembership(m *Membership) error {
	if m == nil {
		return fmt.Errorf("consistent hash: nil membership")
	}
	return c.Update(m.AliveNodes())
}

// Update 接收最新的物理节点列表，在【锁外】全量重建哈希环，最后通过【锁内】指针替换实现无缝扩缩容。
// 这种模式体现了标准的 Copy-on-Write（写时复制）思想，使得高频的读取操作（Locate）几乎不会被全量重建所阻塞。
func (c *ConsistentHash) Update(nodes []Node) error {
	if c == nil {
		return fmt.Errorf("consistent hash: nil ring")
	}
	unique := make(map[NodeID]Node, len(nodes))
	for _, node := range nodes {
		if node.State != NodeStateAlive {
			continue // 严格隔离：非 Alive 节点（如 Tombstone、Down）绝对不允许上环分配流量
		}
		if err := validateNode(node); err != nil {
			return err
		}
		existing, exists := unique[node.ID]
		// 如果节点重复，仅保留 Version 较大的最新状态
		if !exists || node.Version > existing.Version {
			unique[node.ID] = node
		}
	}
	points := make([]ringPoint, 0, len(unique)*c.replicas)
	for _, node := range unique {
		for replica := 0; replica < c.replicas; replica++ {
			points = append(points, ringPoint{
				hash: hashNodeReplica(node.ID, replica),
				node: node,
			})
		}
	}
	// 3. 对整个哈希环进行严格的升序排列，这是后续能进行 O(log N) 二分查找的绝对核心前提。
	// 如果两个虚拟节点的哈希值极其罕见地撞车了，则通过物理 NodeID 的字典序进行打破僵局的确定性排序。
	sort.Slice(points, func(i, j int) bool {
		if points[i].hash == points[j].hash {
			return points[i].node.ID < points[j].node.ID
		}
		return points[i].hash < points[j].hash
	})
	// 进入内核临界区，以极快的速度原子化替换指针，并自增拓扑版本号
	c.mu.Lock()
	c.nodes = unique
	c.ring = points
	c.version++
	c.mu.Unlock()
	return nil
}

// Replicas 返回哈希环配置的虚拟节点放大倍数。
// 该字段在初始化后为只读常量，因此读取时无需加锁。
func (c *ConsistentHash) Replicas() int {
	if c == nil {
		return 0
	}
	return c.replicas
}

// Len 返回当前参与哈希环分片的真实物理节点总数。
func (c *ConsistentHash) Len() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.nodes)
}

// Version 返回当前哈希环的拓扑版本。
// 客户端（Client）可以通过定时轮询并比对该版本号，来判定本地缓存的路由表是否需要同步刷新。
func (c *ConsistentHash) Version() uint64 {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.version
}

// Nodes 导出当前哈希环中所有物理节点的快照列表。
// 返回的列表会按照 NodeID 的字典序进行严格排序，以保证外部获取时输出顺序的确定性。
func (c *ConsistentHash) Nodes() []Node {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	// 预分配切片内存，避免 append 过程中的频繁动态扩容
	nodes := make([]Node, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, node)
	}
	c.mu.RUnlock()
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	return nodes
}

// Locate 执行标准的一致性哈希单点路由查找。
// 物理语义：顺时针寻找距离 blockID 最近的第一台健康物理机。
func (c *ConsistentHash) Locate(blockID uint64) (Node, error) {
	if c == nil {
		return Node{}, fmt.Errorf("consistent hash: nil ring")
	}
	//  目的就是为了打乱为了无规律的打乱
	keyHash := hashUint64(blockID)
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.ring) == 0 {
		return Node{}, ErrNoAliveNodes
	}
	idx := searchRing(c.ring, keyHash)
	return c.ring[idx].node, nil
}

// LocateN 是面向分布式高可用（High Availability）设计的级联路由查找算法。
// 它不仅返回最优的首选节点，还会沿着哈希环顺时针方向依次导出 N-1 个备选节点。
// 特性：算法在内部自动执行了【物理节点去重】，确保返回的 N 个节点分属不同的真实机器，防止灾难发生时 fallback 到同台机器的其他虚拟节点上。
func (c *ConsistentHash) LocateN(blockID uint64, n int) ([]Node, error) {
	nodes, _, err := c.LocateNWithVersion(blockID, n)
	return nodes, err
}

// LocateNWithVersion 和 LocateN 一样返回候选节点，同时返回同一个哈希环快照下的版本号。
// 调用方如果要把路由结果缓存到客户端，必须使用这个方法避免“旧节点列表 + 新版本号”的撕裂快照。
func (c *ConsistentHash) LocateNWithVersion(blockID uint64, n int) ([]Node, uint64, error) {
	if c == nil {
		return nil, 0, fmt.Errorf("consistent hash: nil ring")
	}
	if n <= 0 {
		return nil, 0, ErrInvalidLocateN
	}
	keyHash := hashUint64(blockID)
	c.mu.RLock()
	defer c.mu.RUnlock()
	version := c.version
	if len(c.ring) == 0 {
		return nil, version, ErrNoAliveNodes
	}
	// 防御性拦截：请求的容灾物理节点数不能超过当前全网总存活机器数
	if n > len(c.nodes) {
		n = len(c.nodes)
	}
	// 在有序的哈希环数组中，精准定位到第一个哈希值大于等于 keyHash 的虚拟节点索引。
	start := searchRing(c.ring, keyHash)
	nodes := make([]Node, 0, n)
	seen := make(map[NodeID]struct{}, n)
	for scanned := 0; scanned < len(c.ring) && len(nodes) < n; scanned++ {
		// 环形回绕寻路
		point := c.ring[(start+scanned)%len(c.ring)]
		if _, ok := seen[point.node.ID]; ok {
			continue
		}
		seen[point.node.ID] = struct{}{}
		nodes = append(nodes, point.node)
	}
	return nodes, version, nil
}

// hashNodeReplica 为指定的物理节点构建其专属第 replica 个分身（虚拟节点）的哈希值。
func hashNodeReplica(id NodeID, replica int) uint64 {
	// 格式化为 "node_id#index"（如 "192.168.1.100:8080#42"）作为唯一的输入源
	return hashString(fmt.Sprintf("%s#%d", id, replica))
}

// hashUint64 将 64 位无符号整数形式的数据块 ID 映射为哈希环上的空间刻度。
func hashUint64(v uint64) uint64 {
	return murmurFinalizer64(v)
}

// murmurFinalizer64 是经典的 MurmurHash3 算法中最后的 64位雪崩混合器（Finalizer）。
// 物理意义：它通过一系列精心挑选的黄金质数进行高强度的位移异或与乘法混合。
// 分布式价值：即使输入值 `v` 是高度连续的自增数字（如 1, 2, 3... 这种极其常见的 BlockID），
// 经过该函数的计算，输出的 uint64 结果也会彻底失去原有的线性连续特征，变成在 [0, 2^64-1] 范围内均匀喷射的离散值。
// 这是一致性哈希能够实现全局完美负载均衡、彻底告别数据倾斜的数学基石。
func murmurFinalizer64(v uint64) uint64 {
	// 高位向下揉碎
	v ^= v >> 33
	// 动作：乘上一个十六进制常量。打乱整个数字
	v *= 0xff51afd7ed558ccd
	v ^= v >> 33
	v *= 0xc4ceb9fe1a85ec53
	v ^= v >> 33
	return v
}

// searchRing 封装了一致性哈希环的顺时针二分查找底层逻辑。
func searchRing(ring []ringPoint, keyHash uint64) int {
	// sort.Search 会返回第一个满足判定条件的索引 i
	idx := sort.Search(len(ring), func(i int) bool {
		return ring[i].hash >= keyHash
	})
	// 边界条件处理（环形闭环）：
	// 如果 idx == len(ring)，说明当前数据的 keyHash 已经超越了环上的最大刻度。
	// 根据一致性哈希环首尾相接的规则，它应该跨越零点，交由全环顺时针方向的第一个节点（索引 0）来接管。
	if idx == len(ring) {
		return 0
	}
	return idx
}

// hashString 对字符串输入进行 FNV + Murmur 级联双重散列混淆，最大化其雪崩效应。
func hashString(s string) uint64 {
	// 相当于揉成数字
	h := fnv.New64a()
	_, _ = io.WriteString(h, s)
	// 级联调用 Murmur Finalizer 主要是为了进一步打散 FNV 出来的密集特征码，使虚拟节点更加完美地平铺分布
	return murmurFinalizer64(h.Sum64())
}
