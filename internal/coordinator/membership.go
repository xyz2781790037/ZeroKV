package coordinator

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// 预定义成员管理相关的哨兵错误
var (
	ErrEmptyNodeID   = errors.New("membership: empty node id")
	ErrEmptyNodeAddr = errors.New("membership: empty node address")
	ErrNodeNotFound  = errors.New("membership: node not found")
	// (节点已处于墓碑状态)
	ErrNodeTombstoned = errors.New("membership: node tombstoned") // 用于拦截对已物理下线节点的无效操作
)

type NodeID string
type NodeState uint8

const (
	NodeStateUnknown   NodeState = iota
	NodeStateAlive               // 节点正常，且最近有心跳
	NodeStateSuspect             // 节点心跳超时，疑似宕机（用于 gossip 协议中的缓冲期）
	NodeStateDown                // 节点确诊宕机，但尚未从集群拓扑中彻底剔除
	NodeStateTombstone           // 墓碑状态：节点已逻辑删除，保留此状态用于压制网络中迟到的旧状态包
)

func (s NodeState) String() string {
	switch s {
	case NodeStateAlive:
		return "alive"
	case NodeStateSuspect:
		return "suspect"
	case NodeStateDown:
		return "down"
	case NodeStateTombstone:
		return "tombstone"
	default:
		return "unknown"
	}
}

// Node 表示集群中的一个参与节点
type Node struct {
	ID            NodeID    // 节点全局唯一标识
	Addr          string    // 节点的网络通信地址 (IP:Port)
	State         NodeState // 节点当前状态
	Version       uint64    // 单调递增的版本号，用于分布式状态同步时的 LWW (Last-Write-Wins) 冲突决议
	LastHeartbeat time.Time // 最后一次收到心跳的绝对时间，用于故障检测(Failure Detection)
}

// Membership 维护集群全局的节点拓扑状态
type Membership struct {
	mu    sync.RWMutex    // 保护 nodes 映射表的并发读写安全
	nodes map[NodeID]Node // 节点路由哈希表
}

// NewMembership 初始化成员列表，并将自身(Coordinator本节点)加入其中
func NewMembership(self Node) (*Membership, error) {
	if err := validateNode(self); err != nil {
		return nil, err
	}
	self.State = NodeStateAlive
	if self.LastHeartbeat.IsZero() {
		self.LastHeartbeat = time.Now()
	}
	return &Membership{
		nodes: map[NodeID]Node{
			self.ID: self,
		},
	}, nil
}

// Upsert 插入或更新节点状态（通常用于接收来自其他节点的 Gossip 同步包）
func (m *Membership) Upsert(node Node) error {
	if m == nil {
		return fmt.Errorf("membership: nil membership")
	}
	if err := validateNode(node); err != nil {
		return err
	}
	if node.State == NodeStateUnknown {
		node.State = NodeStateAlive
	}
	if node.LastHeartbeat.IsZero() {
		node.LastHeartbeat = time.Now()
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	existing, exists := m.nodes[node.ID]
	// LWW (Last-Write-Wins) 机制：
	// 如果本地已存在的版本号大于等于收到的版本号，说明收到的是网络延迟导致的旧包，直接丢弃
	if exists && node.Version <= existing.Version {
		return nil
	}
	m.nodes[node.ID] = node
	return nil
}

// Heartbeat 续约指定节点的心跳
func (m *Membership) Heartbeat(id NodeID, at time.Time) error {
	if m == nil {
		return fmt.Errorf("membership: nil membership")
	}
	if id == "" {
		return ErrEmptyNodeID
	}
	if at.IsZero() {
		at = time.Now()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	node, ok := m.nodes[id]
	if !ok {
		return ErrNodeNotFound
	}
	// 拦截对墓碑节点的心跳：已下线的节点不允许通过心跳诈尸复活
	if node.State == NodeStateTombstone {
		return ErrNodeTombstoned
	}
	node.LastHeartbeat = at
	node.State = NodeStateAlive
	node.Version++
	m.nodes[id] = node
	return nil
}

// Mark 强制扭转指定节点的状态（如故障检测协程发现节点超时，将其标记为 Down）
func (m *Membership) Mark(id NodeID, state NodeState) error {
	if m == nil {
		return fmt.Errorf("membership: nil membership")
	}
	if id == "" {
		return ErrEmptyNodeID
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	node, ok := m.nodes[id]
	if !ok {
		return ErrNodeNotFound
	}
	// 状态机保护：一旦节点变成墓碑，绝不允许被扭转回其他活跃状态
	if node.State == NodeStateTombstone && state != NodeStateTombstone {
		return ErrNodeTombstoned
	}
	if state == NodeStateTombstone && node.LastHeartbeat.IsZero() {
		node.LastHeartbeat = time.Now()
	}
	node.State = state
	node.Version++
	m.nodes[id] = node
	return nil
}

// Remove 逻辑删除节点（生成墓碑）
func (m *Membership) Remove(id NodeID) error {
	if m == nil {
		return fmt.Errorf("membership: nil membership")
	}
	if id == "" {
		return ErrEmptyNodeID
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	node, ok := m.nodes[id]
	if !ok {
		// 防御性设计：如果节点原本不存在，直接凭空造一个墓碑。
		// 防止集群其他节点频繁发送该节点的心跳过来导致无休止的同步。
		node = Node{ID: id, State: NodeStateTombstone, Version: 1, LastHeartbeat: time.Now()}
		m.nodes[id] = node
		return nil
	}
	// 递增版本号确保墓碑能够覆盖集群中其他节点持有的旧 Alive 状态
	node.Version++
	node.State = NodeStateTombstone
	node.LastHeartbeat = time.Now()
	m.nodes[id] = node
	return nil
}

// Get O(1) 获取节点当前状态快照
func (m *Membership) Get(id NodeID) (Node, bool) {
	if m == nil {
		return Node{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	node, ok := m.nodes[id]
	return node, ok
}

// List 获取当前拓扑所有节点（包含疑似宕机和墓碑节点）
func (m *Membership) List() []Node {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	nodes := make([]Node, 0, len(m.nodes))
	for _, node := range m.nodes {
		nodes = append(nodes, node)
	}
	m.mu.RUnlock()
	sortNodes(nodes) // 锁外排序
	return nodes
}

// AliveNodes 仅获取存活节点（常用于路由和负载均衡选址）
func (m *Membership) AliveNodes() []Node {
	if m == nil {
		return nil
	}
	m.mu.RLock()

	nodes := make([]Node, 0, len(m.nodes))
	for _, node := range m.nodes {
		if node.State == NodeStateAlive {
			nodes = append(nodes, node)
		}
	}
	m.mu.RUnlock() // 提前释放读锁

	sortNodes(nodes)
	return nodes
}

// Snapshot 导出全量节点哈希表（常用于序列化后向新加入的节点发送全量拓扑）
func (m *Membership) Snapshot() map[NodeID]Node {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	snapshot := make(map[NodeID]Node, len(m.nodes))
	for id, node := range m.nodes {
		snapshot[id] = node // Go 的 struct 是值拷贝，这里直接赋值是安全的
	}
	return snapshot
}

// Len 返回跟踪的节点总数（包含墓碑）
func (m *Membership) Len() int {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.nodes)
}

// validateNode 验证节点核心元数据是否合法
func validateNode(node Node) error {
	if node.ID == "" {
		return ErrEmptyNodeID
	}
	// 墓碑节点允许没有真实地址（因为它不提供服务），但其他节点必须包含有效的网络可达地址
	if node.State != NodeStateTombstone && node.Addr == "" {
		return ErrEmptyNodeAddr
	}
	return nil
}

// sortNodes 按 NodeID 的字典序排序，保证多次调用返回的节点列表顺序一致（对 radix 路由构建非常重要）
func sortNodes(nodes []Node) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
}
