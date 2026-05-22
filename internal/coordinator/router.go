package coordinator

import (
	"errors"
	"fmt"
)

// ErrNilMembership 触发于路由器初始化或刷新时未提供成员表。
var ErrNilMembership = errors.New("router: nil membership")

// minRouteCandidates 定义了路由查询时允许的最小副本数，防止业务层传入 0 或负数导致寻址失败。
const minRouteCandidates = 1

// BlockRoute 是一次数据块 (Block) 路由决策的不可变快照。
// 在分布式存储中，网关或协调节点会基于此快照决定将读写请求发往何处。
type BlockRoute struct {
	BlockID uint64 // 目标数据块的唯一标识
	Primary Node   // 首选节点（radix 前缀路由命中的节点）
	// Candidates 包含了包括 Primary 在内的容灾节点列表，按 radix 区间顺序排列。
	// 当 Primary 宕机时，网关将按此列表顺序进行 Fallback（降级重试）。
	Candidates []Node
	// IsPrimary 标识当前物理机是否是该 Block 的绝对首选处理者。
	// 在强一致性模型（如 Raft）中，通常只有 Primary 允许处理写请求。
	IsPrimary bool
	// Involved 标识当前物理机是否在容灾候选列表 (Candidates) 中。
	// 如果为 true，说明当前节点持有该 Block 的冗余副本，可用于本地直接响应读请求或参与副本同步。
	Involved bool
}

// Router 是控制面向业务层暴露的高级路由器。
// 它组合了 Membership（维护集群物理存活状态）和 RadixTree（提供前缀路由映射）。
// 它是无状态的计算层，真正的数据分片状态由 Membership 驱动。
type Router struct {
	selfID     NodeID      // 当前物理节点的全局唯一 ID
	membership *Membership // 集群成员状态表（事实来源）
	tree       *RadixTree  // 压缩前缀树（路由算法引擎）
}

// NewRouter 初始化并构造一个与当前集群成员状态对齐的路由器。
func NewRouter(selfID NodeID, membership *Membership) (*Router, error) {
	if selfID == "" {
		return nil, ErrEmptyNodeID
	}
	if membership == nil {
		return nil, ErrNilMembership
	}
	tree, err := NewRadixTreeFromMembership(membership)
	if err != nil {
		return nil, err
	}
	return &Router{
		selfID:     selfID,
		membership: membership,
		tree:       tree,
	}, nil
}

// SelfID 返回当前物理节点的 ID。
func (r *Router) SelfID() NodeID {
	if r == nil {
		return ""
	}
	return r.selfID
}

// Version 返回底座 radix tree 的拓扑版本号。
// 业务侧可定时比对此版本号，以决定是否需要刷新本地缓存的路由表，避免路由黑洞。
func (r *Router) Version() uint64 {
	if r == nil || r.tree == nil {
		return 0
	}
	return r.tree.Version()
}

// Nodes 返回当前参与路由计算的所有活跃物理节点的排序快照。
func (r *Router) Nodes() []Node {
	if r == nil || r.tree == nil {
		return nil
	}
	return r.tree.Nodes()
}

// Refresh 从底层的 Membership 同步最新存活节点，并全量重建 radix tree。
// 触发时机：集群有新节点加入、旧节点宕机(Down/Tombstone) 被 Gossip 协议或心跳监测确诊时。
func (r *Router) Refresh() error {
	if r == nil {
		return fmt.Errorf("router: nil router")
	}
	if r.membership == nil {
		return ErrNilMembership
	}
	if r.tree == nil {
		return fmt.Errorf("router: nil radix tree")
	}
	return r.tree.UpdateMembership(r.membership)
}

// RouteBlock 执行基础的单点路由寻址，返回该 blockID 的最佳归属节点。
func (r *Router) RouteBlock(blockID uint64) (Node, error) {
	if r == nil || r.tree == nil {
		return Node{}, fmt.Errorf("router: nil router")
	}
	return r.tree.Locate(blockID)
}

// RouteBlockReplicas 获取数据块的多个副本存储位置。
// n 为期望的物理节点数量，函数内部在入口处进行了降级防御，保底返回至少 1 个节点。
func (r *Router) RouteBlockReplicas(blockID uint64, n int) ([]Node, error) {
	if r == nil || r.tree == nil {
		return nil, fmt.Errorf("router: nil router")
	}
	return r.tree.LocateN(blockID, normalizeCandidateCount(n))
}

// RouteBlockReplicasWithVersion 返回候选节点以及生成该候选列表时的 radix tree 版本。
// 这两个值来自底层 radix tree 的同一个读锁临界区，适合直接返回给会缓存路由的客户端。
func (r *Router) RouteBlockReplicasWithVersion(blockID uint64, n int) ([]Node, uint64, error) {
	if r == nil || r.tree == nil {
		return nil, 0, fmt.Errorf("router: nil router")
	}
	return r.tree.LocateNWithVersion(blockID, normalizeCandidateCount(n))
}

// Route 是对业务侧暴露的最高层路由决策接口。
// 它封装了底层寻址逻辑，并自动判定当前物理节点（自己）与目标数据块的关系，生成路由快照。
func (r *Router) Route(blockID uint64, candidates int) (BlockRoute, error) {
	nodes, err := r.RouteBlockReplicas(blockID, candidates)
	if err != nil {
		return BlockRoute{}, err
	}
	if len(nodes) == 0 {
		// 防御性保护：哪怕有一丝可能路由树查不到数据，也要直接拦截，防止越界 panic
		return BlockRoute{}, ErrNoAliveNodes
	}
	return BlockRoute{
		BlockID:    blockID,
		Primary:    nodes[0],
		Candidates: nodes,
		IsPrimary:  r.IsLocal(nodes[0].ID),
		Involved:   r.inCandidates(nodes),
	}, nil
}

// IsLocal 判断传入的节点 ID 是否为当前运行的物理机进程自身。
func (r *Router) IsLocal(id NodeID) bool {
	return r != nil && id != "" && id == r.selfID
}

// inCandidates 是遍历比对候选列表的内部辅助方法。
// 统一收口节点身份判定逻辑。
func (r *Router) inCandidates(nodes []Node) bool {
	if r == nil || r.selfID == "" {
		return false
	}
	for _, node := range nodes {
		if r.IsLocal(node.ID) {
			return true
		}
	}
	return false
}

// normalizeCandidateCount 确保向下层 radix tree 传递的路由节点数合法。
// 避免上层业务配置读取错误（比如反序列化失败读出 0）导致触发底层的 ErrInvalidLocateN。
func normalizeCandidateCount(n int) int {
	if n < minRouteCandidates {
		return minRouteCandidates
	}
	return n
}

// OwnsBlock 判断当前物理节点是否应该持有该 blockID 的数据。
// 传入 replicas（副本因子），只要当前节点处于这 replicas 个候选节点中，即认为 Owns。
// 典型场景：垃圾回收 (GC) 时判断本地磁盘上的数据块是否为合法冗余，如果不属于自己则物理清理。
func (r *Router) OwnsBlock(blockID uint64, replicas int) (bool, error) {
	nodes, err := r.RouteBlockReplicas(blockID, replicas)
	if err != nil {
		return false, err
	}
	return r.inCandidates(nodes), nil
}
