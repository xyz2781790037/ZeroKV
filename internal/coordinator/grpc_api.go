package coordinator

import (
	"context"
	"errors"
	"kvcache/proto/controlplane"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *ControlPlaneService) RegisterNode(ctx context.Context, req *controlplane.RegisterNodeRequest) (*controlplane.RegisterNodeResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// 把网络层的 Protobuf 裸字节流转换成纯内存结构体。
	node, err := nodeFromProto(req.GetNode())
	if err != nil {
		return nil, toStatusError(err)
	}
	node.State = NodeStateAlive
	if node.LastHeartbeat.IsZero() {
		node.LastHeartbeat = time.Now()
	}
	// 先查旧状态，再写新状态，最后对比新旧状态决定是否触发路由刷新。
	prev, hadPrev := s.membership.Get(node.ID)
	if hadPrev && node.Version <= prev.Version {
		node.Version = prev.Version + 1
	}
	if err := s.membership.Upsert(node); err != nil {
		return nil, toStatusError(err)
	}
	if routeAffectingNodeChange(prev, hadPrev, node) {
		s.scheduleRouterRefresh()
	}
	version := s.bumpMembershipVersion()
	// 这里再读取一下，获取入库后的最终事实快照。
	latest, _ := s.membership.Get(node.ID)
	return &controlplane.RegisterNodeResponse{
		Self:              nodeToProto(latest),
		MembershipVersion: version,
		// [修复]：只返回当前最终一致性的路由版本
		RouteVersion: s.currentRouteVersion(),
	}, nil
}
func nodeFromProto(node *controlplane.Node) (Node, error) {
	if node == nil {
		return Node{}, ErrNilNode
	}
	lastHeartbeat := time.Time{}
	// get last_heartbeat_unix_nano
	if node.GetLastHeartbeatUnixNano() != 0 {
		lastHeartbeat = time.Unix(0, node.GetLastHeartbeatUnixNano())
	}
	return Node{
		ID:            NodeID(node.GetId()),
		Addr:          node.GetAddr(),
		State:         nodeStateFromProto(node.GetState()),
		Version:       node.GetVersion(),
		LastHeartbeat: lastHeartbeat,
	}, nil
}
func (s *ControlPlaneService) Heartbeat(ctx context.Context, req *controlplane.HeartbeatRequest) (*controlplane.HeartbeatResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id := NodeID(req.GetNodeId())
	// 从内存字典树中查询该节点是否存在。
	prev, ok := s.membership.Get(id)
	if !ok {
		return nil, toStatusError(ErrNodeNotFound)
	}
	// 解析网络传来的纳秒时间戳，如果为空则用本地时间兜底。
	at := time.Unix(0, req.GetHeartbeatUnixNano())
	if req.GetHeartbeatUnixNano() == 0 {
		at = time.Now()
	}

	if err := s.membership.Heartbeat(id, at); err != nil {
		return nil, toStatusError(err)
	}
	// 如果节点之前被标记为宕机/可疑，现在恢复了心跳，说明复活了，需要重新重构路由
	if prev.State != NodeStateAlive {
		s.scheduleRouterRefresh()
	}
	version := s.bumpMembershipVersion()
	node, _ := s.membership.Get(id)
	return &controlplane.HeartbeatResponse{
		Node:              nodeToProto(node),
		MembershipVersion: version,
		RouteVersion:      s.currentRouteVersion(),
	}, nil
}
func (s *ControlPlaneService) LeaveNode(ctx context.Context, req *controlplane.LeaveNodeRequest) (*controlplane.LeaveNodeResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id := NodeID(req.GetNodeId())
	prev, hadPrev := s.membership.Get(id)
	if err := s.membership.Remove(id); err != nil {
		return nil, toStatusError(err)
	}
	if !hadPrev || prev.State == NodeStateAlive {
		s.scheduleRouterRefresh()
	}
	version := s.bumpMembershipVersion()
	node, _ := s.membership.Get(id)
	return &controlplane.LeaveNodeResponse{
		Tombstone:         nodeToProto(node),
		MembershipVersion: version,
		RouteVersion:      s.currentRouteVersion(),
	}, nil
}

// GetMembership 拉取当前集群全量拓扑
func (s *ControlPlaneService) GetMembership(ctx context.Context, req *controlplane.GetMembershipRequest) (*controlplane.GetMembershipResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	nodes := s.membership.List()
	out := make([]*controlplane.Node, 0, len(nodes))
	for _, node := range nodes {
		if node.State == NodeStateTombstone && !req.GetIncludeTombstones() {
			continue
		}
		out = append(out, nodeToProto(node))
	}
	return &controlplane.GetMembershipResponse{
		Nodes:             out,
		MembershipVersion: s.currentMembershipVersion(),
		RouteVersion:      s.currentRouteVersion(),
	}, nil
}

// SyncMembership 强制同步/覆盖拓扑（通常用于控制面脑裂恢复，或多机房同步）
func (s *ControlPlaneService) SyncMembership(ctx context.Context, req *controlplane.SyncMembershipRequest) (*controlplane.SyncMembershipResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	routeDirty := false
	for _, pbNode := range req.GetNodes() {
		node, err := nodeFromProto(pbNode)
		if err != nil {
			return nil, toStatusError(err)
		}
		prev, hadPrev := s.membership.Get(node.ID)
		if err := s.membership.Upsert(node); err != nil {
			return nil, toStatusError(err)
		}
		latest, _ := s.membership.Get(node.ID)
		if routeAffectingNodeChange(prev, hadPrev, latest) {
			routeDirty = true
		}
	}
	if routeDirty {
		s.scheduleRouterRefresh()
	}
	version := s.bumpMembershipVersion()
	nodes := nodesToProto(s.membership.List())
	return &controlplane.SyncMembershipResponse{
		Nodes:             nodes,
		MembershipVersion: version,
		RouteVersion:      s.currentRouteVersion(),
	}, nil
}

// RouteBlock 核心读接口：查询数据块应存放的目标节点列表
func (s *ControlPlaneService) RouteBlock(ctx context.Context, req *controlplane.RouteBlockRequest) (*controlplane.RouteBlockResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// 依赖 radix 前缀路由树计算目标节点，返回主节点及多个候选（副本）节点
	nodes, routeVersion, err := s.router.RouteBlockReplicasWithVersion(req.GetBlockId(), int(req.GetCandidates()))
	if err != nil {
		return nil, toStatusError(err)
	}

	if len(nodes) == 0 {
		return nil, status.Error(codes.Unavailable, ErrNoAliveNodes.Error())
	}
	return &controlplane.RouteBlockResponse{
		BlockId:      req.GetBlockId(),
		Primary:      nodeToProto(nodes[0]),
		Candidates:   nodesToProto(nodes),
		RouteVersion: routeVersion,
	}, nil
}

// AnnounceBlock 核心写接口：数据面节点汇报自己刚刚落盘了某个 KV Block
func (s *ControlPlaneService) AnnounceBlock(ctx context.Context, req *controlplane.AnnounceBlockRequest) (*controlplane.AnnounceBlockResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	nodeID := NodeID(req.GetNodeId())
	// 拒绝接受不在拓扑名单内的幽灵节点数据
	node, ok := s.membership.Get(nodeID)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "control plane: node %q not found", nodeID)
	}
	meta := req.GetMeta()
	if meta == nil {
		return nil, toStatusError(ErrNilMeta)
	}
	if meta.GetBlockId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "control plane: zero block id")
	}

	record := blockLocationRecord{
		nodeID: node.ID, // [修复]：提取 NodeID
		tier:   req.GetTier(),
		meta:   blockMetaRecordFromProto(meta),
	}
	// 追加 WAL，分配最新逻辑时钟 Version
	event, err := s.appendLocationEvent(ctx, LocationEvent{
		Kind:   LocationEventUpsert,
		NodeID: record.nodeID, // [修复]：传入正确的 NodeID 类型
		Tier:   record.tier,
		Meta:   record.meta,
	})
	if err != nil {
		return nil, toStatusError(err)
	}
	record.version = event.Version

	return &controlplane.AnnounceBlockResponse{
		Location:        locationToProto(record, node.Addr), // [修复]：按新签名传入 Addr
		LocationVersion: event.Version,
	}, nil
}

// ForgetBlock 节点汇报已删除某个数据块（如因 LRU 淘汰）
func (s *ControlPlaneService) ForgetBlock(ctx context.Context, req *controlplane.ForgetBlockRequest) (*controlplane.ForgetBlockResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	nodeID := NodeID(req.GetNodeId())
	if nodeID == "" {
		return nil, toStatusError(ErrEmptyNodeID)
	}
	blockID := req.GetBlockId()
	if blockID == 0 {
		return nil, status.Error(codes.InvalidArgument, "control plane: zero block id")
	}
	if req.GetTier() == controlplane.StorageTier_STORAGE_TIER_UNKNOWN {
		return nil, status.Error(codes.InvalidArgument, "control plane: unknown storage tier")
	}

	// 追加 Delete 类型日志
	event, err := s.appendLocationEvent(ctx, LocationEvent{
		Kind:    LocationEventDelete,
		BlockID: blockID,
		NodeID:  nodeID,
		Tier:    req.GetTier(),
	})
	if err != nil {
		return nil, toStatusError(err)
	}
	return &controlplane.ForgetBlockResponse{
		BlockId:         blockID,
		NodeId:          string(nodeID),
		LocationVersion: event.Version,
	}, nil
}

// GetBlockLocations 客户端/其他节点查询某个数据块当前所在的真实物理机器
func (s *ControlPlaneService) GetBlockLocations(ctx context.Context, req *controlplane.GetBlockLocationsRequest) (*controlplane.GetBlockLocationsResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	blockID := req.GetBlockId()
	// 获取经过排序后的安全只读快照
	records := s.sortedLocations(blockID)
	locations := make([]*controlplane.BlockLocation, 0, len(records))

	// [修复]：动态获取最新的 Node Addr 组装 Proto。
	// 物理意义：节点 IP 可能因为重启发生变化，因此元数据中绝对不能硬编码 Addr，
	// 必须在查询时实时 Join (联表查询) Membership 拓扑获取最新 IP。
	for _, record := range records {
		node, ok := s.membership.Get(record.nodeID)
		addr := ""
		if ok {
			addr = node.Addr
		}
		locations = append(locations, locationToProto(record, addr))
	}
	return &controlplane.GetBlockLocationsResponse{
		BlockId:         blockID,
		Locations:       locations,
		LocationVersion: s.locationVersion.Load(),
	}, nil
}

// routeAffectingNodeChange 判断节点变更是否实质性影响了网络拓扑，决定是否触发防抖重建
func routeAffectingNodeChange(prev Node, hadPrev bool, next Node) bool {
	// 如果它是以 Alive（存活/就绪）状态加入的，说明集群多了一个可用算力/存储节点，需要刷新
	if !hadPrev {
		return next.State == NodeStateAlive
	}
	// 只要这个状态跳变牵扯到了 Alive 状态，就必须重建拓扑。
	if prev.State != next.State {
		return prev.State == NodeStateAlive || next.State == NodeStateAlive
	}
	return next.State == NodeStateAlive && prev.Addr != next.Addr
}
func nodeToProto(node Node) *controlplane.Node {
	return &controlplane.Node{
		Id:                    string(node.ID),
		Addr:                  node.Addr,
		State:                 nodeStateToProto(node.State),
		Version:               node.Version,
		LastHeartbeatUnixNano: node.LastHeartbeat.UnixNano(),
	}
}

func nodesToProto(nodes []Node) []*controlplane.Node {
	out := make([]*controlplane.Node, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, nodeToProto(node))
	}
	return out
}

func nodeStateToProto(state NodeState) controlplane.NodeState {
	switch state {
	case NodeStateAlive:
		return controlplane.NodeState_NODE_STATE_ALIVE
	case NodeStateSuspect:
		return controlplane.NodeState_NODE_STATE_SUSPECT
	case NodeStateDown:
		return controlplane.NodeState_NODE_STATE_DOWN
	case NodeStateTombstone:
		return controlplane.NodeState_NODE_STATE_TOMBSTONE
	default:
		return controlplane.NodeState_NODE_STATE_UNKNOWN
	}
}

func blockMetaRecordFromProto(meta *controlplane.BlockMeta) BlockMetaRecord {
	if meta == nil {
		return BlockMetaRecord{}
	}
	return BlockMetaRecord{
		BlockID:    meta.GetBlockId(),
		Length:     meta.GetLength(),
		Checksum:   meta.GetChecksum(),
		Generation: meta.GetGeneration(),
		Seq:        meta.GetSeq(),
	}
}

func blockMetaRecordToProto(meta BlockMetaRecord) *controlplane.BlockMeta {
	return &controlplane.BlockMeta{
		BlockId:    meta.BlockID,
		Length:     meta.Length,
		Checksum:   meta.Checksum,
		Generation: meta.Generation,
		Seq:        meta.Seq,
	}
}

// [修复]：解耦原记录结构体，将动态 Addr 作为依赖注入参数传入
// 这是消除脏元数据（如节点 IP 变更导致数据不可达）的关键一步
func locationToProto(record blockLocationRecord, addr string) *controlplane.BlockLocation {
	return &controlplane.BlockLocation{
		BlockId: record.meta.BlockID,
		NodeId:  string(record.nodeID),
		Addr:    addr,
		Tier:    record.tier,
		Meta:    blockMetaRecordToProto(record.meta),
		Version: record.version,
	}
}

func nodeStateFromProto(state controlplane.NodeState) NodeState {
	switch state {
	case controlplane.NodeState_NODE_STATE_ALIVE:
		return NodeStateAlive
	case controlplane.NodeState_NODE_STATE_SUSPECT:
		return NodeStateSuspect
	case controlplane.NodeState_NODE_STATE_DOWN:
		return NodeStateDown
	case controlplane.NodeState_NODE_STATE_TOMBSTONE:
		return NodeStateTombstone
	default:
		return NodeStateUnknown
	}
}

// toStatusError 包装统一的 gRPC 状态码返回，屏蔽内部逻辑 error details 对外的泄露
func toStatusError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrEmptyNodeID), errors.Is(err, ErrEmptyNodeAddr), errors.Is(err, ErrNilNode), errors.Is(err, ErrNilMeta), errors.Is(err, ErrInvalidLocateN), errors.Is(err, ErrInvalidLocationEvent):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrNodeNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrNodeTombstoned):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrNoAliveNodes):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
