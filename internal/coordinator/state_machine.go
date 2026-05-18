package coordinator

import (
	"context"
	"kvcache/proto/controlplane"
	"sort"
	"sync"
	"time"
)

// blockLocationRecord 记录了单个数据块（Block）在某一个物理节点上的具体存放状态
type blockLocationRecord struct {
	nodeID  NodeID                   // 存放该 Block 的节点 ID
	tier    controlplane.StorageTier // 存储层级（例如：堆外内存、NVMe 磁盘）
	meta    BlockMetaRecord          // 数据块的元数据属性
	version uint64                   // 该记录的单调递增版本号（用于解决分布式网络下的乱序覆盖问题）
}

// BlockMetaRecord 是控制面内部专用的元数据结构。
type BlockMetaRecord struct {
	BlockID    uint64
	Length     uint64
	Checksum   uint32
	Generation uint64
	Seq        uint64
}

// locationKey 作为 locationBlock 内部 Map 的联合键
// 物理意义：精确到 Tier 级别。因为同一个节点（NodeID）可能同时在内存（TierMemory）
// 和磁盘（TierDisk）中各自拥有一份该 Block 的副本或降级状态。
type locationKey struct {
	nodeID NodeID
	// 标记这个数据块（Block）在物理机器上，到底是被存放在了“哪一种硬件介质”里。 内存 硬盘等
	tier controlplane.StorageTier
}

// locationBlock 维护了全局唯一的某一个 BlockID 在全网所有节点上的状态集合。
// 这是控制面最高频并发读写的数据结构。
type locationBlock struct {
	// records 记录当前存活的有效副本。
	records map[locationKey]blockLocationRecord

	// versions 记录该 Block 的历史最大版本号（包含已被删除的节点）。
	versions map[locationKey]uint64

	// sorted 缓存了基于特定规则排序后的 records 列表。
	// 物理意义：读放大优化（OCC）。GetBlockLocations 是高频读接口，避免每次查询都执行昂贵的 O(NlogN) 排序。
	sorted []blockLocationRecord

	// deletedAt 记录某个副本被彻底删除的物理时间。
	// 物理意义：用于后台 GC 协程。为了防止 versions 墓碑字典无限膨胀导致 OOM，
	// GC 协程会根据该时间戳清理过期（超出 TTL）的防乱序墓碑。
	deletedAt map[locationKey]time.Time

	// dirty 脏数据标记。当 records 发生变更时置 true，提示下次读取时需要重新生成 sorted 切片。
	dirty bool

	// version 记录该 Block 整体的宏观最新版本号。
	version uint64
}

// locationShard 内存分片池
// 物理意义：如果用一个全局大 Map 存所有的 Block，在高频汇报下全局锁会导致严重排队。
// 拆分成 64 个 Shard（分片），利用 BlockID 哈希打散，将写锁冲突概率降低 64 倍。
type locationShard struct {
	mu     sync.RWMutex
	blocks map[uint64]*locationBlock // Key 为 BlockID
}
type LocationEventKind uint8

const (
	// 写入 / 更新
	LocationEventUpsert LocationEventKind = iota + 1
	LocationEventDelete
)

// LocationEvent 是追加写入 WAL（预写式日志）的最小单元实体。
// 物理意义：它代表了集群中一个具体的事件状态。在机器断电重启后，
// 控制面需要通过重新读取这些事件流来精确重放当时的数据流动过程。
type LocationEvent struct {
	// Kind 区分事件类型（如：存入数据块 vs 移除数据块）。
	Kind LocationEventKind

	// NodeID 指明该事件影响的具体物理节点标识。
	NodeID NodeID

	// Tier 指明该数据块被存放到了哪个存储层（内存/磁盘），以便路由下发给 P2P 传输时指明存储来源。
	Tier controlplane.StorageTier

	// Meta 保存数据块的具体元数据（比如长度、校验和）。
	// 在 LocationEventUpsert (宣告保存) 时该字段不能为空。
	Meta BlockMetaRecord

	// BlockID 唯一标识一个独立的数据块（KV Tensor 数据块）。
	BlockID uint64

	// Version 是由 locationClock 生成的全局单调递增逻辑时间戳。
	// 物理意义：核心防乱序机制。用于确保当并发落盘导致 I/O 乱序到达时，
	// 也能通过严格的大小比对，拒绝让陈旧的（Version 更小）的数据覆盖新数据。
	Version uint64

	// DeletedAt 记录该记录（墓碑）被物理删除的时间点。
	// 物理意义：主要配合后台的 LocationTombstoneGC，帮助控制面判断何时才能安全地在物理磁盘上将该墓碑彻底消除，以控制 WAL 的体积膨胀。
	DeletedAt time.Time
}

// LocationStore 定义了持久化存储（WAL）的行为契约
type LocationStore interface {
	// Load 支持流式回调加载历史日志。
	// 物理意义：防 OOM 炸弹。绝不能返回全量切片，而是每读到一条日志就触发 apply 函数推入内存，
	// 使得启动时无论 WAL 有多大（如上亿条），内存常数都是 O(1)。
	Load(ctx context.Context, apply func(LocationEvent) error) error

	// Append 执行绝对的顺序追加写（Append-only），用于高吞吐落盘。
	Append(ctx context.Context, event LocationEvent) error
}

func (s *ControlPlaneService) loadLocations(ctx context.Context) error {
	var version uint64
	err := s.store.Load(ctx, func(e LocationEvent) error {
		if e.Version == 0 {
			return nil
		}
		// 重放状态机：将历史操作重新在内存中执行一遍
		s.applyLocationEvent(e)
		// 寻找历史上最大的逻辑时钟刻度
		if e.Version > version {
			version = e.Version
		}
		return nil
	})
	if err != nil {
		return err
	}
	// ?????
	// 强制校准（Sync）：将系统重启后的逻辑时钟基线对齐到历史最大版本，
	// 防止新生成的事件与旧事件产生版本号冲突。
	s.locationClock.Store(version)
	s.locationVersion.Store(version)
	return nil
}

// appendLocationEvent WAL 日志落盘入口，分配全局单调递增 Version
func (s *ControlPlaneService) appendLocationEvent(ctx context.Context, event LocationEvent) (LocationEvent, error) {
	// 全局单调递增的唯一号牌（Version）
	event.Version = s.locationClock.Add(1)
	switch event.Kind {
	case LocationEventUpsert:
		// 防止 RPC 调用时发生“信封和信件内容不一致”的漏洞,只确保最后是真实数据
		event.BlockID = event.Meta.BlockID
		// BlockID == 0：数据块没有合法 ID（0 通常是初始化的零值），不知道存的是什么。NodeID == ""：不知道这个数据块到底被存到了哪台物理机器上（幽灵数据）。Tier == UNKNOWN：不知道数据是在内存里还是磁盘里。这会导致后面无法做智能的读延迟优化。
		if event.BlockID == 0 || event.NodeID == "" || event.Tier == controlplane.StorageTier_STORAGE_TIER_UNKNOWN {
			return LocationEvent{}, ErrInvalidLocationEvent
		}
	case LocationEventDelete:
		if event.BlockID == 0 || event.NodeID == "" || event.Tier == controlplane.StorageTier_STORAGE_TIER_UNKNOWN {
			return LocationEvent{}, ErrInvalidLocationEvent
		}
	default:
		return LocationEvent{}, ErrInvalidLocationEvent
	}
	// 写入wal
	if s.store != nil {
		if err := s.store.Append(ctx, event); err != nil {
			return LocationEvent{}, err
		}
	}
	// 落盘成功后，应用到内存状态机
	s.applyLocationEvent(event)
	// 更新安全水位线
	s.storeMaxLocationVersion(event.Version)
	return event, nil
}
func (s *ControlPlaneService) applyLocationEvent(event LocationEvent) {
	switch event.Kind {
	case LocationEventUpsert:
		s.upsertLocation(blockLocationRecord{
			nodeID:  event.NodeID,
			tier:    event.Tier,
			meta:    event.Meta,
			version: event.Version,
		})
	case LocationEventDelete:
		s.deleteLocation(event.BlockID, event.NodeID, event.Tier, event.Version)
	}
}

// storeMaxLocationVersion 无锁 CompareAndSwap 更新安全水位线
// 加for循环是为了保正并发的时候能把所有的改变计算上
func (s *ControlPlaneService) storeMaxLocationVersion(version uint64) {
	for {
		current := s.locationVersion.Load()
		if version <= current {
			return
		}
		if s.locationVersion.CompareAndSwap(current, version) {
			return
		}
	}
}

// upsertLocation 修改内存状态树（新增/更新）
func (s *ControlPlaneService) upsertLocation(record blockLocationRecord) {
	shard := s.locationShard(record.meta.BlockID)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	blockID := record.meta.BlockID
	block := shard.blocks[blockID]
	if block == nil {
		block = &locationBlock{
			records:   make(map[locationKey]blockLocationRecord),
			versions:  make(map[locationKey]uint64),
			deletedAt: make(map[locationKey]time.Time),
		}
		shard.blocks[blockID] = block
	}
	key := locationKey{
		nodeID: record.nodeID,
		tier:   record.tier,
	}
	if record.version <= block.versions[key] {
		return
	}
	block.records[key] = record
	block.versions[key] = record.version
	// 相当于复活了，从死亡名单删除
	delete(block.deletedAt, key)
	if record.version > block.version {
		// 全网状态刚刚发生了实质性刷新
		block.version = record.version
	}
	block.dirty = true // 标记脏数据，提示下一次读取需重新排序
}
func (s *ControlPlaneService) deleteLocation(blockID uint64, nodeID NodeID, tier controlplane.StorageTier, version uint64) {
	shard := s.locationShard(blockID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	block := shard.blocks[blockID]
	if block == nil {
		block = &locationBlock{
			records:   make(map[locationKey]blockLocationRecord),
			versions:  make(map[locationKey]uint64),
			deletedAt: make(map[locationKey]time.Time),
			version:   version,
		}
		shard.blocks[blockID] = block
	}
	key := locationKey{nodeID: nodeID, tier: tier}
	// 同样执行防乱序防御
	if version <= block.versions[key] {
		return
	}
	delete(block.records, key)        // 物理清除活跃记录
	block.versions[key] = version     // 留下版本号作为墓碑，阻挡旧请求
	block.deletedAt[key] = time.Now() // 记录死亡时间，供 GC 回收
	if version > block.version {
		block.version = version
	}
	block.dirty = true
}
func (s *ControlPlaneService) sortedLocations(blockID uint64) []blockLocationRecord {
	shard := s.locationShard(blockID)
	for {
		shard.mu.RLock()
		block := shard.blocks[blockID]
		if block == nil {
			shard.mu.RUnlock()
			return nil
		}
		if !block.dirty {
			sorted := block.sorted
			shard.mu.RUnlock()
			return append([]blockLocationRecord(nil), sorted...)
		}
		version := block.version
		records := make([]blockLocationRecord, 0, len(block.records))
		for _, record := range block.records {
			records = append(records, record)
		}
		shard.mu.RUnlock()
		// 离线排序 (耗时计算脱离了临界区)
		sort.Slice(records, func(i, j int) bool {
			if records[i].nodeID == records[j].nodeID {
				return records[i].tier < records[j].tier
			}
			return records[i].nodeID < records[j].nodeID
		})

		// 第二阶段：持有写锁，比对版本进行状态更新 (类似 CAS 思想)
		shard.mu.Lock()
		block = shard.blocks[blockID]
		if block == nil {
			shard.mu.Unlock()
			return nil
		}
		// 如果在这期间没有其他人修改过数据 (version 相等)，则安全覆盖
		if block.dirty && block.version == version {
			block.sorted = records
			block.dirty = false
			shard.mu.Unlock()
			return append([]blockLocationRecord(nil), records...)
		}
		// 如果 version 被改了，说明有人插队，释放写锁，再次进入 for 循环重试
		shard.mu.Unlock()
	}
}

// pruneLocationTombstones 回收过期墓碑
func (s *ControlPlaneService) pruneLocationTombstones(now time.Time) {
	// 把当前时间（now）往前倒退一个 TTL（存活时间），计算出一个截止线
	cutoff := now.Add(-locationTombstoneTTL)
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		for blockID, block := range shard.blocks {
			for key, delTime := range block.deletedAt {
				// delTime.Before()计算的是delTime > now - 10
				if !delTime.IsZero() && delTime.Before(cutoff) {
					// 抹除它的死亡时间记录。
					delete(block.deletedAt, key)
					// 抹除它的历史最大版本号
					delete(block.versions, key)
				}
			}
			if len(block.records) == 0 && len(block.versions) == 0 {
				delete(shard.blocks, blockID)
			}
		}
		shard.mu.Unlock()
	}
}
func (s *ControlPlaneService) locationShard(blockID uint64) *locationShard {
	return &s.shards[blockID%locationShardCount]
}
