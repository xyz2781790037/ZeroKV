package coordinator

import (
	"context"
	"errors"
	"fmt"
	"kvcache/proto/controlplane"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
)

const (
	locationShardCount          = 64
	routeRefreshDebounceTime    = 50 * time.Millisecond
	locationTombstoneTTL        = 10 * time.Minute
	locationTombstoneGCInterval = time.Minute
)

var (
	ErrNilRouter            = errors.New("control plane: nil router")
	ErrNilNode              = errors.New("control plane: nil node")
	ErrNilMeta              = errors.New("control plane: nil block meta")
	ErrInvalidLocationEvent = errors.New("control plane: invalid location event")
)

// ControlPlaneService 控制面的核心大脑，协调 Membership(拓扑)、Router(路由) 和 Store(持久化)。
type ControlPlaneService struct {
	controlplane.UnimplementedControlPlaneServer

	membership *Membership // 负责节点生死存活状态
	router     *Router     // 负责 radix 前缀路由计算

	store LocationStore // 底层 WAL 日志存储引擎

	// 以下使用 atomic 保证多核 CPU 架构下的无锁并发自增和可见性
	membershipVersion atomic.Uint64
	routeVersion      atomic.Uint64
	locationClock     atomic.Uint64 // 逻辑时钟，分配全局单调递增的 Event 版本号
	locationVersion   atomic.Uint64 // 当前已确认落盘的最大 Version

	// routeRefreshCh 路由刷新防抖通道。
	// 物理意义：将同步的 O(NlogN) 路由重构（极其耗费 CPU）与高频的网络心跳彻底解耦，防止雪崩。
	routeRefreshCh chan struct{}

	ctx    context.Context    // 用于管控所有后台守护协程的生命周期
	cancel context.CancelFunc // 用于优雅停机（Graceful Shutdown）时主动广播终止信号
	wg     sync.WaitGroup     // 优雅停机屏障，确保在主进程退出前，后台的 WAL 刷盘和 GC 协程能安全结束

	shards [locationShardCount]locationShard // 64个无锁冲突的元数据内存分片
}

func NewControlPlaneService(membership *Membership, router *Router) (*ControlPlaneService, error) {
	return NewControlPlaneServiceWithStore(membership, router, nil)
}
func NewControlPlaneServiceWithStore(membership *Membership, router *Router, store LocationStore) (*ControlPlaneService, error) {
	if membership == nil {
		return nil, fmt.Errorf("control plane: nil membership")
	}
	if router == nil {
		return nil, ErrNilRouter
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &ControlPlaneService{
		membership:     membership,
		router:         router,
		store:          store,
		routeRefreshCh: make(chan struct{}, 1),
		ctx:            ctx,
		cancel:         cancel,
	}
	for i := range s.shards {
		s.shards[i].blocks = make(map[uint64]*locationBlock)
	}
	// 它会把之前崩溃前保存在磁盘上的历史事件读取出来，在内存中重新计算一遍，从而恢复到崩溃前的拓扑状态。
	s.routeVersion.Store(router.Version())
	if store != nil {
		if err := s.loadLocations(context.Background()); err != nil {
			cancel()
			return nil, err
		}
	}
	s.wg.Add(2)
	// 负责监听集群的拓扑变动，一旦发现有节点上下线，在防抖时间窗口后自动重构路由环。
	go s.runRouteRefreshLoop()
	// 负责定期扫描内存，清理已经失效的墓碑，防止内存泄漏。
	go s.runLocationTombstoneGC()
	return s, nil
}
func (s *ControlPlaneService) Close() {
	if s == nil {
		return
	}
	s.cancel()
	s.wg.Wait()
}

// RegisterControlPlaneService 将控制面服务注册到 gRPC Server 中 **??**
func RegisterControlPlaneService(registrar grpc.ServiceRegistrar, service *ControlPlaneService) {
	controlplane.RegisterControlPlaneServer(registrar, service)
}

// refreshRouter 触发 radix 路由树重建，属于相对高成本的拓扑计算操作
func (s *ControlPlaneService) refreshRouter() error {
	if s.router == nil {
		return ErrNilRouter
	}
	return s.router.Refresh()
}
func (s *ControlPlaneService) scheduleRouterRefresh() {
	if s == nil {
		return
	}
	// 安全退出检查
	select {
	case <-s.ctx.Done():
		return
		//为了不阻塞
	default:
	}
	// 这只有一个信号可以进去
	// 即使突发 1000 个节点掉线，也只会向 channel 塞入 1 个信号，多余的被 default 丢弃。（防止出现1000个同时refresh撑爆cpu）
	select {
	case s.routeRefreshCh <- struct{}{}:
	case <-s.ctx.Done():
	default:
	}
}

// runRouteRefreshLoop 后台路由防抖守护协程 (Debouncer)
func (s *ControlPlaneService) runRouteRefreshLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.routeRefreshCh:
		}
		// 开启时间窗口：收到第一个信号后，强行等待 50ms。
		// 这 50ms 内不管再死多少个节点，都只攒着，等时间过了统一只算 1 次。
		timer := time.NewTimer(routeRefreshDebounceTime)
		select {
		case <-s.ctx.Done():
			// 结束之前把这个任务结束
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		}
		// 疯狂地从通道里掏信号，直到掏空为止
		for {
			select {
			case <-s.routeRefreshCh:
				continue
			case <-s.ctx.Done():
				return
			default:
			}
			break
		}
		// 最后的安全检查
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		if err := s.refreshRouter(); err == nil {
			s.routeVersion.Add(1)
		}
	}
}

// 推进拓扑时钟，将当前集群的“物理名单版本号”强行加 1。
func (s *ControlPlaneService) bumpMembershipVersion() uint64 {
	return s.membershipVersion.Add(1)
}

// 无锁、极速地读取当前的物理名单版本号。
func (s *ControlPlaneService) currentMembershipVersion() uint64 {
	return s.membershipVersion.Load()
}

// 无锁读取当前的路由树版本号。
func (s *ControlPlaneService) currentRouteVersion() uint64 {
	if s == nil {
		return 0
	}
	return s.routeVersion.Load()
}
func (s *ControlPlaneService) runLocationTombstoneGC() {
	defer s.wg.Done()
	ticker := time.NewTicker(locationTombstoneGCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.pruneLocationTombstones(time.Now())
		}
	}
}
