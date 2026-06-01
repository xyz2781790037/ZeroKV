# kvcache

`kvcache` 是一个面向大模型推理场景的 KV Cache 数据面/控制面原型。它的核心目标是让 C++ 推理侧把大块 KV 数据写入共享内存，再由 Go daemon 接管、校验、缓存、登记位置，并在多节点之间通过控制面元数据和 P2P TCP 完成远程读取。

当前项目已经跑通了单机端到端链路：

1. C++ demo 通过 UDS 向 Go daemon 申请 daemon-owned POSIX shared memory slot。
2. C++ mmap 该 slot，把 block payload 写入 Go daemon 管理的共享内存。
3. C++ 通过 Unix Domain Socket 发送 `BlockReady` 元数据给 Go daemon。
4. Go daemon 直接索引自己的 shared memory slice，校验 CRC32。
5. Go daemon 通过控制面登记该 block 的位置。

分布式链路的代码骨架已经存在：节点 membership、控制面 gRPC、block location 元数据、P2P TCP 拉取、本地 miss 后远程 fetch 并回填。但多节点端到端流程还需要继续验证。

## 系统模型
我们有多级存储，第一层内存池，第二层是磁盘，第三层是网络，去其他节点
这个项目把 KV Cache 拆成两类信息：

- **数据面 payload**：真正的大块二进制 KV 数据，不走 protobuf，不走 gRPC。本机写入时，C++ 先向 Go daemon 申请 daemon-owned POSIX shared memory slot，再 mmap 写入该 slot；Go 侧直接把这段共享内存 slice 登记为本地 block。跨节点读取时通过自定义 P2P TCP 协议传输。
- **控制面 metadata**：block id、长度、checksum、节点地址、存储层级、membership、路由版本等小元数据，通过 UDS 或 gRPC 传递。

这样设计的原因是：KV Cache block 可能很大，如果所有 payload 都走 gRPC/protobuf，会引入额外拷贝和序列化成本。项目中采用的是“payload 走裸字节流，metadata 走结构化协议”的方式。

## 顶层目录

```text
.
├── cmd/                    Go daemon 启动入口
├── csrc/                   C++ 客户端库和 demo
├── internal/               Go 内部模块：IPC、存储、网络、控制面、分布式 store
├── pkg/                    Go 公共包：UDS 协议编码、日志
├── proto/                  控制面 protobuf 定义和生成代码
├── go.mod / go.sum         Go module 依赖
├── Makefile                当前为空，后续可放常用构建命令
└── README.md               项目说明文档
```

## 每个文件的职责

### `cmd/`

#### `cmd/main.go`

Go daemon 的主入口，负责把所有模块组装起来。

它做的事情包括：

- 解析启动参数：
  - `-socket`：C++ 客户端连接的 UDS 路径。
  - `-p2p-addr`：本节点 P2P TCP 服务监听地址。
  - `-control-addr`：本节点控制面 gRPC 监听地址。
  - `-node-id`：当前节点 ID。
  - `-node-addr`：对外暴露的 P2P 地址。
  - `-join-control-addrs`：要加入的其他节点控制面地址。
  - `-offheap-bytes`：Go 本地 offheap pool 大小，主要用于远端回填、disk promote 和旧客户端兼容路径。
  - `-shm-name`：Go daemon 创建的 POSIX shared memory pool 名称。
  - `-shm-bytes`：Go daemon-owned POSIX shared memory pool 大小；默认等于 `-offheap-bytes`。
  - `-disk-dir`：本地 disk tier 目录；为空则关闭磁盘层。
- 创建 `storage.OffheapPool` 和 `storage.Handler`。
- 如果配置了 `-disk-dir`，创建 `storage.DiskTier`，扫描目录中已有的 `.kvblk` 文件，并把恢复出的磁盘 block 登记到本地控制面。
- 创建本节点 `Membership`、一致性哈希 `Router`、`ControlPlaneService`。
- 连接 peer control plane，并用 `grpcControlPlaneAdapter` 把生成的 gRPC client 适配成项目内部接口。
- 创建 `distributed.Store`，把本地存储、控制面和 P2P 读回填逻辑组合起来。
- 启动：
  - UDS server：接收 C++ 写入通知。
  - control-plane gRPC server：处理 membership 和 block location RPC。
  - P2P TCP server：给其他节点传输 block payload。
  - membership sync loop：和 peer 控制面同步拓扑。
- 处理 SIGINT/SIGTERM/SIGQUIT，优雅关闭 UDS、gRPC、P2P 和后台 goroutine。

### `csrc/`

#### `csrc/connector.h`

C++ 客户端门面接口定义。上层推理引擎只需要使用 `KVCacheConnector`，不需要直接关心 UDS、共享内存、协议编码。

主要类型：

- `KVCacheConnectorOptions`：配置 UDS socket、共享内存名、共享内存大小、是否等待 ACK。
- `KVCacheBlockMeta`：一次 block 写入后的元数据，包含 seq、block id、shm name、offset、length、checksum。
- `KVCacheConnector`：C++ 侧核心入口，提供：
  - `Connect()`
  - `Close()`
  - `PutBlock()`
  - `ResetLocalPool()`
  - `connected()`

#### `csrc/connector.cc`

`KVCacheConnector` 的实现。

`PutBlock()` 的工作流程：

1. 检查 `data != nullptr` 且 `length > 0`。
2. 生成分配请求 seq，并通过 `UDSClient` 发送 `AllocateBlock` frame 给 Go daemon。
3. Go daemon 从自己拥有的 POSIX shared memory pool 中分配 slot，并返回 shm name、offset、length。
4. C++ 打开 `/dev/shm/<shm_name>`，mmap 返回的 slot，把调用方传入的 payload 写进去。
5. C++ 计算 CRC32 checksum。
6. C++ 生成提交 seq，通过 `UDSClient` 发送 `BlockReady` frame 给 Go daemon。
7. 如果配置 `wait_for_ack=true`，等待 Go daemon 返回 ACK 或 ERROR。
8. 如果调用方传入 `meta`，回填这次写入的物理坐标。

注意：本机写入路径的共享内存由 Go daemon 创建和回收。C++ 不拥有共享内存池生命周期，因此 Go 可以直接长期持有该 slice，不需要在接管时再复制到 offheap pool。

#### `csrc/shm_allocator.h`

C++ 共享内存分配器接口定义。

它抽象的是一个基于 POSIX shm + mmap 的 bump-pointer allocator：

- `Init()`：创建/打开 shm，设置大小，mmap 到当前进程。
- `Allocate()`：只分配空间，不写数据。
- `Write()`：分配空间、复制 payload、计算 checksum。
- `Reset()`：把分配游标归零。
- `Close()`：munmap、close fd、shm_unlink。
- `ChecksumIEEE()`：计算 CRC32-IEEE。

#### `csrc/shm_allocator.cc`

共享内存分配器实现。

这是旧版 C++ owned shm 路径的分配器实现，目前主写入路径已经切到 daemon-owned SHM：

- 把业务 shm name 规范成 POSIX shm name：
  - C++ 内部 `posix_name_` 以 `/` 开头，例如 `/kvcache_cpp_1234`。
  - 通过 UDS 发给 Go 的 `shm_name_` 不带 `/`，例如 `kvcache_cpp_1234`。
- 用 `shm_open()` 创建共享内存对象。
- 用 `ftruncate()` 设置共享内存大小。
- 用 `mmap()` 映射到 C++ 进程地址空间。
- 每次写入按 64 字节对齐分配。
- `Close()` 会 `shm_unlink()`，所以 C++ demo 退出后会清理自己的 shm 对象。

当前 `KVCacheConnector::PutBlock()` 不再使用这个 allocator 创建本地 shm；它会先向 Go daemon 申请 slot，再 mmap daemon 返回的 shm 坐标。

#### `csrc/uds_client.h`

C++ UDS 客户端接口定义。

它负责和 Go daemon 的 `internal/ipc/uds_server.go` 通信，协议常量必须和 `pkg/protocol/codec.go` 保持一致：

- magic：`0x5043564b`
- wire version：`1`
- header size：`32`
- message type：
  - `1` = BlockReady
  - `2` = ACK
  - `3` = ERROR
  - `4` = AllocateBlock
  - `5` = BlockAllocation

#### `csrc/uds_client.cc`

C++ UDS 客户端实现。

它负责：

- 创建 Unix socket。
- 连接 Go daemon 的 socket path。
- 编码 `AllocateBlock` frame，向 Go daemon 申请 daemon-owned SHM slot。
- 解析 `BlockAllocation` frame，得到 shm name、offset、length。
- 编码 `BlockReady` frame：
  - header：magic、version、header size、message type、flags、payload length、seq、reserved。
  - payload：block id、offset、length、checksum、shm name length、reserved、shm name bytes。
- 用 `send()` 完整写出 frame。
- 如果需要 ACK，用 `recv()` 读取 Go 返回的 ACK/ERROR frame。

#### `csrc/CMakeLists.txt`

C++ 构建脚本。

它会构建：

- `kvcache_client` 静态库：
  - `connector.cc`
  - `shm_allocator.cc`
  - `uds_client.cc`
- `kvcache_client` CLI 可执行文件。

Linux 下会链接 `Threads::Threads` 和 `rt`。

#### `csrc/client/kvcache_client.cc`

C++ 写入 CLI。

运行 `put` 或 `text` 后会：

1. 创建 `KVCacheConnector`。
2. 连接指定 UDS socket。
3. 向 Go daemon 申请 daemon-owned SHM slot。
4. mmap 该 slot 并写入 payload。
5. 发送 `BlockReady` 给 Go daemon。
6. 打印发布成功后的 seq、block id、shm name、offset、length、checksum。

#### `csrc/build/`

CMake 生成的构建目录，不是源码。

常见产物：

- `csrc/build/libkvcache_client.a`
- `csrc/build/kvcache_client`
- `csrc/build/CMakeCache.txt`
- `csrc/build/CMakeFiles/...`

这些文件可以重新生成，通常不需要手工阅读。

### `proto/`

#### `proto/control_plane.proto`

控制面 gRPC 协议定义。

它定义了：

- `ControlPlane` service：
  - `RegisterNode`
  - `Heartbeat`
  - `LeaveNode`
  - `GetMembership`
  - `SyncMembership`
  - `RouteBlock`
  - `AnnounceBlock`
  - `ForgetBlock`
  - `GetBlockLocations`
- `NodeState`：节点状态。
- `StorageTier`：存储层级，当前支持 memory 和 disk，控制面会记录 block 位于哪个 tier。
- `Node`：节点元数据。
- `BlockMeta`：block 长度、checksum、generation、seq。
- `BlockLocation`：某个 block 在哪个 node、哪个 addr、哪个 tier 上。

控制面只传小元数据，不传大 payload。

#### `proto/controlplane/control_plane.pb.go`

由 protobuf 生成的 Go message 类型代码。

业务代码会使用这里的类型，例如：

- `controlplane.Node`
- `controlplane.BlockMeta`
- `controlplane.BlockLocation`
- `controlplane.AnnounceBlockRequest`
- `controlplane.GetBlockLocationsResponse`

#### `proto/controlplane/control_plane_grpc.pb.go`

由 protobuf 生成的 Go gRPC client/server 代码。

业务代码会使用：

- `controlplane.ControlPlaneClient`
- `controlplane.ControlPlaneServer`
- `controlplane.RegisterControlPlaneServer`
- `controlplane.NewControlPlaneClient`

### `pkg/`

#### `pkg/protocol/codec.go`

C++ 和 Go 之间的 UDS wire protocol 编解码。

它定义：

- frame header 布局。
- `BlockReady` payload 布局。
- `ReadFrame()`：从 UDS 字节流中读出完整 frame。
- `WriteFrame()`：把 ACK/ERROR 写回 C++。
- `NewBlockReadyFrame()`：构造 BlockReady frame。
- `DecodeBlockReadyFrame()`：解析 C++ 发来的 BlockReady。
- `NewAckFrame()`：构造 ACK。
- `NewErrorFrame()`：构造 ERROR。

这是 C++ `uds_client.cc` 必须严格对齐的 Go 端协议实现。

#### `pkg/logger/logger.go`

项目日志封装。

Go daemon 启动、UDS 连接、P2P 错误、membership sync 错误等都会通过它输出。`cmd/main.go` 中 daemon 出错退出时也使用它的 `Fatal()`。

### `internal/ipc/`

#### `internal/ipc/uds_server.go`

Go 侧 Unix Domain Socket server。

它负责：

- 监听 `-socket` 指定的 UDS path。
- 接受 C++ 客户端连接。
- 循环读取 UDS frame。
- 遇到 `MessageTypeAllocateBlock` 时解析出 `protocol.AllocateBlock`，调用 handler 分配 daemon-owned SHM slot，并返回 `BlockAllocation`。
- 遇到 `MessageTypeBlockReady` 时解析出 `protocol.BlockReady`。
- 调用 `BlockReadyHandler.HandleBlockReady()`。
- 处理成功返回 ACK，失败返回 ERROR。
- 停机时关闭 listener、关闭所有活跃连接、清理 socket 文件。

这个文件只负责本机 IPC，不负责 mmap、存储、路由、分布式同步。真正处理 block 的是传入的 handler，目前启动时传入的是 `distributed.Store`。

#### `internal/ipc/shm_mapper.go`

Go 侧共享内存 mmap 工具。

它负责：

- 把 C++ 发来的 shm name 规范成 `/dev/shm/<name>`。
- 校验 shm name，防止路径穿越、NUL 截断、非法 `/`。
- 根据 offset/length 做页对齐 mmap。
- 返回精确的 `Data` 切片。
- 用 CRC32 校验 C++ 发来的 checksum。
- `Unmap()` 时释放原始 mapping。

这是旧客户端兼容路径：如果 block 来自 C++ owned shm，Go 会通过这里 mmap 并复制到 offheap。daemon-owned SHM 主路径不需要重新 mmap，它直接使用 `SharedMemoryPool` 中已经映射好的 slice。

### `internal/storage/`

#### `internal/storage/shared_memory_pool.go`

Go daemon-owned POSIX shared memory pool。

它负责：

- 创建 `/dev/shm/<shm-name>`。
- `ftruncate()` 设置 pool 大小。
- 在 Go daemon 内 mmap 整个 pool。
- 为 C++ 客户端的 `AllocateBlock` 请求分配 64 字节对齐 slot。
- 返回 shm name、offset、length 给 C++。
- 在 daemon 退出时 munmap、close 并 unlink shm 文件。

这是真正的本机写入主路径：C++ 只写 Go daemon 分配的 slot，Go 直接把同一个 slice 登记进本地 block index。

#### `internal/storage/offheap_pool.go`

Go 本地匿名 offheap 内存池。

它用匿名 `mmap` 申请一大块进程私有内存，然后用原子 bump pointer 分配 block 空间。

关键点：

- 不受 Go GC 管理，适合保存大块 KV Cache payload。
- `Alloc()` 返回指向 offheap pool 的 `[]byte`。
- `Reset()` 只把分配游标归零。
- `Release()` munmap 整个 pool。

它现在主要用于远端 P2P 回填、disk promote，以及旧客户端 C++ owned shm 的兼容路径。本机 C++ 写入主路径使用 daemon-owned POSIX shared memory pool。

#### `internal/storage/handler.go`

Go 本地内存存储的核心处理器。

它负责：

- 接收 UDS 解码后的 `protocol.AllocateBlock`，从 daemon-owned SHM pool 分配 slot。
- 接收 UDS 解码后的 `protocol.BlockReady`。
- 如果 block 来自 daemon-owned SHM，直接使用已经 mmap 好的 shared memory slice。
- 如果 block 来自旧客户端 C++ owned SHM，则调用 `ipc.MapBlockReady()` mmap 并复制到 offheap 兼容路径。
- 校验 checksum。
- 建立 `blockID -> Block` 索引。
- 支持 `Acquire()` 零拷贝借用 block。
- 支持 `OpenBlock()` 给 P2P server 以 stream 方式读 block。
- 支持 `NewImportBlockWriter()`，用于从远端 P2P 拉取 block 后先暂存、校验、再提交到本地索引。

`Handler` 是单机本地缓存的核心。

#### `internal/storage/evict.go`

本地内存淘汰和 disk tier 迁移逻辑。

它定义：

- `EvictPolicy`
- `KeepAllPolicy`
- `KeepGenerationPolicy`
- `EvictBlockIDsPolicy`
- `Evict()`
- `ReclaimAll()`
- `SpillToDisk()`
- `PromoteFromDisk()`

当前 offheap pool 是 bump allocator，所以逻辑淘汰只删除索引，不会回收中间碎片；真正物理复用需要 `Reset()` 或 `ReclaimAll()`。

#### `internal/storage/disk_tier.go`

本地磁盘层实现。

它用于把内存中的 block 写成 `.kvblk` 文件，并能重新打开/读取/校验。

主要职责：

- `NewDiskTier(root)`：创建磁盘目录并扫描已有 `.kvblk` 文件。
- `Put(block)`：写临时文件、fsync、rename 成正式文件、sync 目录。
- `Open(blockID)`：打开 block 文件并校验 header。
- `Get(blockID)`：读取完整数据副本。
- `Delete(blockID)`：删除磁盘文件并更新索引。
- `Stats()`：返回磁盘层统计。

这是未来内存不足时做冷热分层的基础。

### `internal/network/`

#### `internal/network/zerocopy_tcp.go`

Go 节点之间的 P2P TCP server 和自定义 TCP frame 协议。

它定义：

- TCP magic/version/header。
- message type：
  - `tcpMessageGetBlock`
  - `tcpMessageBlock`
  - `tcpMessageError`
- `Server`：监听 P2P 地址，处理其他节点的 block 拉取请求。
- `BlockStore` 接口：只要求底层实现 `OpenBlock()`。
- `handleConn()`：读请求 header，调用 store 打开 block，写回 block header 和 payload。
- deadline、payload timeout、连接数限制、错误回传等网络防御逻辑。

这个模块不关心 block 存在内存还是磁盘，只依赖 `OpenBlock()`。

#### `internal/network/p2p_client.go`

Go 节点之间的 P2P TCP client。

它负责：

- 连接远端 node addr。
- 发送 `GetBlock` 请求。
- 读取远端 block header。
- 校验 block id、length、checksum。
- 把 payload stream 到传入的 writer。

`distributed.Store` 在本地 miss 时会调用它，把远端 payload 拉到本地 `ImportBlockWriter`。

### `internal/coordinator/`

#### `internal/coordinator/membership.go`

集群节点成员表。

它维护：

- 节点 ID。
- 节点 P2P 地址。
- 节点状态：
  - alive
  - suspect
  - down
  - tombstone
- 节点版本号。
- 最后心跳时间。

主要能力：

- `NewMembership(self)`
- `Upsert(node)`
- `Heartbeat(id, at)`
- `Mark(id, state)`
- `Remove(id)`
- `Get(id)`
- `List()`
- `AliveNodes()`

这里是控制面拓扑事实来源。

#### `internal/coordinator/consistent_hash.go`

一致性哈希环实现。

它负责把 block id 映射到一个或多个 alive 节点：

- 支持虚拟节点 replicas。
- 根据 membership 中的 alive nodes 构建 hash ring。
- `Locate(blockID)` 找 primary。
- `LocateN(blockID, n)` 找多个候选节点。
- `UpdateMembership()` 在 membership 变化后重建 ring。
- 维护 ring version。

分布式 KV cache 里，路由层用它回答“这个 block 理论上应该归哪些节点”。

#### `internal/coordinator/router.go`

控制面对一致性哈希的业务封装。

它把 `Membership` 和 `ConsistentHash` 组合起来，提供：

- `Refresh()`：从 membership 重建 hash ring。
- `RouteBlock()`
- `RouteBlockReplicas()`
- `RouteBlockReplicasWithVersion()`
- `Route()`：返回 primary、candidates、当前节点是否参与。
- `OwnsBlock()`：判断本节点是否应持有某个 block。

#### `internal/coordinator/control_plane.go`

控制面服务核心对象。

它组合：

- `Membership`
- `Router`
- 可选的 `LocationStore`
- membership version
- route version
- location clock/version
- block location shards
- 后台 route refresh loop
- 后台 location tombstone GC

它还提供：

- `NewControlPlaneService()`
- `NewControlPlaneServiceWithStore()`
- `RegisterControlPlaneService()`
- `Close()`

这个文件更像控制面的“对象生命周期管理”和后台任务管理。

#### `internal/coordinator/grpc_api.go`

控制面 gRPC API 实现。

它实现 `proto/control_plane.proto` 中定义的 RPC：

- `RegisterNode()`：注册节点，必要时刷新路由。
- `Heartbeat()`：更新心跳，节点从非 alive 恢复时刷新路由。
- `LeaveNode()`：生成 tombstone。
- `GetMembership()`：返回当前成员表。
- `SyncMembership()`：合并 peer 传来的成员表。
- `RouteBlock()`：返回 block 的 primary 和 candidates。
- `AnnounceBlock()`：节点声明自己已经持有某个 block。
- `ForgetBlock()`：节点声明自己删除了某个 block。
- `GetBlockLocations()`：查询某个 block 当前在哪些节点上。

注意：`GetBlockLocations()` 返回的 addr 是查询时从 membership 动态 join 出来的，避免节点 IP/端口变更后 block location 里残留旧地址。

#### `internal/coordinator/state_machine.go`

block location 元数据状态机。

它记录的是“哪个 block 当前在哪些节点、哪个 tier 上存在副本”。

核心结构：

- `blockLocationRecord`
- `BlockMetaRecord`
- `locationKey`
- `locationBlock`
- `locationShard`
- `LocationEvent`
- `LocationStore`

核心逻辑：

- `appendLocationEvent()`：为 upsert/delete 分配单调递增 version，并可选写 WAL。
- `applyLocationEvent()`：把事件应用到内存状态机。
- `upsertLocation()`：新增或更新 block location。
- `deleteLocation()`：删除 location 并留下版本墓碑。
- `sortedLocations()`：返回某个 block 的位置快照。
- `pruneLocationTombstones()`：清理过期墓碑。

这里是远程读回填的关键：`distributed.Store` miss 时会查询这里拿到可拉取的远端 node addr。

### `internal/distributed/`

#### `internal/distributed/store.go`

本地存储和分布式控制面的组合层。

它实现两个关键方向：

写路径：

1. UDS server 调用 `Store.HandleBlockReady()`。
2. `Store` 先调用本地 `storage.Handler.HandleBlockReady()`。
3. 本地 offheap 写入成功后，调用控制面 `AnnounceBlock(tier=MEMORY)`。
4. 如果启用了 `DiskTier`，同步把 block 镜像到本地 `.kvblk` 文件，并调用 `AnnounceBlock(tier=DISK)`。
5. 同时尝试向 peer control plane 公告该 block 的 memory/disk 位置。

读路径：

1. P2P server 或本地调用方调用 `Store.OpenBlock(blockID)`。
2. `Store` 先查本地 `storage.Handler.OpenBlock()`。
3. 如果本地命中，直接返回 reader。
4. 如果 memory miss 且启用了 `DiskTier`，查本地磁盘层。
5. 如果本地 disk 命中，优先 promote 回 memory 并公告 `tier=MEMORY`；如果 promote 因内存不足失败，则直接从 disk stream 返回。
6. 如果本地 memory/disk 都 miss，调用控制面 `GetBlockLocations()`。
7. 按 tier/version 排序 location，优先 remote memory，再考虑 remote disk。
8. 跳过本机 location。
9. 用 `network.Client.FetchBlockTo()` 从远端 P2P 拉取 payload。
10. 用 `storage.ImportBlockWriter` 校验并提交到本地 offheap。
11. 回填成功后再次 `AnnounceBlock(tier=MEMORY)`，让控制面知道本节点也有内存副本。
12. 如果启用了 `DiskTier`，把远端回填的数据也镜像到本地 disk，并公告 `tier=DISK`。
13. 再打开本地 block 返回 reader。

这是项目从单机缓存进入分布式缓存的核心 glue layer。

## 数据从哪里来，到哪里去

### 写入路径：C++ -> Go 本机缓存 -> 控制面

```text
C++ caller
  |
  | PutBlock(block_id, data, length)
  v
csrc/connector.cc
  |
  | UDSClient::AllocateBlock()
  v
Go daemon-owned POSIX SHM slot: /dev/shm/<shm_name>@offset
  |
  | mmap + memcpy + checksum
  v
C++ writes payload into daemon-owned SHM slot
  |
  | UDSClient::SendBlockReadyAndWait()
  v
Go UDS server: internal/ipc/uds_server.go
  |
  | protocol.DecodeBlockReadyFrame()
  v
distributed.Store.HandleBlockReady()
  |
  | storage.Handler.HandleBlockReady()
  v
storage.SharedMemoryPool slice, verify checksum
  |
  | blockID -> Block index
  v
coordinator.ControlPlaneService.AnnounceBlock()
  |
  | block location metadata
  v
control-plane state machine
```

这条链路里，payload 的流向是：

```text
C++ 内存 -> Go daemon-owned POSIX shared memory -> Go block index
```

metadata 的流向是：

```text
C++ BlockReady frame -> Go UDS protocol -> local storage index -> control plane block location
```

### 本地读取路径：P2P server 从本机发出 block

```text
remote node
  |
  | TCP GetBlock(block_id)
  v
internal/network/zerocopy_tcp.go
  |
  | Server.handleConn()
  v
distributed.Store.OpenBlock(block_id)
  |
  | storage.Handler.OpenBlock()
  v
BlockLeaseReader
  |
  | io.CopyN(conn, reader, length)
  v
remote node receives payload
```

这里 `BlockLeaseReader` 持有底层 block lease，避免读取过程中 `Reset/Evict` 破坏数据。

### 三级读取路径：本地 memory -> 本地 disk -> 远端 peer

```text
local node wants block_id
  |
  | distributed.Store.OpenBlock(block_id)
  v
local storage.Handler.OpenBlock()
  |
  | memory miss
  v
local DiskTier.OpenBlock()
  |
  | disk hit: promote to memory if possible, otherwise stream from disk
  |
  | disk miss
  v
control plane GetBlockLocations(block_id)
  |
  | returns node_id, addr, tier, meta
  v
network.Client.FetchBlockTo(addr, block_id, ImportBlockWriter)
  |
  | TCP GetBlock
  v
remote node P2P Server
  |
  | remote Store.OpenBlock(block_id)
  v
remote storage.Handler.OpenBlock()
  |
  | stream payload back
  v
local ImportBlockWriter
  |
  | verify length/checksum
  v
local storage.Handler.importBlock()
  |
  | publish into local offheap index
  v
local control plane AnnounceBlock(tier=MEMORY)
  |
  | if disk tier is enabled, mirror to .kvblk
  v
local control plane AnnounceBlock(tier=DISK)
```

回填后，本地节点也成为该 block 的一个副本；如果开启了 disk tier，这个副本会同时存在于本地 memory 和本地 disk。

### membership 同步路径

```text
node B starts with -join-control-addrs 127.0.0.1:19091
  |
  v
cmd/main.go connectPeerControlPlanes()
  |
  v
node B runMembershipSyncLoop()
  |
  | peer.RegisterNode(node B)
  | peer.SyncMembership(node B local view)
  | peer.GetMembership(include tombstones)
  v
node B local ControlPlaneService.SyncMembership()
```

这个流程让新节点把自己注册到 peer，并把 peer 的 membership 视图合并回本地。

## 控制面和数据面的边界

### 控制面负责

- 节点是否存在。
- 节点是否 alive/down/tombstone。
- 节点 P2P 地址。
- 一致性哈希路由。
- block 当前在哪些节点上。
- block 元数据：length、checksum、seq、generation。

### 数据面负责

- C++ 到 Go 的共享内存交接。
- Go daemon-owned SHM 保存本机 C++ 写入 payload。
- Go offheap pool 保存远端回填、disk promote 和旧客户端兼容路径 payload。
- P2P TCP 传输 payload。
- checksum 校验。
- block reader/lease 生命周期。

## 构建

### Go

```bash
go test ./...
```

如果默认 Go build cache 没有写权限，可以指定临时 cache：

```bash
GOCACHE=/tmp/kvcache-go-build go test ./...
```

### C++

```bash
cmake -S csrc -B csrc/build
cmake --build csrc/build
```

构建成功后会生成：

```text
csrc/build/libkvcache_client.a
csrc/build/kvcache_client
```

## 单机端到端 demo

启动 Go daemon：

```bash
GOCACHE=/tmp/kvcache-go-build go run ./cmd \
  -socket /tmp/kvcache-demo.sock \
  -p2p-addr 127.0.0.1:19090 \
  -control-addr 127.0.0.1:19091 \
  -node-id node-a \
  -node-addr 127.0.0.1:19090 \
  -offheap-bytes 1048576 \
  -shm-name kvcache_demo_daemon \
  -shm-bytes 1048576 \
  -disk-dir /tmp/kvcache-demo-disk
```

另一个终端运行 C++ 客户端：

```bash
csrc/build/kvcache_client put --socket /tmp/kvcache-demo.sock --block-id 1001 --data "hello from c++ demo"
```

成功时会看到类似输出：

```text
已发布块 seq=2 block_id=1001 shm_name=kvcache_demo_daemon offset=0 length=19 checksum=<crc32>
```

这说明 C++ payload 已经写入 Go daemon-owned SHM slot，并且 Go 返回了 ACK。

## 两节点分布式启动方式

node A：

```bash
GOCACHE=/tmp/kvcache-go-build go run ./cmd \
  -socket /tmp/kvcache-a.sock \
  -node-id node-a \
  -p2p-addr 127.0.0.1:19090 \
  -control-addr 127.0.0.1:19091 \
  -node-addr 127.0.0.1:19090 \
  -disk-dir /tmp/kvcache-a-disk
```

node B：

```bash
GOCACHE=/tmp/kvcache-go-build go run ./cmd \
  -socket /tmp/kvcache-b.sock \
  -node-id node-b \
  -p2p-addr 127.0.0.1:19092 \
  -control-addr 127.0.0.1:19093 \
  -node-addr 127.0.0.1:19092 \
  -disk-dir /tmp/kvcache-b-disk \
  -join-control-addrs 127.0.0.1:19091
```

当前代码已经具备 node B 加入 node A 控制面的基础流程。下一步应该重点验证：

1. node A 写入 block。
2. node B 能通过 control plane 查询到 block location。
3. node B 本地 miss 后能从 node A P2P 拉取 block。
4. node B 回填成功后能向控制面 announce 自己的新副本。

## 当前实现状态

已经实现：

- C++ shared memory allocator。
- C++ UDS client。
- Go UDS server。
- C++/Go UDS frame 协议。
- daemon-owned POSIX shared memory pool。
- Go mmap shared memory。
- Go offheap memory pool。
- 本地 disk tier，可通过 `-disk-dir` 接入主链路。
- 本地 block index。
- control-plane protobuf/gRPC。
- membership。
- consistent hash router。
- block location state machine。
- P2P TCP server/client。
- distributed store 的三级读取路径：本地 memory -> 本地 disk -> 远端 peer。
- 写入和远端回填后的 disk mirror，以及 memory/disk tier announce。
- 单机 C++ demo 到 Go daemon ACK 的端到端验证。

仍需继续验证或完善：

- 多节点真实端到端读回填。
- C++ 侧还没有 `GetBlock` demo 或 API，目前 demo 只有 `PutBlock`。
- control plane 的 location WAL 接口存在抽象，但当前启动路径没有挂持久化 store。
- membership 心跳和故障检测还比较初级，当前主要是注册和同步。
- eviction 后应同步 `ForgetBlock()` 的策略还需要继续接入。

## 设计直觉

这个项目可以类比为一个很小的 KV Cache 版 buffer manager：

- Go daemon-owned shared memory 是本机跨语言写入和本地缓存池。
- Go offheap pool 是远端回填、disk promote 和旧客户端兼容路径的本地缓存池。
- control plane 是“目录服务”，记录哪个 block 在哪里。
- P2P TCP 是真正搬运大块 payload 的数据通道。
- consistent hash 是未来做自动分片和副本选择的基础。

和 Redis 或数据库 buffer pool 类似，真正的数据读写路径要尽量短；而 membership、路由、位置索引这些信息应该留在控制面，避免大 payload 被复杂协议拖慢。
