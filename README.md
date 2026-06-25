# ZeroKV

ZeroKV 是一个面向大模型推理场景的 KV Cache 数据面 / 控制面原型。它的目标是让 C++ 推理侧生成的大块 KV tensor payload 直接进入 Go daemon 的本地存储层，并在多节点之间通过控制面元数据和 P2P 数据面完成查找、拉取、回填和副本公告。

当前主链路已经从旧的 UDS + POSIX shared memory 写入方式切换为：

```text
C++ compute KV tensor
        |
        v
RDMA write endpoint / P2P TCP fallback
        |
        v
Go distributed.Store.IngestBlock()
        |
        v
storage.Handler / memory tier / optional disk tier
        |
        v
CommitCacheObject(ObjectID)
        |
        v
Prefix Lookup 可见 + control plane announce location
```

当 RDMA 写入入口不可用时，C++ connector 可以自动降级到 P2P TCP put-block 写入路径。跨节点读取仍然使用 P2P TCP fetch 路径。`integration/vllm` 还提供了一个面向 vLLM 0.14.0 V1 API 的实验性外部 KV connector，用于在真实 GPU 上验证 prompt prefix KV Cache 的正确性和收益。

## 当前实现了什么

已经实现：

- C++ `KVCacheConnector`：上层推理引擎只需要调用 `Connect()` / `PutBlock()` / `Close()`。
- KVCacheKey：根据 namespace、模型版本、adapter、KV layout 和链式 token prefix hash 确定缓存身份。
- 256-bit ObjectID：作为语义缓存对象的权威身份；原有 uint64 BlockID 仅作为物理存储投影。
- 两阶段发布：先写入并确认 payload，再通过 `CommitCacheObject` 发布语义对象，避免查询看到半写状态。
- 批量 Prefix Lookup：一次请求全部候选块，一次返回最长连续命中、来源位置和短租约，不返回 KV payload。
- Prefix Lease：命中块在加载期间禁止删除或 spill，支持主动释放和 TTL 自动释放。
- 持久化语义 manifest：`.kvmeta` 与 `.kvblk` 一起恢复，daemon 重启后可重建 ObjectID 索引。
- C++ 文本 demo：先批量查询最长连续 KV prefix，只为 miss 后的完整 token chunk 生成并写入模拟 KV tensor。
- RDMA 数据面入口：Go 侧新增 `internal/rdma.Server`，接收 C++ 推来的 block payload。
- P2P 写入降级：RDMA 写入失败时，C++ 可通过 P2P TCP put-block 协议推送 payload。
- P2P 跨节点读取：本地 miss 后可以从远端 peer 拉取 block payload 并回填本地。
- Go 本地 offheap memory tier：用 mmap 匿名内存池保存 block。
- Go disk tier：可选本地磁盘层，用于 spill、恢复和降级存储。
- distributed store：组合本地 memory、disk、control plane、P2P fetch 和本地写入公告。
- control plane：gRPC + protobuf，支持 membership、block route、announce、forget、location 查询。
- membership sync：节点之间同步拓扑和存活状态。
- compute-to-data 骨架：可以把小计算请求发到已有 block 的远端节点执行，失败时可回退到 fetch-local。
- vLLM connector：实现 scheduler 侧 prefix 命中判定和 worker 侧 KV page 加载/保存，缓存异常时交给 vLLM 重新计算。

需要明确：

- 当前 `internal/rdma` 是 RDMA 数据面边界和兼容 listener，实现了 C++ -> Go 的直接 payload 写入协议。真正基于 rdma-core / ibverbs 的 MR/QP/RDMA Write 后端还需要在这个模块边界内继续替换。
- UDS 不再是主写入链路，daemon 默认不启动 UDS server，C++ 主构建也不再依赖 `uds_client.cc` / `shm_allocator.cc`。
- 旧 UDS、shared memory 相关文件仍保留在仓库里，主要用于历史参考或后续兼容，不属于当前默认运行路径。

## 系统模型

ZeroKV 把 KV Cache 拆成两类信息：

- 数据面 payload：真正的大块 KV tensor bytes，不走 protobuf，不走 gRPC。本机写入走 RDMA endpoint，RDMA 不可用时走 P2P TCP fallback。跨节点读取走 P2P TCP fetch。
- 控制面 metadata：block id、length、checksum、node id、node addr、storage tier、membership、route version 等小元数据，通过 gRPC/protobuf 管理。

这样拆分的原因是 KV Cache block 通常很大。如果 payload 走 gRPC/protobuf，会引入额外拷贝、序列化和调度开销。ZeroKV 采用的是：

```text
payload: raw bytes data plane
metadata: structured control plane
```

### KV Cache 语义边界

ZeroKV 把 KV Cache 的语义身份和物理存储位置分开：

```text
model + revision + adapter + layout + token prefix
                         |
                         v
                    KVCacheKey
                         |
                         v
               256-bit ObjectID
                         |
                         +------> semantic index / Prefix Lookup
                         |
                         v
            uint64 BlockID projection
                         |
                         v
        storage / transport / physical lookup
```

- `pkg/kvcachekey` 是 Go 侧规范实现，提供 canonical SHA-256 编码、链式 prefix hash、不可变绑定和最长连续前缀查询。
- `csrc/cache_key.h` / `csrc/cache_key.cc` 是 C++ 同构实现，并用固定 golden vector 保证和 Go 逐字节一致。
- `integration/vllm/zerokv_vllm/key.py` 是 Python 同构实现，使用同一个 golden vector 校验三种语言的结果。
- ObjectID 由 scope digest、链式 prefix digest 和 token 边界共同决定。scope 包含 schema、namespace、模型 ID/revision、adapter、chunk size，以及 layout version、dtype、层数、KV heads、head dimension、TP world/rank。
- prefix digest 是链式的；任意较早 token 改变都会让该点及之后的 ObjectID 全部改变。
- semantic index 使用完整 256-bit ObjectID 做不可变绑定；相同 ObjectID 不能被重新绑定到不一致的 BlockID、长度或 checksum。
- `Digest.BlockID()` 只生成 ObjectID 的确定性 64-bit 物理投影。控制面筛选远端副本时仍必须比较完整 ObjectID，避免 64-bit 碰撞造成错误命中。
- placement、replica、memory/disk tier、checksum 和 TCP/RDMA 不属于 KVCacheKey；移动数据不会改变缓存身份。
- 只为完整且 block-aligned 的 prompt token chunk 建 key 和缓存对象；不足一块的尾部由模型计算但不进入共享缓存。

## 顶层目录

```text
.
├── cmd/                    Go daemon 启动入口
├── csrc/                   C++ connector、CLI demo、历史 UDS/SHM 代码
├── integration/vllm/       vLLM 0.14.0 外部 KV connector 与 Python 协议客户端
├── internal/               Go 内部模块：RDMA、P2P、存储、控制面、分布式 store
├── pkg/                    Go 公共包：KVCacheKey、logger、历史 wire codec
├── proto/                  控制面 protobuf 定义和生成代码
├── go.mod / go.sum         Go module 依赖
└── README.md               项目说明文档
```

## 语义 KV Cache 实现索引

| 文件 | 已实现内容 |
| --- | --- |
| `pkg/kvcachekey/key.go` | canonical key、完整 ObjectID、`Digest.BlockID()` 物理投影 |
| `pkg/kvcachekey/index.go` | `LookupObject`、`LookupBlock`、`ForgetBlock`、不可变绑定和索引移除 |
| `internal/network/prefix.go` | Commit、批量 Prefix Lookup、结果解码、lease release wire codec/client/server 与 store interfaces |
| `internal/distributed/prefix.go` | 两阶段语义提交、连续前缀选择、远端副本筛选、pin/lease 生命周期 |
| `internal/storage/cache_manifest.go` | `.kvmeta` 编解码、CRC、原子持久化、启动恢复 |
| `proto/control_plane.proto` | `BlockMeta.object_id` 可选 32-byte 字段 |
| `proto/controlplane/*.pb.go` | 由新控制面 schema 重新生成的 Go protobuf/gRPC binding |
| `internal/coordinator/` | ObjectID 校验、状态保存和 gRPC 转换 |
| `csrc/cache_key.*` | 与 Go 一致的 C++ CacheKey/ObjectID 实现 |
| `csrc/connector.*` | `PutCacheObject`、`LookupPrefix`、`LoadPrefixEntry`、`ReleasePrefixLease` |
| `csrc/client/kvcache_client.cc` | 完整块切分、一次 prefix 查询、命中加载、suffix 计算与提交 |
| `integration/vllm/zerokv_vllm/key.py` | Python CacheKey/ObjectID 实现 |
| `integration/vllm/zerokv_vllm/protocol.py` | 纯 Python framed TCP Get/Put/Commit/Lookup/Release 客户端 |
| `integration/vllm/zerokv_vllm/connector.py` | vLLM V1 scheduler/worker connector |
| `internal/network/prefix_test.go` | codec 和真实 loopback Client→Server 协议测试 |
| `internal/distributed/store_test.go` | 连续命中、租约、重启恢复、完整 ObjectID 远端筛选测试 |
| `integration/vllm/tests/test_key.py` | Python/C++/Go golden vector 和 partial-tail 行为测试 |

## 核心组件

### Go Daemon

#### `cmd/main.go`

ZeroKV daemon 的启动入口，负责组装所有模块。

主要职责：

- 解析启动参数。
- 创建本地 `storage.OffheapPool` 和 `storage.Handler`。
- 可选创建 `storage.DiskTier`。
- 创建 membership、router、control plane service。
- 连接 peer control planes。
- 创建 `distributed.Store`。
- 启动 RDMA 写入 server。
- 启动 P2P TCP server。
- 启动 control-plane gRPC server。
- 启动 membership sync loop。
- 处理 SIGINT / SIGTERM / SIGQUIT 并优雅退出。

核心启动参数：

```text
-rdma-addr                  C++ 本机写入入口，默认 :19100
-rdma-max-conns             RDMA 写入入口最大并发连接数
-p2p-addr                   P2P TCP 服务监听地址，默认 :19090
-p2p-max-conns              P2P 最大并发连接数
-control-addr               控制面 gRPC 监听地址，默认 :19091
-node-id                    当前节点 ID
-node-addr                  对外公告的 P2P 地址；默认等于 -p2p-addr
-join-control-addrs         要加入的 peer control-plane 地址列表
-offheap-bytes              本地 offheap memory pool 大小
-disk-dir                   本地磁盘层目录；为空则禁用 disk tier
-memory-high-bytes          memory tier 高水位；需要 -disk-dir
-memory-low-bytes           spill 后目标低水位
-prefix-lease-ttl           Prefix Lookup 命中批次的最大 pin 时间（默认 5s）
-membership-sync-interval   membership 同步周期
-shutdown-timeout           优雅退出超时
```

### C++ Connector

#### `csrc/connector.h`

C++ 推理侧使用的门面接口。

主要类型：

- `KVCacheConnectorOptions`
  - `rdma_addr`：RDMA 写入入口，默认 `127.0.0.1:19100`。
  - `p2p_fallback_addr`：P2P 降级写入入口，默认 `127.0.0.1:19090`。
  - `enable_p2p_fallback`：RDMA 失败后是否自动降级。
  - `wait_for_ack`：写入后是否等待 Go daemon ACK。
- `KVCacheBlockMeta`
  - `seq`
  - `block_id`
  - `transport`
  - `length`
  - `checksum`
- `KVCacheConnector`
  - `Connect()`
  - `PutBlock()`
  - `PutCacheObject()`：物理写入 ACK 后提交 256-bit 语义对象。
  - `LookupPrefix()`：一次查询最长连续前缀，只返回元数据和位置。
  - `LoadPrefixEntry()`：按查询选定的位置加载并校验 KV 字节。
  - `ReleasePrefixLease()`：加载结束后释放批次短租约。
  - `Close()`
  - `connected()`

其中 `PutBlock()` 是物理调试/兼容接口，单独调用不会让 block 出现在 Prefix Lookup 中。正式语义缓存写入应使用 `PutCacheObject()`。

Prefix 接口还定义了 `PrefixStopReason`、`PrefixLocation` 和 `PrefixLookupResult`。`LookupPrefix()` 会验证返回的 entry 与原请求 ObjectID/token boundary 对应；`LoadPrefixEntry()` 按选定 address 拉取 payload，并再次核对 length/checksum。

#### `csrc/connector.cc`

C++ 数据面实现。

`PutBlock()` 流程：

1. 检查 block id、data pointer、length。
2. 生成单调递增 `seq`。
3. 计算 payload CRC32。
4. 优先连接 `rdma_addr` 并发送 RDMA put-block frame。
5. 如果 RDMA 失败且开启 fallback，连接 `p2p_fallback_addr` 并发送 P2P put-block frame。
6. 默认等待 Go daemon 返回 ACK 或 ERROR。
7. 回填 `KVCacheBlockMeta`。

`PutCacheObject()` 在上述步骤之后继续执行语义提交：

1. 要求 `wait_for_ack=true`，确保 daemon 已完整接收并校验 payload。
2. 向 P2P TCP 入口发送 scope digest、prefix digest、ObjectID、token count、BlockID、length 和 checksum。
3. daemon 重新计算 ObjectID 和 BlockID，并核对已存在物理块的 length/checksum。
4. 语义 commit 成功后才进入 Prefix Lookup 索引。

如果第 2～4 步失败，已写入的 bytes 只是不可见的 orphan physical block，不会产生错误命中。之后可由后台孤儿回收机制清理；当前版本尚未实现自动回收。

#### `csrc/client/kvcache_client.cc`

简单 CLI demo，用于生成或发送 payload。

支持：

- `put`：发送原始字符串或文件 payload。
- `text`：根据模型与 token prefix 自动生成 key，复用最长连续命中，只计算并写入剩余 KV chunk。
- `interactive`：交互式 demo。

`text`/`interactive` 的 prefix 流程是：构造所有完整 chunk 的 key，一次 `LookupPrefix()`，依次加载命中项，释放 lease，从第一个 miss 或 load failure 开始重新计算，只用 `PutCacheObject()` 发布完整 suffix block。任何缓存异常都按 miss 处理，不影响模型正确性。

常用参数：

```text
--rdma-addr <addr>
--p2p-fallback-addr <addr>
--no-p2p-fallback
--no-ack
--block-id <id>
--namespace <name>
--model-id <id>
--model-revision <revision>
--adapter-id <id>
--data <text>
--file <path>
--text <text>
--text-file <path>
--stdin
--layers <n>
--heads <n>
--head-dim <n>
```

#### `csrc/CMakeLists.txt`

C++ 构建配置。当前主库编译 `cache_key.cc` 和 `connector.cc`，不再把 UDS client 和 SHM allocator 编入默认 connector。

### RDMA Data Plane

#### `internal/rdma/server.go`

C++ -> Go 的本机写入入口。

职责：

- 监听 `-rdma-addr`。
- 接收 put-block frame。
- 校验 magic、version、header size、reserved field。
- 校验 seq、block id、payload length。
- 读取 payload。
- 计算 CRC32 并与 header checksum 对比。
- 调用 `Store.IngestBlock()`。
- 返回 ACK 或 ERROR。

当前 wire header：

```text
0:4    magic = "RDMA"
4:6    version
6:8    header size
8:10   message type
10:12  reserved
12:16  checksum
16:24  seq
24:32  block id
32:40  payload length
```

### Unified Block Transport

#### `internal/transport`

`distributed.Store` 的跨节点 block fetch 通过统一 transport 接口执行：

- `P2PTransport`：适配现有 TCP `FetchBlockTo`，始终作为可用的降级路径。
- `RDMATransport`：只定义云端 RDMA backend 边界，不把 C/C++/Go verbs API 泄漏到 cache manager。
- `FailoverTransport`：RDMA primary 失败后先回滚目标 writer，再使用 P2P，避免部分 payload 被拼接提交。

云端接入时实现 `transport.RDMABackend`，再通过
`StoreOptions.PrimaryTransport: transport.NewRDMA(backend)` 注入。未注入 RDMA
时，`Store` 默认保持 P2P-only 行为。

这里的 transport 负责完整 KV block 的搬运；`ComputeBlockNearData()` 仍是独立的
计算放置策略，当前远端 compute 继续使用 P2P 小请求协议。

### P2P Data Plane

#### `internal/network/zerocopy_tcp.go`

P2P TCP server 和协议实现。

支持数据面动作和小型 Prefix Lookup 控制消息：

- `GetBlock`：远端节点拉取本地 block。
- `ComputeBlock`：远端节点请求在本地 block 上执行内置小算子。
- `PutBlock`：本机 C++ connector 的 P2P 降级写入路径。
- `CommitCacheObject`：把完整语义 key 原子发布到可查询索引。
- `LookupPrefix`：批量返回最长连续命中、确定性来源和短租约，不返回 KV payload。
- `ReleasePrefixLease`：显式解除命中批次的删除/搬运保护。

现有 framed TCP header 保持 32 bytes、magic `0x4e50564b`、version `1`。原 message type `1..8` 不变，新增：

| Message type | 值 | 用途 |
| --- | ---: | --- |
| `CommitCacheObject` | 9 | 发布一个已完成物理写入的语义对象 |
| `LookupPrefix` | 10 | 批量查询有序候选 prefix |
| `PrefixLookupResult` | 11 | 返回最长连续命中、位置和 lease |
| `ReleasePrefixLease` | 12 | 提前释放整个命中批次 |

因为旧 message type 和 frame header 没有改变，不使用新语义接口的旧客户端仍可继续走原物理块协议。

Prefix metadata payload 最大 2 MiB，单次最多 8192 个候选。该限制独立于大 block payload 限制。

Commit payload 固定 128 bytes：

```text
0:2      prefix protocol version = 1
8:16     token_count
16:24    payload length
24:28    payload CRC32
32:64    scope_digest[32]
64:96    prefix_digest[32]
96:128   object_id[32]
BlockID  放在外层 32-byte TCP frame header
```

Lookup request 由 40-byte header 加若干 40-byte candidate 组成：

```text
request header:
  version + candidate_count + scope_digest[32]

candidate[i]:
  object_id[32] + token_end[8]
```

Lookup result 由 40-byte header、每项 68-byte 固定 entry 和变长位置字符串组成：

```text
result header:
  version + stop_reason + entry_count + matched_tokens
  + lease_id + expires_unix_nano

entry[i]:
  object_id[32] + block_id + token_end + length + checksum
  + tier + transport + node_id_length + address_length
  + node_id bytes + address bytes
```

返回值只有元数据和确定性数据来源，不携带 KV payload。`stop_reason` 为 `full_match`、`not_found`、`unavailable` 或 `busy`；调用方从第一处未命中开始重算。

P2P put-block payload 布局：

```text
header:
  block_id
  payload_len = 8 + data_len
  checksum = crc32(data)

payload:
  0:8       seq
  8:N       raw block bytes
```

#### `internal/network/p2p_client.go`

P2P TCP client。

主要能力：

- `FetchBlockTo()`：远端拉取 block，并写入 caller 提供的 writer。
- `ComputeBlock()`：向远端 holder 发送 compute-to-data 请求。

同一个 `network.Client` 的语义控制方法实现在 `internal/network/prefix.go`：

- `CommitCacheObject()`：提交完整 ObjectID 语义。
- `LookupPrefix()`：一次收发全部候选和命中结果。
- `ReleasePrefixLease()`：主动释放命中批次 pin。

### Local Storage

#### `internal/storage/offheap_pool.go`

基于 mmap anonymous memory 的 offheap bump-pointer pool。它避开 Go GC 管理大块 KV payload。

#### `internal/storage/handler.go`

本地 block 存储入口。

职责：

- 管理 block index。
- 防止重复加载和并发冲突。
- 校验 checksum。
- 把 RDMA/P2P 收到的 payload 导入 offheap pool。
- 提供 zero-copy lease reader 给 P2P server 和 disk tier。
- 提供 `ImportBlock()` 给 `distributed.Store.IngestBlock()` 使用。
- 提供 `ImportBlockWriter` 给远端 fetch 回填使用。

#### `internal/storage/disk_tier.go`

本地磁盘层。

职责：

- block 落盘。
- daemon 启动时扫描已有 `.kvblk` 文件。
- memory spill 后保存冷数据。
- disk promote 到 memory。

#### `internal/storage/cache_manifest.go`

每个已 commit 的语义 cache object 都可以在 disk tier 保存一个 `.kvmeta` manifest。manifest 固定 140 bytes，包含：

- magic `ZKVM`、version `1` 和 header size；
- BlockID、token count、payload length、payload checksum；
- scope digest、prefix digest、完整 ObjectID；
- manifest header 的 CRC32。

写入采用临时文件、file sync、rename 和 directory sync，避免断电时暴露半个 manifest。启动扫描时只有 `.kvmeta` CRC 正确、ObjectID 可重算、且对应 `.kvblk` 的 length/checksum 一致，才会恢复进语义索引；损坏或不匹配文件会被忽略并计入 invalid files。删除 block 时会同步删除 manifest。

当前一个语义对象对应一个物理 block。多 shard、TP rank 聚合或多个 KV group 所需的通用 manifest 还未实现。

#### `internal/storage/evict.go`

memory tier eviction / compaction 相关策略。

#### `internal/storage/shared_memory_pool.go`

历史 daemon-owned shared memory pool。当前 RDMA 主链路不依赖它。

### Distributed Store

#### `internal/distributed/store.go`

把本地存储、控制面、P2P client、disk tier 和 memory LRU 组合成一个统一的分布式 KV cache store。

主要能力：

- `IngestBlock()`：RDMA/P2P 写入入口调用，提交本地 block 并公告控制面。
- `HandleBlockReady()`：历史 UDS/SHM 路径保留接口。
- `OpenBlock()`：本地读入口，按 memory -> disk -> remote peer 顺序查找。
- `ComputeBlockNearData()`：由 runtime 选择远端计算或 fetch-local；daemon 默认使用 fetch-local。
- `AnnounceDiskBlocks()`：启动时把 disk tier 中恢复出的 block 公告到控制面；已恢复语义 manifest 的位置会带完整 ObjectID。
- memory high/low watermark 控制。
- cold block spill 到 disk。
- 远端 fetch 后本地回填并公告新副本。

#### `internal/distributed/prefix.go`

负责语义 cache object 的生命周期：

- `CommitCacheObject()`：重新计算 ObjectID/BlockID，核对物理 length/checksum，持久化 manifest，绑定不可变语义索引，并重新公告带 ObjectID 的 memory/disk 位置。
- `LookupPrefix()`：从第一个候选开始顺序处理，遇到第一处 missing、unavailable 或 busy 立即停止，只返回连续前缀。
- 本地选择顺序为 semantic memory -> semantic disk；本地没有时查询控制面。
- 远端位置必须与完整 256-bit ObjectID 一致。候选按 memory < disk < unknown，再按 node ID、address、version 确定性排序，只选一个来源，不广播读取。
- 选定远端副本后先 fetch/refill 到本地 daemon，再向调用方返回本地地址，使后续 worker load 使用统一 GetBlock 路径。
- 命中前先 pin，再检查和读取，避免查询与 delete/spill 竞态；只要命中至少一个 block 就发布一个批次 lease。
- 显式 `ReleasePrefixLease()` 会立即 unpin；未释放时由 `-prefix-lease-ttl` 对应的定时器兜底。

被 pin 的 block 执行 `DeleteBlock()` 会返回 busy，LRU spill 会跳过。正在 WRITING 的 block 在 commit 前不可见；处于删除/搬运 mutation 的 block 会让 prefix 查询在该点以 busy/miss 停止。

### Control Plane

#### `proto/control_plane.proto`

控制面 protobuf 定义。

核心对象：

- `Node`
- `BlockMeta`
- `BlockLocation`
- `RegisterNodeRequest`
- `HeartbeatRequest`
- `RouteBlockRequest`
- `AnnounceBlockRequest`
- `ForgetBlockRequest`
- `GetBlockLocationsRequest`

核心 RPC：

- `RegisterNode`
- `Heartbeat`
- `LeaveNode`
- `GetMembership`
- `SyncMembership`
- `RouteBlock`
- `AnnounceBlock`
- `ForgetBlock`
- `GetBlockLocations`

`BlockMeta` 新增可选 `bytes object_id = 6`。旧物理写入可以不带该字段，只有 commit 后的语义对象才公告 32-byte ObjectID。gRPC API 只接受空值或严格 32 bytes；Prefix Lookup 选择 cluster replica 时必须匹配该完整值。

#### `internal/coordinator/`

控制面实现。

主要文件：

- `membership.go`：节点 membership 状态。
- `router.go`：block -> node 路由。
- `state_machine.go`：block location 状态机。
- `grpc_api.go`：gRPC API 实现。
- `control_plane.go`：control plane service 聚合。
- `radix_tree.go`：block location 索引结构。
- `consistent_hash.go`：历史 consistent hash 实现。

### Logger

#### `pkg/logger/logger.go`

统一日志封装。

## 数据流

### 语义写入：payload ACK -> cache object commit

```text
runtime 计算完整 KV block
  |
  v
PutBlock(BlockID, bytes) -> daemon 校验并 ACK
  |
  | bytes 已安全存在，但 Prefix Lookup 不可见
  v
CommitCacheObject(scope/prefix/ObjectID/length/checksum)
  |
  v
重算 key + 核对物理块 + 写 .kvmeta + bind semantic index
  |
  v
Prefix Lookup 可见 + announce ObjectID location
```

这条两阶段边界保证“metadata 可见”不会早于“payload 可读”。commit 失败只留下不可见物理块，不会把错误 KV 返回给模型。

### Prompt Prefix 读取：一次查询，一批返回

```text
prompt tokens
  |
  v
只构造完整 chunk 的有序 ObjectID 列表
  |
  v
一次 LookupPrefix([object0, object1, ...])
  |
  v
最长连续 locations + matched_tokens + batch lease
  |
  v
逐块 Get/校验/装入 runtime KV pages
  |
  +---- 全部成功 -> ReleasePrefixLease
  |
  +---- miss/load error -> ReleasePrefixLease，从边界开始重算
```

它不是“把所有 KV bytes 一次返回”。优化的是查询往返：以前每个 block 单独判断一次命中，现在一次返回整个连续前缀的元数据；真正的 KV payload 仍按选定位置逐块加载。

### 本机写入：C++ -> RDMA -> Go

```text
C++ model runtime / demo
  |
  | KVCacheConnector::PutBlock(block_id, data, length)
  v
RDMA put-block frame
  |
  v
internal/rdma.Server
  |
  | validate header / length / checksum
  v
distributed.Store.IngestBlock()
  |
  v
storage.Handler.ImportBlock()
  |
  v
offheap memory index
  |
  v
control plane AnnounceBlock(memory)
```

如果开启 disk tier，写入后还会 mirror 到 disk，并公告 disk tier location。

### 本机写入降级：C++ -> P2P TCP -> Go

```text
RDMA connect/send failed
  |
  v
KVCacheConnector fallback
  |
  v
P2P put-block frame
  |
  v
network.Server.handlePutBlock()
  |
  v
distributed.Store.IngestBlock()
```

这条路径用于 RDMA 硬件、驱动、verbs 后端不可用时保证功能可用。

### 跨节点读取：local miss -> remote fetch -> local refill

```text
OpenBlock(block_id)
  |
  | memory miss
  v
disk tier lookup
  |
  | disk miss
  v
control plane GetBlockLocations(block_id)
  |
  v
P2P FetchBlockTo(remote_addr, block_id, ImportBlockWriter)
  |
  v
checksum verify + commit local offheap
  |
  v
AnnounceBlock(memory)
```

### Compute Near Data

```text
ComputeBlockNearData(request)
  |
  | local block exists
  v
compute locally

or

query block locations
  |
  v
try remote holder ComputeBlock()
  |
  | remote failed / overloaded / policy fallback
  v
fetch block locally and compute
```

## 构建

### Go

```bash
go build ./cmd
```

如果默认 Go build cache 没有写权限，可以指定临时 cache：

```bash
GOCACHE=/tmp/kvcache-go-build go build ./cmd
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

### Python / vLLM

使用 Python 3.10+ 环境，并先安装与 GPU 镜像匹配的 CUDA/PyTorch：

```bash
python -m pip install -e ./integration/vllm
```

该 package 固定依赖 `vllm==0.14.0`，目的是让首轮验证对应一个明确的 runtime API 和 tensor layout，不宣称兼容其他版本。

## 本地测试

运行完整的本地正确性检查、race detector 和 C++ 构建：

```bash
make test-local
```

该命令覆盖：

- storage handler 的 block 导入、checksum、lease、删除和物理压缩。
- disk tier 的落盘、进程重启后索引恢复和 payload 损坏检测。
- control plane 的 announce、location query 和 forget 生命周期。
- Go/C++ KVCacheKey golden vector、模型/layout 隔离、链式 prefix 和不可变绑定。
- Prefix wire codec 与真实 loopback TCP 的 Commit→Lookup→Release。
- Prefix 连续命中、lease 阻止删除、主动释放、TTL 边界和 disk manifest 重启恢复。
- 远端存在相同 64-bit BlockID 的错误/正确两个位置时，只选择完整 256-bit ObjectID 正确的位置。
- RDMA primary 成功、部分写入失败、错误 block ID 和 P2P fallback。
- distributed store 的远程 fetch、本地回填、只拉取一次和并发 miss singleflight。
- loopback TCP P2P 的真实协议收发。
- 两个真实 daemon 进程与 C++ CLI 的 KV prefix 复用、RDMA 兼容写入、P2P 写入降级、跨节点回填，以及源节点下线后的本地命中。
- Go race detector、C++ connector/CLI 编译和 C++ KVCacheKey 测试。

本轮还单独完成了 Python 语法检查，以及 Python/Go/C++ KVCacheKey golden vector 和 partial tail 测试；这些目前不包含在 `make test-local` 中。

本地 C++ smoke 已验证相同 32-byte 文本的两次执行：第一次 `hit_blocks=0 written_blocks=2`，第二次 `hit_blocks=2 written_blocks=0`。这证明语义提交和批量命中链路可用，但本地输出中的 `transport=rdma` 只是当前兼容 listener，不等价于真实 RDMA 硬件验证。

只运行进程级双节点验证：

```bash
make integration
```

运行本机性能基线：

```bash
make bench
```

查看核心包 statement coverage：

```bash
make coverage
```

benchmark 分成三类：

- `HandlerLeaseAcquire`：获取和释放一个零拷贝 block lease 的开销。
- `HandlerStreamRead`：完整读取 16 KiB / 1 MiB 本地 block 的吞吐。
- `P2PTransportFetch`：通过 loopback TCP 拉取 64 KiB / 1 MiB block 的端到端延迟、吞吐和 allocations。

这些结果只用于同一台机器上的版本回归。真实 RDMA 上云后必须使用相同 block
大小重新测量，并与 P2P 的 p50/p99、吞吐、CPU 和 allocations 对比。

测试指标设计参考了
[LMCache storage backend benchmarks](https://github.com/LMCache/LMCache/tree/dev/benchmarks/storage_backend_io)
和 [Mooncake Store tests](https://github.com/kvcache-ai/Mooncake/tree/main/mooncake-store/tests)，
但测试实现针对 ZeroKV 自己的协议、存储和控制面，没有引入它们的运行时依赖。

## 单机 Demo

启动 Go daemon：

```bash
GOCACHE=/tmp/kvcache-go-build go run ./cmd \
  -rdma-addr 127.0.0.1:19100 \
  -p2p-addr 127.0.0.1:19090 \
  -control-addr 127.0.0.1:19091 \
  -node-id node-a \
  -node-addr 127.0.0.1:19090 \
  -offheap-bytes 1048576 \
  -disk-dir /tmp/zerokv-demo-disk
```

另一个终端运行 C++ 客户端：

```bash
csrc/build/kvcache_client put \
  --rdma-addr 127.0.0.1:19100 \
  --p2p-fallback-addr 127.0.0.1:19090 \
  --block-id 1001 \
  --data "hello from c++ demo"
```

成功时会看到类似输出：

```text
已发布块 seq=1 block_id=1001 transport=rdma length=19 checksum=<crc32>
```

如果 RDMA 写入入口不可用但 P2P 入口可用，并且没有指定 `--no-p2p-fallback`，可能看到：

```text
已发布块 seq=1 block_id=1001 transport=p2p_fallback length=19 checksum=<crc32>
```

文本生成 KV payload：

```bash
csrc/build/kvcache_client text \
  --rdma-addr 127.0.0.1:19100 \
  --p2p-fallback-addr 127.0.0.1:19090 \
  --model-id zerokv-demo \
  --model-revision v1 \
  --text "ZeroKV stores KV cache blocks" \
  --layers 1 \
  --heads 8 \
  --head-dim 16
```

连续运行两次相同 `text` 请求时，第一轮应写入所有完整块，第二轮应命中相同的最长连续 prefix。改变模型 revision、adapter、layout 或任一较早 token 后不应复用原对象。

## vLLM 0.14.0 接入

`integration/vllm` 是不修改 vLLM 源码的动态 `KVConnectorBase_V1` 实现。首版目的不是覆盖所有部署形态，而是先在一张 GPU 上证明：外部 prompt KV Cache 能正确复用，并且 TTFT 收益大于同步 TCP/CPU copy 成本。

### 当前支持边界

- 一张 GPU，TP=1、PP=1；
- 恰好一个标准 `FullAttentionSpec` KV group；
- K/V-first paged tensor，K/V head dimension 相同；
- 一个 ZeroKV object 按 `layer_names` 顺序串联所有 attention layer 的一个完整 token page；
- scheduler 侧批量 Prefix Lookup，worker 侧同步 load/save；
- `cache_salt` 与 LoRA identity 被编码进 adapter scope；
- cache lookup/load/save 失败时按 miss 处理，由 `kv_load_failure_policy=recompute` 保证正确输出；
- 首轮走 Python TCP，尚未接 native C++/硬件 RDMA。

当前不支持 MLA、Mamba、hybrid KV groups、多模态 prompt embedding、TP>1、PP>1 和未固定版本的 vLLM。必须使用 `--disable-hybrid-kv-cache-manager`。

### Scheduler 与 worker 行为

Scheduler 只把完整 block、且为 vLLM 保留至少一个实际执行 prompt token 之前的部分作为 external hit。相同 request 的 side-effect-free 查询会复用 pending lookup，避免重复获取 lease；block allocation 完成后把 ObjectID 与 vLLM block ID 的映射传给 worker，请求结束时清理状态和 lease。

Worker 注册各层 K/V-first paged tensors。load 时从 Prefix Lookup 选定位置读取一个全层对象，检查 length/checksum，按 layer page 切分并复制到 device；失败的 block ID 通过 `get_block_ids_with_load_errors()` 报给 vLLM。save 时先把每层完整 page 拷到 CPU，在 `wait_for_save()` 聚合全层对象并执行 Put+Commit。

### 启动方式

先启动本地 daemon。下面给 16 GiB memory tier，并设置 NVMe spill 水位和 30 秒 lookup lease：

```bash
go build -o /tmp/zerokv-daemon ./cmd
/tmp/zerokv-daemon \
  -node-id gpu-0 \
  -node-addr 127.0.0.1:19090 \
  -p2p-addr 127.0.0.1:19090 \
  -rdma-addr 127.0.0.1:19100 \
  -offheap-bytes 17179869184 \
  -disk-dir /local_nvme/zerokv \
  -memory-high-bytes 13743895347 \
  -memory-low-bytes 10995116277 \
  -prefix-lease-ttl 30s
```

再用不可变模型 revision 启动 vLLM；vLLM 和 connector 必须使用同一个 revision：

```bash
vllm serve Qwen/Qwen2.5-7B-Instruct \
  --revision MODEL_COMMIT_SHA \
  --disable-hybrid-kv-cache-manager \
  --kv-transfer-config '{
    "kv_connector":"ZeroKVConnector",
    "kv_connector_module_path":"zerokv_vllm.connector",
    "kv_role":"kv_both",
    "kv_load_failure_policy":"recompute",
    "kv_connector_extra_config":{
      "zerokv_address":"127.0.0.1:19090",
      "namespace":"benchmark",
      "model_revision":"MODEL_COMMIT_SHA",
      "layout_version":1400,
      "timeout_seconds":30
    }
  }'
```

`layout_version=1400` 是此 pinned adapter 的显式 byte-layout 边界。只要 tensor serialization/layout 改变，就必须增加该值，从语义上隔离旧缓存。更短的安装说明也保留在 `integration/vllm/README.md`。

## 两节点启动示例

node A：

```bash
GOCACHE=/tmp/kvcache-go-build go run ./cmd \
  -node-id node-a \
  -rdma-addr 127.0.0.1:19100 \
  -p2p-addr 127.0.0.1:19090 \
  -control-addr 127.0.0.1:19091 \
  -node-addr 127.0.0.1:19090 \
  -disk-dir /tmp/zerokv-a-disk
```

node B：

```bash
GOCACHE=/tmp/kvcache-go-build go run ./cmd \
  -node-id node-b \
  -rdma-addr 127.0.0.1:19102 \
  -p2p-addr 127.0.0.1:19092 \
  -control-addr 127.0.0.1:19093 \
  -node-addr 127.0.0.1:19092 \
  -disk-dir /tmp/zerokv-b-disk \
  -join-control-addrs 127.0.0.1:19091
```

写入 node A：

```bash
csrc/build/kvcache_client put \
  --rdma-addr 127.0.0.1:19100 \
  --p2p-fallback-addr 127.0.0.1:19090 \
  --block-id 3001 \
  --data "block on node-a"
```

预期能力：

1. node A 本地存储 block。
2. node A 向 control plane announce block location。
3. node B 可以通过 control plane 查询 block location。
4. node B 本地 miss 后通过 P2P 从 node A fetch。
5. node B 回填本地后 announce 自己的新副本。

## GPU 云服务器方案与验收门槛

项目下一步先验证单机单卡 TCP 基线，再决定是否投入 RDMA。建议第一阶段租用一台带 48 GiB 显存 GPU、16 vCPU、128 GiB 内存和 500 GiB～1 TiB 本地 NVMe 的实例；阿里云可优先查找 L20 48 GiB 对应的 `ecs.gn8is.4xlarge` 或同等级可用规格。实例名称和库存会随地域变化，购买前应在目标地域控制台复核。

单卡必须通过以下正确性门槛：

1. 冷请求 external hit token 为 0；相同长 prefix 的第二次请求只命中允许范围内最长的完整块，同时至少保留一个 prompt token 给 vLLM 执行。
2. 改变 model revision、LoRA/cache salt、dtype/layout version 或任何更早 token，必须在对应边界 miss；不接受任何 false hit。
3. lookup/load 前关闭 daemon，输出仍正确，vLLM 对失败 block 重算。
4. 触发 memory watermark spill 后仍可从 NVMe 命中；重启 daemon 后能从 `.kvmeta` 重建语义索引。

性能矩阵固定为 shared prefix `1K/4K/8K` × concurrency `1/4/16`，同时保留冷/热两组原始数据：TTFT p50/p95、吞吐、external hit tokens、CPU/memory、NVMe I/O、GPU utilization。原型晋级门槛是：

- 4K/8K、并发 1 的热请求 median TTFT 相比关闭 external cache 至少下降 20%；
- 并发 4/16 的吞吐回退不超过 10%，且 warm P95 TTFT 有改善；
- 所有故障注入仍保证输出正确。

这些数字是当前项目的工程判定标准，不是对所有模型/硬件的通用性能承诺。只有单卡 TCP 通过后，第二阶段才租同可用区的两台同规格实例并开通 ERI/eRDMA，替换 `transport.RDMABackend` 或接入成熟 Transfer Engine；CacheKey、Commit、Prefix Lookup、manifest 和 lease 语义保持不变。

## 旧链路说明

仓库仍保留一些历史代码：

- `internal/ipc/uds_server.go`
- `internal/ipc/shm_mapper.go`
- `pkg/protocol/codec.go`
- `csrc/uds_client.h`
- `csrc/uds_client.cc`
- `csrc/shm_allocator.h`
- `csrc/shm_allocator.cc`
- `internal/storage/shared_memory_pool.go`

这些文件描述的是旧版 UDS + POSIX shared memory 链路。当前默认 daemon 不启动 UDS server，C++ 默认构建也不再链接 UDS/SHM client。

保留它们的原因：

- 方便对比旧链路和新 RDMA 数据面边界。
- 未来如果需要兼容旧客户端，可以重新接入。
- 一些存储层接口仍保留历史适配能力。

## 当前边界和下一步

当前已经完成工程主干：

- Go/C++/Python 使用同一套模型、revision、adapter、layout、token-prefix CacheKey 算法。
- C++ text demo 一次查询完整 block 列表并复用最长连续命中；尾部不足一块不缓存。
- 256-bit ObjectID 已进入 TCP wire 和控制面；uint64 BlockID 只作为物理投影。
- 语义 commit 是可见性边界；未完成 commit 的物理 block 不会被 Prefix Lookup 返回。
- Prefix Lookup 命中带短租约，删除和 memory→disk 搬运会跳过被 pin 的 block。
- `.kvmeta` manifest 随 NVMe block 持久化，daemon 重启会恢复语义索引。
- C++ 计算出的 KV payload 可以通过 RDMA 数据面入口进入 Go 存储。
- RDMA 不可用时可以降级到 P2P TCP 写入。
- Go 存储层会做 checksum 校验、offheap 导入、本地索引、磁盘镜像和控制面公告。
- 多节点之间具备 membership、block location、P2P fetch、fetch refill 的基础能力。
- `integration/vllm` 提供固定 `vllm==0.14.0` 的外部 `ZeroKVConnector`，首版边界和启动方式见该目录 README。

当前尚未完成或尚未验证：

- 当前机器没有 vLLM/CUDA/GPU，Python connector 只完成语法、key golden vector 和静态接口层验证，尚未在真实 vLLM worker 上执行 KV page copy。
- 当前 `internal/rdma` 是协议边界/兼容 listener，不是基于 rdma-core/ibverbs 的 MR/QP/RDMA Write 实现。
- vLLM MVP 只有单 FullAttention group、TP=1、PP=1 和同步 TCP；不应把它描述成已经支持 TP、多 KV group 或异步 prefetch。
- manifest 当前是一对象一物理块，没有多 shard 聚合，也没有后台 orphan block GC。

后续重点：

- 在阿里云单 GPU 上验证 vLLM 0.14.0 的真实 KV page 布局、冷/热请求正确性和 TTFT。
- 通过单卡基线后增加 TP rank / 多 KV group manifest，不在未验证前假装支持。
- 补异步 offload/prefetch 策略和 KV Cache 命中 token、tier latency 等专属指标。
- 将当前单物理块 manifest 扩展为通用多 shard manifest，并增加后台孤儿块回收。
- RDMA 保持可选 transport；需要硬件演示时优先接入成熟 Transfer Engine，不在 ZeroKV 内展开 verbs 实现。

## 设计直觉

ZeroKV 的核心原则是：大 payload 不进控制面，小 metadata 不混入数据面。

这和很多推理系统、分布式缓存、数据库存储引擎的工程边界类似：

- payload 走专门的数据面，减少序列化和不必要复制。
- metadata 走结构化控制面，保证路由、可观测性和一致性。
- 本地优先，miss 后远端 fetch。
- fetch 后回填并公告副本，让热点数据逐渐靠近使用方。
- RDMA 是高性能主路径，P2P 是功能可用性降级路径。
