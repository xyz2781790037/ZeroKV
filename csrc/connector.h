#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include "cache_key.h"

// KVCacheConnectorOptions 定义 C++ 推理引擎与 Go 后端进程进行 IPC
// 通信的底层配置。
struct KVCacheConnectorOptions {
  // 本机 C++ -> Go 的 RDMA 写入入口。
  // 当前接口边界按 RDMA 数据面设计；没有 verbs 后端时由 Go 侧兼容 listener
  // 承接。
  std::string rdma_addr = "127.0.0.1:19100";

  // P2P TCP 数据面入口。RDMA 写入失败时用于降级写入，也用于 get/delete。
  std::string p2p_fallback_addr = "127.0.0.1:19090";

  // 是否在 RDMA 连接或写入失败时自动尝试 P2P 降级。
  bool enable_p2p_fallback = true;

  // 写入后是否阻塞等待 Go 端的 ACK。
  // true: 强一致性，保证 Go 端已接管数据；false: 异步高吞吐，允许丢包。
  bool wait_for_ack = true;
};

// KVCacheBlockMeta 记录一次数据面写入后的结果元数据。
struct KVCacheBlockMeta {
  // 全局单调递增的序列号，用于 Go 端处理乱序包或去重。
  uint64_t seq = 0;

  // 数据块的全局唯一 ID（通常是哈希值或发号器生成）。
  uint64_t block_id = 0;

  // 实际使用的数据面传输："rdma" 或 "p2p_fallback"。
  std::string transport;

  // 数据块的有效载荷长度。
  uint64_t length = 0;

  // CRC32/Murmur3 校验和，防内存静默损坏（Data Rot）。
  uint32_t checksum = 0;
};

enum class KVCacheLookupResult {
  kFound,
  kNotFound,
  kError,
};

enum class KVCachePrefixStopReason : uint16_t {
  kUnknown = 0,
  kFullMatch = 1,
  kNotFound = 2,
  kUnavailable = 3,
  kBusy = 4,
};

struct KVCachePrefixLocation {
  KVCacheDigest object_id{};
  uint64_t block_id = 0;
  uint64_t token_end = 0;
  uint64_t length = 0;
  uint32_t checksum = 0;
  uint16_t tier = 0;
  uint16_t transport = 0;
  std::string node_id;
  std::string address;
};

struct KVCachePrefixLookup {
  std::vector<KVCachePrefixLocation> entries;
  uint64_t matched_tokens = 0;
  uint64_t lease_id = 0;
  int64_t expires_unix_nano = 0;
  KVCachePrefixStopReason stop_reason = KVCachePrefixStopReason::kUnknown;
};

// KVCacheConnector 是 C++ 端的 IPC 核心门面（Facade）。
// 典型生命周期：初始化 -> Connect() -> 高频调用 PutBlock() -> Close()。
class KVCacheConnector {
public:
  explicit KVCacheConnector(KVCacheConnectorOptions options = {});
  ~KVCacheConnector();

  // 禁用拷贝构造和赋值，防止底层的 fd 和原子计数器被错误复制（RAII 语义）。
  KVCacheConnector(const KVCacheConnector &) = delete;
  KVCacheConnector &operator=(const KVCacheConnector &) = delete;

  // 检查 RDMA 或 P2P 降级入口是否可达。
  bool Connect();

  // 释放连接状态。数据面连接按 block 建立，此方法只更新生命周期状态。
  void Close();

  // PutBlock 是核心的数据面写入接口。
  // 物理执行流程：
  // 1. C++ 侧完成 KV 张量计算并传入 payload。
  // 2. 优先通过 RDMA 数据面推送给 Go daemon。
  // 3. RDMA 不可用时可降级到 P2P TCP 写入入口。
  // 4. 默认等待 Go 端 ACK，确认数据已进入本地存储。
  bool PutBlock(uint64_t block_id, const void *data, uint64_t length,
                KVCacheBlockMeta *meta = nullptr);

  // PutCacheObject first commits KV bytes through the existing data plane,
  // then publishes the full 256-bit semantic identity to Prefix Lookup.
  bool PutCacheObject(const KVCacheChunkKey &key, const void *data,
                      uint64_t length, KVCacheBlockMeta *meta = nullptr);

  // 从当前 P2P 数据面读取一个 block。读取可能触发 Go daemon 的本地
  // memory -> disk -> remote peer 回填逻辑。
  bool GetBlock(uint64_t block_id, std::vector<uint8_t> *data,
                KVCacheBlockMeta *meta = nullptr);

  // LookupBlock distinguishes a normal cache miss from transport/protocol
  // failures so inference code does not silently recompute during outages.
  KVCacheLookupResult LookupBlock(uint64_t block_id, std::vector<uint8_t> *data,
                                  KVCacheBlockMeta *meta = nullptr);

  // Performs one batched longest-prefix lookup. Only complete, ordered chunk
  // keys should be supplied. The response contains metadata/locations, never
  // the KV payload itself.
  KVCacheLookupResult
  LookupPrefix(const KVCacheDigest &scope_digest,
               const std::vector<KVCacheChunkKey> &candidates,
               KVCachePrefixLookup *result);

  // Loads one selected entry and verifies the returned physical metadata.
  bool LoadPrefixEntry(const KVCachePrefixLocation &entry,
                       std::vector<uint8_t> *data,
                       KVCacheBlockMeta *meta = nullptr);

  // Explicit release is preferred; the daemon also expires abandoned leases.
  bool ReleasePrefixLease(uint64_t lease_id);

  // 删除当前 daemon 上的本地副本，并清理当前节点的控制面位置。
  // 这不是集群广播删除，不会删除其他节点已经持有的副本。
  bool DeleteBlock(uint64_t block_id);

  // 检查 connector 生命周期状态。
  bool connected() const;

private:
  KVCacheConnectorOptions options_;

  // 线程安全的单调递增发号器，用于生成 Meta 的 seq。
  std::atomic<uint64_t> next_seq_;

  // 内部状态标记。
  bool connected_;

  // 保护 Connector 生命周期变更（Connect/Close）的互斥锁。
  // 注意：高频的 PutBlock 不应抢占此锁，应保证并发写入性能。
  mutable std::mutex mutex_;

  // 获取下一个单调递增的序列号。
  uint64_t NextSeq();
};
