#pragma once

#include "uds_client.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>

// KVCacheConnectorOptions 定义 C++ 推理引擎与 Go 后端进程进行 IPC
// 通信的底层配置。
struct KVCacheConnectorOptions {
    // Unix Domain Socket 路径。
    // 对应 Go 端的 internal/ipc/uds_server.go，用于传输轻量级的控制信令
    std::string socket_path = "/tmp/kvcache.sock";

    // 保留字段：daemon-owned SHM 模式下共享内存由 Go daemon 分配。
    std::string shm_name;

    // 保留字段：daemon-owned SHM 模式下池大小由 Go daemon 的 -shm-bytes 控制。
    uint64_t shm_size = 64ULL * 1024ULL * 1024ULL;

    // 写入后是否阻塞等待 Go 端的 ACK。
    // true: 强一致性，保证 Go 端已接管数据；false: 异步高吞吐，允许丢包。
    bool wait_for_ack = true;
};

// KVCacheBlockMeta 定义数据块在共享内存中的绝对寻址元数据。
// 这个结构体最终会被序列化并通过 UDS 发送给 Go 进程。
struct KVCacheBlockMeta {
    // 全局单调递增的序列号，用于 Go 端处理乱序包或去重。
    uint64_t seq = 0;

    // 数据块的全局唯一 ID（通常是哈希值或发号器生成）。
    uint64_t block_id = 0;

    // 所在共享内存的名称（支持多 SHM 文件隔离）。
    std::string shm_name;

    // 数据在该共享内存文件中的绝对物理偏移量（Offset）。
    // Go 端的 shm_mapper 依赖此字段通过 mmap 直接读取对应物理页。
    uint64_t offset = 0;

    // 数据块的有效载荷长度。
    uint64_t length = 0;

    // CRC32/Murmur3 校验和，防内存静默损坏（Data Rot）。
    uint32_t checksum = 0;
};

// KVCacheConnector 是 C++ 端的 IPC 核心门面（Facade）。
// 典型生命周期：初始化 -> Connect() -> 高频调用 PutBlock() -> Close()。
class KVCacheConnector {
   public:
    explicit KVCacheConnector(KVCacheConnectorOptions options = {});
    ~KVCacheConnector();

    // 禁用拷贝构造和赋值，防止底层的 fd 和原子计数器被错误复制（RAII 语义）。
    KVCacheConnector(const KVCacheConnector&) = delete;
    KVCacheConnector& operator=(const KVCacheConnector&) = delete;

    // 建立 UDS 连接。共享内存 slot 由 Go daemon 在 PutBlock 时分配。
    bool Connect();

    // 释放资源，向 Go 端发送断开信令。
    void Close();

    // PutBlock 是核心的数据面写入接口。
    // 物理执行流程：
    // 1. 通过 UDS 向 Go daemon 申请 daemon-owned SHM slot。
    // 2. mmap 该 slot，把 payload 写入 Go daemon 管理的共享内存。
    // 3. 通过 UDS 发送 BlockReady 元数据。
    // 4. 如果 options_.wait_for_ack 为 true，阻塞等待 Go 返回成功响应。
    bool PutBlock(uint64_t block_id,
                  const void* data,
                  uint64_t length,
                  KVCacheBlockMeta* meta = nullptr);

    // daemon-owned SHM 模式下 C++ 不拥有本地共享内存池；该方法保留为兼容接口。
    void ResetLocalPool();

    // 检查 UDS 与 SHM 的挂载状态。
    bool connected() const;

   private:
    KVCacheConnectorOptions options_;

    // UDS 客户端，负责维护与 Go Daemon 的长连接。
    UDSClient uds_;

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
