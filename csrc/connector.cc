#include "connector.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <limits>
#include <string>

namespace {

const std::array<uint32_t, 256>& Crc32Table() {
    static const std::array<uint32_t, 256> table = [] {
        std::array<uint32_t, 256> values {};
        for (uint32_t i = 0; i < values.size(); ++i) {
            uint32_t crc = i;
            for (int bit = 0; bit < 8; ++bit) {
                if ((crc & 1) != 0) {
                    crc = (crc >> 1) ^ 0xEDB88320u;
                } else {
                    crc >>= 1;
                }
            }
            values[i] = crc;
        }
        return values;
    }();
    return table;
}

uint32_t ChecksumIEEE(const void* data, uint64_t length) {
    if (data == nullptr || length == 0) {
        return 0;
    }
    const auto& table = Crc32Table();
    const auto* bytes = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFFu;
    for (uint64_t i = 0; i < length; ++i) {
        crc = table[(crc ^ bytes[i]) & 0xFFu] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

bool IsValidShmName(const std::string& name) {
    if (name.empty() || name.size() > 255 ||
        name.find('\0') != std::string::npos ||
        name.find('/') != std::string::npos || name == "." ||
        name == "..") {
        return false;
    }
    return true;
}

bool CheckedAdd(uint64_t a, uint64_t b, uint64_t* out) {
    if (out == nullptr || a > std::numeric_limits<uint64_t>::max() - b) {
        return false;
    }
    *out = a + b;
    return true;
}

bool WriteDaemonSharedMemory(const UDSBlockAllocation& allocation,
                             const void* data,
                             uint64_t length,
                             uint32_t* checksum) {
    if (data == nullptr || checksum == nullptr || length == 0) {
        return false;
    }
    if (allocation.length != length || !IsValidShmName(allocation.shm_name)) {
        std::cerr << "[KVCacheConnector] Invalid daemon SHM allocation."
                  << std::endl;
        return false;
    }
    uint64_t end = 0;
    if (!CheckedAdd(allocation.offset, length, &end)) {
        std::cerr << "[KVCacheConnector] SHM range overflow." << std::endl;
        return false;
    }

    std::string path = "/dev/shm/" + allocation.shm_name;
    int fd = open(path.c_str(), O_RDWR);
    if (fd == -1) {
        int err = errno;
        std::cerr << "[KVCacheConnector] Failed to open daemon SHM " << path
                  << ": " << std::strerror(err) << std::endl;
        return false;
    }

    struct stat st {};
    if (fstat(fd, &st) == -1 || st.st_size < 0 ||
        end > static_cast<uint64_t>(st.st_size)) {
        int err = errno;
        std::cerr << "[KVCacheConnector] Daemon SHM range exceeds file size."
                  << (err != 0 ? std::string(" error=") + std::strerror(err)
                               : std::string())
                  << std::endl;
        close(fd);
        return false;
    }

    uint64_t page_size = static_cast<uint64_t>(sysconf(_SC_PAGESIZE));
    uint64_t map_offset = allocation.offset / page_size * page_size;
    uint64_t data_start = allocation.offset - map_offset;
    uint64_t map_length = data_start + length;
    if (map_offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) ||
        map_length >
            static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        std::cerr << "[KVCacheConnector] SHM mmap range too large."
                  << std::endl;
        close(fd);
        return false;
    }

    void* mapped = mmap(nullptr, static_cast<size_t>(map_length),
                        PROT_READ | PROT_WRITE, MAP_SHARED, fd,
                        static_cast<off_t>(map_offset));
    if (mapped == MAP_FAILED) {
        int err = errno;
        std::cerr << "[KVCacheConnector] mmap daemon SHM failed: "
                  << std::strerror(err) << std::endl;
        close(fd);
        return false;
    }

    auto* dst = static_cast<uint8_t*>(mapped) + data_start;
    std::memcpy(dst, data, static_cast<size_t>(length));
    *checksum = ChecksumIEEE(dst, length);

    if (munmap(mapped, static_cast<size_t>(map_length)) == -1) {
        int err = errno;
        std::cerr << "[KVCacheConnector] munmap daemon SHM failed: "
                  << std::strerror(err) << std::endl;
        close(fd);
        return false;
    }
    close(fd);
    return true;
}

}  // namespace
KVCacheConnector::KVCacheConnector(KVCacheConnectorOptions options)
    : options_(std::move(options)),
      uds_(options_.socket_path),
      next_seq_(1),
      connected_(false) {}
KVCacheConnector::~KVCacheConnector() {
    Close();
}
bool KVCacheConnector::Connect() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (connected_) {
        return true;
    }
    if (!uds_.Connect()) {
        return false;
    }

    connected_ = true;
    return true;
}
void KVCacheConnector::Close() {
    std::lock_guard<std::mutex> lock(mutex_);
    uds_.Close();
    connected_ = false;
}
uint64_t KVCacheConnector::NextSeq() {
    // 使用 memory_order_relaxed：告诉编译器和 CPU：“我只要求这个变量本身的 +1
    // 操作是原子的（不发生多线程踩踏），我根本不在乎这行代码前后的其他变量是什么执行顺序”
    uint64_t seq = next_seq_.fetch_add(
        1, std::memory_order_relaxed);  // fetch_add是先获取再+1
    if (seq == 0) {
        seq = next_seq_.fetch_add(1, std::memory_order_relaxed);
    }
    return seq;
}
// PutBlock 是 C++ 推理引擎向 Go 守护进程投递 KV Cache 数据块的核心入口。
// 物理动作：将真实数据写入共享内存（数据面），并通过 UDS
// 发送内存寻址元数据（控制面）。
bool KVCacheConnector::PutBlock(uint64_t block_id,
                                const void* data,
                                uint64_t length,
                                KVCacheBlockMeta* meta) {
    // 1. 物理边界防御
    // 拦截空指针和零长度拷贝，防止底层 allocator 触发系统级段错误 (Segfault)
    // 或未定义行为。
    if (data == nullptr || length == 0) {
        std::cerr << "[KVCacheConnector] Invalid block data or length."
                  << std::endl;
        return false;
    }

    uint64_t alloc_seq = NextSeq();
    uint64_t ready_seq = NextSeq();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_) {
            std::cerr << "[KVCacheConnector] Connector is not connected."
                      << std::endl;
            return false;
        }
    }

    std::string error_message;
    UDSBlockAllocation allocation;
    if (!uds_.AllocateBlock(alloc_seq, block_id, length, &allocation,
                            &error_message)) {
        if (!error_message.empty()) {
            std::cerr << "[KVCacheConnector] Go daemon rejected allocation for "
                      << block_id << ": " << error_message << std::endl;
        }
        return false;
    }

    uint32_t checksum = 0;
    if (!WriteDaemonSharedMemory(allocation, data, length, &checksum)) {
        return false;
    }

    bool ok = false;
    if (options_.wait_for_ack) {
        ok = uds_.SendBlockReadyAndWait(ready_seq, block_id, allocation.shm_name,
                                        allocation.offset, allocation.length,
                                        checksum, &error_message);
    } else {
        ok = uds_.SendBlockReady(ready_seq, block_id, allocation.shm_name,
                                 allocation.offset, allocation.length,
                                 checksum);
    }

    if (!ok) {
        if (!error_message.empty()) {
            std::cerr << "[KVCacheConnector] Go daemon rejected block "
                      << block_id << ": " << error_message << std::endl;
        }
        return false;
    }

    if (meta != nullptr) {
        meta->seq = ready_seq;
        meta->block_id = block_id;
        meta->shm_name = allocation.shm_name;
        meta->offset = allocation.offset;
        meta->length = allocation.length;
        meta->checksum = checksum;
    }

    return true;
}
void KVCacheConnector::ResetLocalPool() {
    // No-op in daemon-owned SHM mode. The daemon owns pool reset/reclaim.
}
bool KVCacheConnector::connected() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return connected_;
}
