#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

// 协议常量，必须与 Go 侧的 pkg/protocol 保持一致
constexpr uint32_t PROTOCOL_MAGIC = 0x5043564b;  // "KVCP"
constexpr uint16_t WIRE_VERSION = 1;
constexpr uint16_t HEADER_SIZE = 32;
constexpr uint16_t MSG_TYPE_BLOCK_READY = 1;
constexpr uint16_t MSG_TYPE_ACK = 2;
constexpr uint16_t MSG_TYPE_ERROR = 3;
constexpr uint16_t MSG_TYPE_ALLOCATE_BLOCK = 4;
constexpr uint16_t MSG_TYPE_BLOCK_ALLOCATION = 5;
constexpr uint16_t BASE_PAYLOAD_SIZE = 32;
constexpr uint16_t ALLOCATE_BLOCK_PAYLOAD_SIZE = 16;
constexpr uint16_t BLOCK_ALLOCATION_BASE_PAYLOAD_SIZE = 32;
constexpr uint32_t MAX_PAYLOAD_SIZE = 1 << 20;

struct UDSBlockAllocation {
    uint64_t block_id = 0;
    std::string shm_name;
    uint64_t offset = 0;
    uint64_t length = 0;
};

class UDSClient {
   public:
    explicit UDSClient(const std::string& socket_path);
    ~UDSClient();

    UDSClient(const UDSClient&) = delete;
    UDSClient& operator=(const UDSClient&) = delete;

    UDSClient(UDSClient&& other) noexcept;
    UDSClient& operator=(UDSClient&& other) noexcept;

    bool Connect();
    void Close();

    // Returns true after the frame is fully written; does not wait for ACK.
    bool SendBlockReady(uint64_t seq,
                        uint64_t block_id,
                        const std::string& shm_name,
                        uint64_t offset,
                        uint64_t length,
                        uint32_t checksum);

    bool SendBlockReadyAndWait(uint64_t seq,
                               uint64_t block_id,
                               const std::string& shm_name,
                               uint64_t offset,
                               uint64_t length,
                               uint32_t checksum,
                               std::string* error_message = nullptr);

    bool AllocateBlock(uint64_t seq,
                       uint64_t block_id,
                       uint64_t length,
                       UDSBlockAllocation* allocation,
                       std::string* error_message = nullptr);

   private:
    struct FrameHeader {
        uint16_t msg_type = 0;
        uint16_t flags = 0;
        uint32_t payload_len = 0;
        uint64_t seq = 0;
    };

    std::string socket_path_;
    int fd_;
    std::mutex mutex_;

    void CloseFd();
    bool BuildBlockReadyFrame(uint64_t seq,
                              uint64_t block_id,
                              const std::string& shm_name,
                              uint64_t offset,
                              uint64_t length,
                              uint32_t checksum,
                              std::vector<uint8_t>* buffer);
    bool BuildAllocateBlockFrame(uint64_t seq,
                                 uint64_t block_id,
                                 uint64_t length,
                                 std::vector<uint8_t>* buffer);
    bool SendAll(const uint8_t* data, std::size_t length);
    bool RecvAll(uint8_t* data, std::size_t length);
    bool ReadFrameHeader(FrameHeader* header);
    bool ReadReply(uint64_t expected_seq, std::string* error_message);
    bool ReadAllocationReply(uint64_t expected_seq,
                             uint64_t expected_block_id,
                             UDSBlockAllocation* allocation,
                             std::string* error_message);
};
