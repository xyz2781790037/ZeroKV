#include "uds_client.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <utility>
#include <vector>
// 仅仅当前文件可用
namespace {

#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif

void AppendUint16LE(std::vector<uint8_t>& buf, uint16_t val) {
    buf.push_back(static_cast<uint8_t>(val & 0xFF));
    buf.push_back(static_cast<uint8_t>((val >> 8) & 0xFF));
}

void AppendUint32LE(std::vector<uint8_t>& buf, uint32_t val) {
    buf.push_back(static_cast<uint8_t>(val & 0xFF));
    buf.push_back(static_cast<uint8_t>((val >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>((val >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((val >> 24) & 0xFF));
}

void AppendUint64LE(std::vector<uint8_t>& buf, uint64_t val) {
    AppendUint32LE(buf, static_cast<uint32_t>(val & 0xFFFFFFFF));
    AppendUint32LE(buf, static_cast<uint32_t>((val >> 32) & 0xFFFFFFFF));
}

uint16_t ReadUint16LE(const uint8_t* data) {
    return static_cast<uint16_t>(data[0]) |
           (static_cast<uint16_t>(data[1]) << 8);
}

uint32_t ReadUint32LE(const uint8_t* data) {
    return static_cast<uint32_t>(data[0]) |
           (static_cast<uint32_t>(data[1]) << 8) |
           (static_cast<uint32_t>(data[2]) << 16) |
           (static_cast<uint32_t>(data[3]) << 24);
}

uint64_t ReadUint64LE(const uint8_t* data) {
    return static_cast<uint64_t>(ReadUint32LE(data)) |
           (static_cast<uint64_t>(ReadUint32LE(data + 4)) << 32);
}

bool ContainsNul(const std::string& value) {
    return value.find('\0') != std::string::npos;
}

}  // namespace

UDSClient::UDSClient(const std::string& socket_path)
    : socket_path_(socket_path), fd_(-1) {}

UDSClient::~UDSClient() {
    Close();
}

UDSClient::UDSClient(UDSClient&& other) noexcept
    : socket_path_(), fd_(-1) {
    std::lock_guard<std::mutex> lock(other.mutex_);
    socket_path_ = std::move(other.socket_path_);
    fd_ = other.fd_;
    other.fd_ = -1;
}

UDSClient& UDSClient::operator=(UDSClient&& other) noexcept {
    if (this != &other) {
        std::lock(mutex_, other.mutex_);
        std::lock_guard<std::mutex> self_lock(mutex_, std::adopt_lock);
        std::lock_guard<std::mutex> other_lock(other.mutex_, std::adopt_lock);

        CloseFd();
        socket_path_ = std::move(other.socket_path_);
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}

void UDSClient::Close() {
    std::lock_guard<std::mutex> lock(mutex_);
    CloseFd();
}

void UDSClient::CloseFd() {
    if (fd_ != -1) {
        close(fd_);
        fd_ = -1;
    }
}

bool UDSClient::Connect() {
    std::lock_guard<std::mutex> lock(mutex_);
    CloseFd();

    sockaddr_un addr {};
    if (socket_path_.empty() || socket_path_.size() >= sizeof(addr.sun_path) ||
        ContainsNul(socket_path_)) {
        std::cerr << "[UDSClient] Invalid socket path." << std::endl;
        return false;
    }

    int new_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (new_fd == -1) {
        int err = errno;
        std::cerr << "[UDSClient] Failed to create socket: "
                  << std::strerror(err) << std::endl;
        return false;
    }

    addr.sun_family = AF_UNIX;
    std::memcpy(addr.sun_path, socket_path_.c_str(), socket_path_.size() + 1);

    auto addr_len = static_cast<socklen_t>(
        offsetof(sockaddr_un, sun_path) + socket_path_.size() + 1);
    if (connect(new_fd, reinterpret_cast<sockaddr*>(&addr), addr_len) == -1) {
        int err = errno;
        std::cerr << "[UDSClient] Failed to connect to " << socket_path_ << ": "
                  << std::strerror(err) << std::endl;
        close(new_fd);
        return false;
    }

    fd_ = new_fd;
    return true;
}

bool UDSClient::SendAll(const uint8_t* data, std::size_t length) {
    if (fd_ == -1)
        return false;

    std::size_t total_sent = 0;
    while (total_sent < length) {
        ssize_t sent =
            send(fd_, data + total_sent, length - total_sent, kSendFlags);
        if (sent < 0) {
            int err = errno;
            if (err == EINTR)
                continue;

            std::cerr << "[UDSClient] Connection lost or send error: "
                      << std::strerror(err) << std::endl;
            CloseFd();
            return false;
        }
        if (sent == 0) {
            std::cerr << "[UDSClient] Connection closed during send."
                      << std::endl;
            CloseFd();
            return false;
        }
        total_sent += static_cast<std::size_t>(sent);
    }
    return true;
}

bool UDSClient::RecvAll(uint8_t* data, std::size_t length) {
    if (fd_ == -1)
        return false;

    std::size_t total_read = 0;
    while (total_read < length) {
        ssize_t n = recv(fd_, data + total_read, length - total_read, 0);
        if (n < 0) {
            int err = errno;
            if (err == EINTR)
                continue;

            std::cerr << "[UDSClient] Connection lost or recv error: "
                      << std::strerror(err) << std::endl;
            CloseFd();
            return false;
        }
        if (n == 0) {
            std::cerr << "[UDSClient] Connection closed during recv."
                      << std::endl;
            CloseFd();
            return false;
        }
        total_read += static_cast<std::size_t>(n);
    }
    return true;
}

bool UDSClient::BuildBlockReadyFrame(uint64_t seq,
                                     uint64_t block_id,
                                     const std::string& shm_name,
                                     uint64_t offset,
                                     uint64_t length,
                                     uint32_t checksum,
                                     std::vector<uint8_t>* buffer) {
    if (buffer == nullptr) {
        std::cerr << "[UDSClient] Nil frame buffer." << std::endl;
        return false;
    }
    if (shm_name.length() > 255 || shm_name.empty()) {
        std::cerr << "[UDSClient] Invalid shm_name length." << std::endl;
        return false;
    }

    if (ContainsNul(shm_name)) {
        std::cerr << "[UDSClient] Invalid shm_name: contains NUL byte."
                  << std::endl;
        return false;
    }

    uint32_t total_payload_len =
        BASE_PAYLOAD_SIZE + static_cast<uint32_t>(shm_name.length());

    buffer->clear();
    buffer->reserve(HEADER_SIZE + total_payload_len);

    // Header offsets match pkg/protocol/codec.go.
    AppendUint32LE(*buffer, PROTOCOL_MAGIC);        // [0:4]
    AppendUint16LE(*buffer, WIRE_VERSION);          // [4:6]
    AppendUint16LE(*buffer, HEADER_SIZE);           // [6:8]
    AppendUint16LE(*buffer, MSG_TYPE_BLOCK_READY);  // [8:10]
    AppendUint16LE(*buffer, 0);                     // Flags [10:12]
    AppendUint32LE(*buffer, total_payload_len);     // Payload Length [12:16]
    AppendUint64LE(*buffer, seq);                   // Sequence [16:24]
    AppendUint64LE(*buffer, 0);                     // Reserved [24:32]

    //有效负载偏移量是相对于有效负载开始的。
    AppendUint64LE(*buffer, block_id);  // [0:8]
    AppendUint64LE(*buffer, offset);    // [8:16]
    AppendUint64LE(*buffer, length);    // [16:24]
    AppendUint32LE(*buffer, checksum);  // [24:28]
    AppendUint16LE(*buffer,
                   static_cast<uint16_t>(shm_name.length()));  // [28:30]
    AppendUint16LE(*buffer, 0);  // Padding [30:32]

    buffer->insert(buffer->end(), shm_name.begin(), shm_name.end());
    return true;
}

bool UDSClient::BuildAllocateBlockFrame(uint64_t seq,
                                        uint64_t block_id,
                                        uint64_t length,
                                        std::vector<uint8_t>* buffer) {
    if (buffer == nullptr) {
        std::cerr << "[UDSClient] Nil frame buffer." << std::endl;
        return false;
    }
    if (block_id == 0 || length == 0) {
        std::cerr << "[UDSClient] Invalid allocation request." << std::endl;
        return false;
    }

    buffer->clear();
    buffer->reserve(HEADER_SIZE + ALLOCATE_BLOCK_PAYLOAD_SIZE);

    AppendUint32LE(*buffer, PROTOCOL_MAGIC);
    AppendUint16LE(*buffer, WIRE_VERSION);
    AppendUint16LE(*buffer, HEADER_SIZE);
    AppendUint16LE(*buffer, MSG_TYPE_ALLOCATE_BLOCK);
    AppendUint16LE(*buffer, 0);
    AppendUint32LE(*buffer, ALLOCATE_BLOCK_PAYLOAD_SIZE);
    AppendUint64LE(*buffer, seq);
    AppendUint64LE(*buffer, 0);

    AppendUint64LE(*buffer, block_id);
    AppendUint64LE(*buffer, length);
    return true;
}

bool UDSClient::SendBlockReady(uint64_t seq,
                               uint64_t block_id,
                               const std::string& shm_name,
                               uint64_t offset,
                               uint64_t length,
                               uint32_t checksum) {
    std::vector<uint8_t> buffer;
    if (!BuildBlockReadyFrame(seq, block_id, shm_name, offset, length,
                              checksum, &buffer)) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    return SendAll(buffer.data(), buffer.size());
}

bool UDSClient::AllocateBlock(uint64_t seq,
                              uint64_t block_id,
                              uint64_t length,
                              UDSBlockAllocation* allocation,
                              std::string* error_message) {
    if (error_message != nullptr) {
        error_message->clear();
    }
    if (allocation == nullptr) {
        std::cerr << "[UDSClient] Nil allocation output." << std::endl;
        return false;
    }
    std::vector<uint8_t> buffer;
    if (!BuildAllocateBlockFrame(seq, block_id, length, &buffer)) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (!SendAll(buffer.data(), buffer.size())) {
        return false;
    }
    return ReadAllocationReply(seq, block_id, allocation, error_message);
}

bool UDSClient::ReadFrameHeader(FrameHeader* header) {
    if (header == nullptr) {
        return false;
    }

    uint8_t data[HEADER_SIZE] {};
    if (!RecvAll(data, HEADER_SIZE)) {
        return false;
    }

    uint32_t magic = ReadUint32LE(data);
    if (magic != PROTOCOL_MAGIC) {
        std::cerr << "[UDSClient] Invalid reply magic: 0x" << std::hex
                  << magic << std::dec << std::endl;
        CloseFd();
        return false;
    }
    uint16_t version = ReadUint16LE(data + 4);
    if (version != WIRE_VERSION) {
        std::cerr << "[UDSClient] Unsupported reply wire version: " << version
                  << std::endl;
        CloseFd();
        return false;
    }
    uint16_t header_size = ReadUint16LE(data + 6);
    if (header_size != HEADER_SIZE) {
        std::cerr << "[UDSClient] Invalid reply header size: " << header_size
                  << std::endl;
        CloseFd();
        return false;
    }

    header->msg_type = ReadUint16LE(data + 8);
    header->flags = ReadUint16LE(data + 10);
    header->payload_len = ReadUint32LE(data + 12);
    header->seq = ReadUint64LE(data + 16);
    if (header->payload_len > MAX_PAYLOAD_SIZE) {
        std::cerr << "[UDSClient] Reply payload too large: "
                  << header->payload_len << std::endl;
        CloseFd();
        return false;
    }
    return true;
}

bool UDSClient::ReadReply(uint64_t expected_seq, std::string* error_message) {
    FrameHeader header;
    if (!ReadFrameHeader(&header)) {
        return false;
    }

    std::vector<uint8_t> payload(header.payload_len);
    if (!payload.empty() && !RecvAll(payload.data(), payload.size())) {
        return false;
    }

    if (header.seq != expected_seq) {
        std::cerr << "[UDSClient] Reply seq mismatch: expected="
                  << expected_seq << " got=" << header.seq << std::endl;
        return false;
    }

    if (header.msg_type == MSG_TYPE_ACK) {
        if (!payload.empty()) {
            std::cerr << "[UDSClient] ACK reply had unexpected payload."
                      << std::endl;
            return false;
        }
        return true;
    }
    if (header.msg_type == MSG_TYPE_ERROR) {
        if (error_message != nullptr) {
            *error_message =
                std::string(payload.begin(), payload.end());
        }
        return false;
    }

    std::cerr << "[UDSClient] Unexpected reply type: " << header.msg_type
              << std::endl;
    return false;
}

bool UDSClient::ReadAllocationReply(uint64_t expected_seq,
                                    uint64_t expected_block_id,
                                    UDSBlockAllocation* allocation,
                                    std::string* error_message) {
    FrameHeader header;
    if (!ReadFrameHeader(&header)) {
        return false;
    }

    std::vector<uint8_t> payload(header.payload_len);
    if (!payload.empty() && !RecvAll(payload.data(), payload.size())) {
        return false;
    }

    if (header.seq != expected_seq) {
        std::cerr << "[UDSClient] Allocation reply seq mismatch: expected="
                  << expected_seq << " got=" << header.seq << std::endl;
        return false;
    }
    if (header.msg_type == MSG_TYPE_ERROR) {
        if (error_message != nullptr) {
            *error_message = std::string(payload.begin(), payload.end());
        }
        return false;
    }
    if (header.msg_type != MSG_TYPE_BLOCK_ALLOCATION) {
        std::cerr << "[UDSClient] Unexpected allocation reply type: "
                  << header.msg_type << std::endl;
        return false;
    }
    if (payload.size() < BLOCK_ALLOCATION_BASE_PAYLOAD_SIZE) {
        std::cerr << "[UDSClient] Allocation reply payload too short."
                  << std::endl;
        return false;
    }

    uint64_t block_id = ReadUint64LE(payload.data());
    uint64_t offset = ReadUint64LE(payload.data() + 8);
    uint64_t length = ReadUint64LE(payload.data() + 16);
    uint16_t shm_name_len = ReadUint16LE(payload.data() + 24);
    std::size_t expected_size =
        BLOCK_ALLOCATION_BASE_PAYLOAD_SIZE + shm_name_len;
    if (payload.size() != expected_size) {
        std::cerr << "[UDSClient] Allocation reply payload size mismatch."
                  << std::endl;
        return false;
    }
    if (block_id != expected_block_id || length == 0) {
        std::cerr << "[UDSClient] Invalid allocation reply metadata."
                  << std::endl;
        return false;
    }
    std::string shm_name(payload.begin() + BLOCK_ALLOCATION_BASE_PAYLOAD_SIZE,
                         payload.end());
    if (shm_name.empty() || shm_name.size() > 255 || ContainsNul(shm_name)) {
        std::cerr << "[UDSClient] Invalid allocation shm name." << std::endl;
        return false;
    }
    allocation->block_id = block_id;
    allocation->shm_name = std::move(shm_name);
    allocation->offset = offset;
    allocation->length = length;
    return true;
}

bool UDSClient::SendBlockReadyAndWait(uint64_t seq,
                                      uint64_t block_id,
                                      const std::string& shm_name,
                                      uint64_t offset,
                                      uint64_t length,
                                      uint32_t checksum,
                                      std::string* error_message) {
    if (error_message != nullptr) {
        error_message->clear();
    }
    std::vector<uint8_t> buffer;
    if (!BuildBlockReadyFrame(seq, block_id, shm_name, offset, length,
                              checksum, &buffer)) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (!SendAll(buffer.data(), buffer.size())) {
        return false;
    }
    return ReadReply(seq, error_message);
}
