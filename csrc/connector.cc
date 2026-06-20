#include "connector.h"

#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace {

constexpr uint32_t kRDMAMagic = 0x414d4452; // "RDMA" little-endian.
constexpr uint16_t kRDMAWireVersion = 1;
constexpr uint16_t kRDMAHeaderSize = 40;
constexpr uint16_t kRDMAMessagePutBlock = 1;
constexpr uint16_t kRDMAMessageAck = 2;
constexpr uint16_t kRDMAMessageError = 3;

constexpr uint32_t kP2PMagic = 0x4e50564b;
constexpr uint16_t kP2PWireVersion = 1;
constexpr uint16_t kP2PHeaderSize = 32;
constexpr uint16_t kP2PMessageGetBlock = 1;
constexpr uint16_t kP2PMessageBlock = 2;
constexpr uint16_t kP2PMessagePutBlock = 6;
constexpr uint16_t kP2PMessageAck = 7;
constexpr uint16_t kP2PMessageError = 3;
constexpr uint16_t kP2PMessageDeleteBlock = 8;
constexpr uint16_t kP2PMessageCommitCacheObject = 9;
constexpr uint16_t kP2PMessageLookupPrefix = 10;
constexpr uint16_t kP2PMessagePrefixLookupResult = 11;
constexpr uint16_t kP2PMessageReleasePrefixLease = 12;
constexpr uint64_t kP2PPutBlockMetaSize = 8;
constexpr uint16_t kPrefixProtocolVersion = 1;
constexpr std::size_t kCacheCommitPayloadSize = 128;
constexpr std::size_t kPrefixRequestHeaderSize = 40;
constexpr std::size_t kPrefixCandidateSize = 40;
constexpr std::size_t kPrefixResultHeaderSize = 40;
constexpr std::size_t kPrefixResultEntrySize = 68;
constexpr std::size_t kMaxPrefixEntries = 8192;
constexpr uint64_t kMaxPrefixPayloadBytes = 2U << 20;
constexpr std::size_t kIOChunkSize = 1U << 20;
constexpr char kP2PBlockNotFoundError[] = "network: block not found";

const std::array<uint32_t, 256> &Crc32Table() {
  static const std::array<uint32_t, 256> table = [] {
    std::array<uint32_t, 256> values{};
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

uint32_t ChecksumIEEE(const void *data, uint64_t length) {
  if (data == nullptr || length == 0) {
    return 0;
  }
  const auto &table = Crc32Table();
  const auto *bytes = static_cast<const uint8_t *>(data);
  uint32_t crc = 0xFFFFFFFFu;
  for (uint64_t i = 0; i < length; ++i) {
    crc = table[(crc ^ bytes[i]) & 0xFFu] ^ (crc >> 8);
  }
  return crc ^ 0xFFFFFFFFu;
}

void PutLE16(uint8_t *dst, uint16_t value) {
  dst[0] = static_cast<uint8_t>(value);
  dst[1] = static_cast<uint8_t>(value >> 8);
}

void PutLE32(uint8_t *dst, uint32_t value) {
  for (int i = 0; i < 4; ++i) {
    dst[i] = static_cast<uint8_t>(value >> (8 * i));
  }
}

void PutLE64(uint8_t *dst, uint64_t value) {
  for (int i = 0; i < 8; ++i) {
    dst[i] = static_cast<uint8_t>(value >> (8 * i));
  }
}

uint16_t GetLE16(const uint8_t *src) {
  return static_cast<uint16_t>(src[0]) | (static_cast<uint16_t>(src[1]) << 8);
}

uint32_t GetLE32(const uint8_t *src) {
  uint32_t value = 0;
  for (int i = 0; i < 4; ++i) {
    value |= static_cast<uint32_t>(src[i]) << (8 * i);
  }
  return value;
}

uint64_t GetLE64(const uint8_t *src) {
  uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value |= static_cast<uint64_t>(src[i]) << (8 * i);
  }
  return value;
}

bool SplitHostPort(const std::string &addr, std::string *host,
                   std::string *port) {
  if (host == nullptr || port == nullptr || addr.empty()) {
    return false;
  }
  std::size_t sep = addr.rfind(':');
  if (sep == std::string::npos || sep + 1 >= addr.size()) {
    return false;
  }
  *host = addr.substr(0, sep);
  *port = addr.substr(sep + 1);
  if (host->empty()) {
    *host = "127.0.0.1";
  }
  return !port->empty();
}

class SocketFD {
public:
  SocketFD() = default;
  explicit SocketFD(int fd) : fd_(fd) {}
  ~SocketFD() { Close(); }

  SocketFD(const SocketFD &) = delete;
  SocketFD &operator=(const SocketFD &) = delete;

  int get() const { return fd_; }
  bool valid() const { return fd_ >= 0; }

  void Reset(int fd) {
    Close();
    fd_ = fd;
  }

  void Close() {
    if (fd_ >= 0) {
      close(fd_);
      fd_ = -1;
    }
  }

private:
  int fd_ = -1;
};

bool ConnectTCP(const std::string &addr, SocketFD *out,
                std::string *error_message) {
  if (out == nullptr) {
    return false;
  }
  std::string host;
  std::string port;
  if (!SplitHostPort(addr, &host, &port)) {
    if (error_message != nullptr) {
      *error_message = "invalid address: " + addr;
    }
    return false;
  }

  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  addrinfo *results = nullptr;
  int rc = getaddrinfo(host.c_str(), port.c_str(), &hints, &results);
  if (rc != 0) {
    if (error_message != nullptr) {
      *error_message = std::string("getaddrinfo failed: ") + gai_strerror(rc);
    }
    return false;
  }

  std::string last_error;
  for (addrinfo *ai = results; ai != nullptr; ai = ai->ai_next) {
    int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) {
      last_error = std::strerror(errno);
      continue;
    }
    if (connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) {
      freeaddrinfo(results);
      out->Reset(fd);
      return true;
    }
    last_error = std::strerror(errno);
    close(fd);
  }

  freeaddrinfo(results);
  if (error_message != nullptr) {
    *error_message = last_error.empty() ? "connect failed" : last_error;
  }
  return false;
}

bool SendAll(int fd, const uint8_t *data, uint64_t length) {
  if (fd < 0 || (data == nullptr && length != 0)) {
    return false;
  }
  uint64_t sent = 0;
  while (sent < length) {
    uint64_t remaining = length - sent;
    std::size_t chunk = remaining > kIOChunkSize
                            ? kIOChunkSize
                            : static_cast<std::size_t>(remaining);
    ssize_t n = send(fd, data + sent, chunk, MSG_NOSIGNAL);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      std::cerr << "[KVCacheConnector] send failed: " << std::strerror(errno)
                << std::endl;
      return false;
    }
    if (n == 0) {
      std::cerr << "[KVCacheConnector] connection closed during send."
                << std::endl;
      return false;
    }
    sent += static_cast<uint64_t>(n);
  }
  return true;
}

bool RecvAll(int fd, uint8_t *data, uint64_t length) {
  if (fd < 0 || (data == nullptr && length != 0)) {
    return false;
  }
  uint64_t read = 0;
  while (read < length) {
    uint64_t remaining = length - read;
    std::size_t chunk = remaining > kIOChunkSize
                            ? kIOChunkSize
                            : static_cast<std::size_t>(remaining);
    ssize_t n = recv(fd, data + read, chunk, 0);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      std::cerr << "[KVCacheConnector] recv failed: " << std::strerror(errno)
                << std::endl;
      return false;
    }
    if (n == 0) {
      std::cerr << "[KVCacheConnector] connection closed during recv."
                << std::endl;
      return false;
    }
    read += static_cast<uint64_t>(n);
  }
  return true;
}

bool ReadErrorPayload(int fd, uint64_t payload_len, uint32_t checksum,
                      std::string *error_message) {
  if (payload_len > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
    if (error_message != nullptr) {
      *error_message = "error payload too large";
    }
    return false;
  }
  std::string payload(static_cast<std::size_t>(payload_len), '\0');
  if (payload_len != 0 &&
      !RecvAll(fd, reinterpret_cast<uint8_t *>(&payload[0]), payload_len)) {
    return false;
  }
  if (ChecksumIEEE(payload.data(), payload_len) != checksum) {
    if (error_message != nullptr) {
      *error_message = "error payload checksum mismatch";
    }
    return false;
  }
  if (error_message != nullptr) {
    *error_message = payload;
  }
  return true;
}

bool ReadRDMAReply(int fd, uint64_t expected_seq, uint64_t expected_block_id,
                   std::string *error_message) {
  uint8_t header[kRDMAHeaderSize]{};
  if (!RecvAll(fd, header, kRDMAHeaderSize)) {
    return false;
  }
  if (GetLE32(header) != kRDMAMagic ||
      GetLE16(header + 4) != kRDMAWireVersion ||
      GetLE16(header + 6) != kRDMAHeaderSize || GetLE16(header + 10) != 0) {
    if (error_message != nullptr) {
      *error_message = "invalid RDMA reply header";
    }
    return false;
  }
  const uint16_t msg_type = GetLE16(header + 8);
  const uint32_t checksum = GetLE32(header + 12);
  const uint64_t seq = GetLE64(header + 16);
  const uint64_t block_id = GetLE64(header + 24);
  const uint64_t payload_len = GetLE64(header + 32);
  if (seq != expected_seq || block_id != expected_block_id) {
    if (error_message != nullptr) {
      *error_message = "RDMA reply sequence or block id mismatch";
    }
    return false;
  }
  if (msg_type == kRDMAMessageError) {
    (void)ReadErrorPayload(fd, payload_len, checksum, error_message);
    return false;
  }
  if (msg_type != kRDMAMessageAck || payload_len != 0 || checksum != 0) {
    if (error_message != nullptr) {
      *error_message = "unexpected RDMA reply";
    }
    return false;
  }
  return true;
}

bool ReadP2PReply(int fd, uint64_t expected_block_id,
                  std::string *error_message) {
  uint8_t header[kP2PHeaderSize]{};
  if (!RecvAll(fd, header, kP2PHeaderSize)) {
    return false;
  }
  if (GetLE32(header) != kP2PMagic || GetLE16(header + 4) != kP2PWireVersion ||
      GetLE16(header + 6) != kP2PHeaderSize || GetLE16(header + 10) != 0) {
    if (error_message != nullptr) {
      *error_message = "invalid P2P reply header";
    }
    return false;
  }
  const uint16_t msg_type = GetLE16(header + 8);
  const uint32_t checksum = GetLE32(header + 12);
  const uint64_t block_id = GetLE64(header + 16);
  const uint64_t payload_len = GetLE64(header + 24);
  if (block_id != expected_block_id) {
    if (error_message != nullptr) {
      *error_message = "P2P reply block id mismatch";
    }
    return false;
  }
  if (msg_type == kP2PMessageError) {
    (void)ReadErrorPayload(fd, payload_len, checksum, error_message);
    return false;
  }
  if (msg_type != kP2PMessageAck || payload_len != 0 || checksum != 0) {
    if (error_message != nullptr) {
      *error_message = "unexpected P2P reply";
    }
    return false;
  }
  return true;
}

bool WriteP2PHeader(int fd, uint16_t message_type, uint64_t block_id,
                    uint64_t payload_len, uint32_t checksum) {
  uint8_t header[kP2PHeaderSize]{};
  PutLE32(header, kP2PMagic);
  PutLE16(header + 4, kP2PWireVersion);
  PutLE16(header + 6, kP2PHeaderSize);
  PutLE16(header + 8, message_type);
  PutLE32(header + 12, checksum);
  PutLE64(header + 16, block_id);
  PutLE64(header + 24, payload_len);
  return SendAll(fd, header, kP2PHeaderSize);
}

bool ReadP2PDataBlock(int fd, uint64_t expected_block_id,
                      std::vector<uint8_t> *data, KVCacheBlockMeta *meta,
                      std::string *error_message) {
  if (data == nullptr) {
    if (error_message != nullptr) {
      *error_message = "nil output buffer";
    }
    return false;
  }
  uint8_t header[kP2PHeaderSize]{};
  if (!RecvAll(fd, header, kP2PHeaderSize)) {
    return false;
  }
  if (GetLE32(header) != kP2PMagic || GetLE16(header + 4) != kP2PWireVersion ||
      GetLE16(header + 6) != kP2PHeaderSize || GetLE16(header + 10) != 0) {
    if (error_message != nullptr) {
      *error_message = "invalid P2P block reply header";
    }
    return false;
  }
  const uint16_t msg_type = GetLE16(header + 8);
  const uint32_t checksum = GetLE32(header + 12);
  const uint64_t block_id = GetLE64(header + 16);
  const uint64_t payload_len = GetLE64(header + 24);
  if (block_id != expected_block_id) {
    if (error_message != nullptr) {
      *error_message = "P2P reply block id mismatch";
    }
    return false;
  }
  if (msg_type == kP2PMessageError) {
    (void)ReadErrorPayload(fd, payload_len, checksum, error_message);
    return false;
  }
  if (msg_type != kP2PMessageBlock) {
    if (error_message != nullptr) {
      *error_message = "unexpected P2P block reply";
    }
    return false;
  }
  if (payload_len >
      static_cast<uint64_t>(std::numeric_limits<std::size_t>::max())) {
    if (error_message != nullptr) {
      *error_message = "P2P block payload too large";
    }
    return false;
  }
  std::vector<uint8_t> payload(static_cast<std::size_t>(payload_len));
  if (payload_len != 0 && !RecvAll(fd, payload.data(), payload_len)) {
    return false;
  }
  if (ChecksumIEEE(payload.data(), payload_len) != checksum) {
    if (error_message != nullptr) {
      *error_message = "P2P block checksum mismatch";
    }
    return false;
  }
  if (meta != nullptr) {
    meta->seq = 0;
    meta->block_id = block_id;
    meta->transport = "p2p";
    meta->length = payload_len;
    meta->checksum = checksum;
  }
  *data = std::move(payload);
  return true;
}

bool SendRDMABlock(const std::string &addr, uint64_t seq, uint64_t block_id,
                   const void *data, uint64_t length, uint32_t checksum,
                   bool wait_for_ack, std::string *error_message) {
  SocketFD fd;
  if (!ConnectTCP(addr, &fd, error_message)) {
    return false;
  }
  uint8_t header[kRDMAHeaderSize]{};
  PutLE32(header, kRDMAMagic);
  PutLE16(header + 4, kRDMAWireVersion);
  PutLE16(header + 6, kRDMAHeaderSize);
  PutLE16(header + 8, kRDMAMessagePutBlock);
  PutLE32(header + 12, checksum);
  PutLE64(header + 16, seq);
  PutLE64(header + 24, block_id);
  PutLE64(header + 32, length);
  if (!SendAll(fd.get(), header, kRDMAHeaderSize) ||
      !SendAll(fd.get(), static_cast<const uint8_t *>(data), length)) {
    if (error_message != nullptr && error_message->empty()) {
      *error_message = "RDMA send failed";
    }
    return false;
  }
  if (!wait_for_ack) {
    return true;
  }
  return ReadRDMAReply(fd.get(), seq, block_id, error_message);
}

bool SendP2PFallbackBlock(const std::string &addr, uint64_t seq,
                          uint64_t block_id, const void *data, uint64_t length,
                          uint32_t checksum, bool wait_for_ack,
                          std::string *error_message) {
  if (length > std::numeric_limits<uint64_t>::max() - kP2PPutBlockMetaSize) {
    if (error_message != nullptr) {
      *error_message = "P2P payload length overflow";
    }
    return false;
  }
  SocketFD fd;
  if (!ConnectTCP(addr, &fd, error_message)) {
    return false;
  }
  uint8_t header[kP2PHeaderSize]{};
  PutLE32(header, kP2PMagic);
  PutLE16(header + 4, kP2PWireVersion);
  PutLE16(header + 6, kP2PHeaderSize);
  PutLE16(header + 8, kP2PMessagePutBlock);
  PutLE32(header + 12, checksum);
  PutLE64(header + 16, block_id);
  PutLE64(header + 24, length + kP2PPutBlockMetaSize);
  uint8_t meta[kP2PPutBlockMetaSize]{};
  PutLE64(meta, seq);
  if (!SendAll(fd.get(), header, kP2PHeaderSize) ||
      !SendAll(fd.get(), meta, kP2PPutBlockMetaSize) ||
      !SendAll(fd.get(), static_cast<const uint8_t *>(data), length)) {
    if (error_message != nullptr && error_message->empty()) {
      *error_message = "P2P fallback send failed";
    }
    return false;
  }
  if (!wait_for_ack) {
    return true;
  }
  return ReadP2PReply(fd.get(), block_id, error_message);
}

bool FetchP2PBlock(const std::string &addr, uint64_t block_id,
                   std::vector<uint8_t> *data, KVCacheBlockMeta *meta,
                   std::string *error_message) {
  SocketFD fd;
  if (!ConnectTCP(addr, &fd, error_message)) {
    return false;
  }
  if (!WriteP2PHeader(fd.get(), kP2PMessageGetBlock, block_id, 0, 0)) {
    if (error_message != nullptr && error_message->empty()) {
      *error_message = "P2P get request failed";
    }
    return false;
  }
  return ReadP2PDataBlock(fd.get(), block_id, data, meta, error_message);
}

bool DeleteP2PBlock(const std::string &addr, uint64_t block_id,
                    std::string *error_message) {
  SocketFD fd;
  if (!ConnectTCP(addr, &fd, error_message)) {
    return false;
  }
  if (!WriteP2PHeader(fd.get(), kP2PMessageDeleteBlock, block_id, 0, 0)) {
    if (error_message != nullptr && error_message->empty()) {
      *error_message = "P2P delete request failed";
    }
    return false;
  }
  return ReadP2PReply(fd.get(), block_id, error_message);
}

bool IsZeroDigest(const KVCacheDigest &digest) {
  return std::all_of(digest.begin(), digest.end(),
                     [](uint8_t value) { return value == 0; });
}

bool CommitP2PCacheObject(const std::string &addr, const KVCacheChunkKey &key,
                          const KVCacheBlockMeta &meta,
                          std::string *error_message) {
  if (key.block_id == 0 || key.key.token_count == 0 || meta.length == 0 ||
      key.block_id != meta.block_id ||
      GetLE64(key.object_id.data()) != key.block_id) {
    if (error_message != nullptr) {
      *error_message = "invalid cache object commit";
    }
    return false;
  }
  std::array<uint8_t, kCacheCommitPayloadSize> payload{};
  PutLE16(payload.data(), kPrefixProtocolVersion);
  PutLE64(payload.data() + 8, key.key.token_count);
  PutLE64(payload.data() + 16, meta.length);
  PutLE32(payload.data() + 24, meta.checksum);
  std::memcpy(payload.data() + 32, key.key.scope_digest.data(),
              key.key.scope_digest.size());
  std::memcpy(payload.data() + 64, key.key.prefix_digest.data(),
              key.key.prefix_digest.size());
  std::memcpy(payload.data() + 96, key.object_id.data(), key.object_id.size());

  SocketFD fd;
  if (!ConnectTCP(addr, &fd, error_message)) {
    return false;
  }
  if (!WriteP2PHeader(fd.get(), kP2PMessageCommitCacheObject, key.block_id,
                      payload.size(),
                      ChecksumIEEE(payload.data(), payload.size())) ||
      !SendAll(fd.get(), payload.data(), payload.size())) {
    if (error_message != nullptr && error_message->empty()) {
      *error_message = "cache object commit send failed";
    }
    return false;
  }
  return ReadP2PReply(fd.get(), key.block_id, error_message);
}

bool ReadPrefixLookupReply(int fd, uint64_t request_id,
                           const std::vector<KVCacheChunkKey> &candidates,
                           KVCachePrefixLookup *result,
                           std::string *error_message) {
  std::array<uint8_t, kP2PHeaderSize> header{};
  if (!RecvAll(fd, header.data(), header.size())) {
    return false;
  }
  if (GetLE32(header.data()) != kP2PMagic ||
      GetLE16(header.data() + 4) != kP2PWireVersion ||
      GetLE16(header.data() + 6) != kP2PHeaderSize ||
      GetLE16(header.data() + 10) != 0 ||
      GetLE64(header.data() + 16) != request_id) {
    if (error_message != nullptr) {
      *error_message = "invalid prefix lookup reply header";
    }
    return false;
  }
  const uint16_t message_type = GetLE16(header.data() + 8);
  const uint32_t checksum = GetLE32(header.data() + 12);
  const uint64_t payload_length = GetLE64(header.data() + 24);
  if (message_type == kP2PMessageError) {
    (void)ReadErrorPayload(fd, payload_length, checksum, error_message);
    return false;
  }
  if (message_type != kP2PMessagePrefixLookupResult ||
      payload_length < kPrefixResultHeaderSize ||
      payload_length > kMaxPrefixPayloadBytes) {
    if (error_message != nullptr) {
      *error_message = "unexpected prefix lookup reply";
    }
    return false;
  }
  std::vector<uint8_t> payload(static_cast<std::size_t>(payload_length));
  if (!RecvAll(fd, payload.data(), payload_length) ||
      ChecksumIEEE(payload.data(), payload_length) != checksum) {
    if (error_message != nullptr) {
      *error_message = "prefix lookup reply checksum mismatch";
    }
    return false;
  }
  if (GetLE16(payload.data()) != kPrefixProtocolVersion) {
    if (error_message != nullptr) {
      *error_message = "unsupported prefix lookup reply version";
    }
    return false;
  }
  const uint32_t count = GetLE32(payload.data() + 4);
  if (count > candidates.size() || count > kMaxPrefixEntries) {
    if (error_message != nullptr) {
      *error_message = "invalid prefix lookup result count";
    }
    return false;
  }
  KVCachePrefixLookup decoded;
  decoded.stop_reason =
      static_cast<KVCachePrefixStopReason>(GetLE16(payload.data() + 2));
  decoded.matched_tokens = GetLE64(payload.data() + 8);
  decoded.lease_id = GetLE64(payload.data() + 16);
  decoded.expires_unix_nano =
      static_cast<int64_t>(GetLE64(payload.data() + 24));
  decoded.entries.reserve(count);

  std::size_t offset = kPrefixResultHeaderSize;
  for (uint32_t index = 0; index < count; ++index) {
    if (payload.size() - offset < kPrefixResultEntrySize) {
      if (error_message != nullptr) {
        *error_message = "truncated prefix lookup entry";
      }
      return false;
    }
    const uint8_t *entry_bytes = payload.data() + offset;
    KVCachePrefixLocation entry;
    std::memcpy(entry.object_id.data(), entry_bytes, entry.object_id.size());
    entry.block_id = GetLE64(entry_bytes + 32);
    entry.token_end = GetLE64(entry_bytes + 40);
    entry.length = GetLE64(entry_bytes + 48);
    entry.checksum = GetLE32(entry_bytes + 56);
    entry.tier = GetLE16(entry_bytes + 60);
    entry.transport = GetLE16(entry_bytes + 62);
    const uint16_t node_length = GetLE16(entry_bytes + 64);
    const uint16_t address_length = GetLE16(entry_bytes + 66);
    offset += kPrefixResultEntrySize;
    if (payload.size() - offset <
        static_cast<std::size_t>(node_length) + address_length) {
      if (error_message != nullptr) {
        *error_message = "truncated prefix lookup location";
      }
      return false;
    }
    entry.node_id.assign(
        reinterpret_cast<const char *>(payload.data() + offset), node_length);
    offset += node_length;
    entry.address.assign(
        reinterpret_cast<const char *>(payload.data() + offset),
        address_length);
    offset += address_length;
    const KVCacheChunkKey &expected = candidates[index];
    if (entry.object_id != expected.object_id ||
        entry.block_id != expected.block_id ||
        entry.token_end != expected.token_end || entry.length == 0 ||
        entry.address.empty()) {
      if (error_message != nullptr) {
        *error_message = "prefix lookup entry does not match request";
      }
      return false;
    }
    decoded.entries.push_back(std::move(entry));
  }
  if (offset != payload.size() ||
      (decoded.entries.empty() && decoded.lease_id != 0) ||
      (!decoded.entries.empty() && decoded.lease_id == 0) ||
      (!decoded.entries.empty() &&
       decoded.matched_tokens != decoded.entries.back().token_end)) {
    if (error_message != nullptr) {
      *error_message = "malformed prefix lookup result";
    }
    return false;
  }
  *result = std::move(decoded);
  return true;
}

bool LookupP2PPrefix(const std::string &addr, uint64_t request_id,
                     const KVCacheDigest &scope_digest,
                     const std::vector<KVCacheChunkKey> &candidates,
                     KVCachePrefixLookup *result, std::string *error_message) {
  if (result == nullptr || candidates.empty() ||
      candidates.size() > kMaxPrefixEntries || IsZeroDigest(scope_digest)) {
    if (error_message != nullptr) {
      *error_message = "invalid prefix lookup request";
    }
    return false;
  }
  const std::size_t payload_size =
      kPrefixRequestHeaderSize + candidates.size() * kPrefixCandidateSize;
  std::vector<uint8_t> payload(payload_size, 0);
  PutLE16(payload.data(), kPrefixProtocolVersion);
  PutLE32(payload.data() + 4, static_cast<uint32_t>(candidates.size()));
  std::memcpy(payload.data() + 8, scope_digest.data(), scope_digest.size());
  std::size_t offset = kPrefixRequestHeaderSize;
  uint64_t previous_token_end = 0;
  for (const auto &candidate : candidates) {
    if (candidate.key.scope_digest != scope_digest ||
        candidate.object_id == KVCacheDigest{} ||
        candidate.block_id != GetLE64(candidate.object_id.data()) ||
        candidate.token_end <= previous_token_end) {
      if (error_message != nullptr) {
        *error_message = "invalid or unordered prefix candidate";
      }
      return false;
    }
    std::memcpy(payload.data() + offset, candidate.object_id.data(),
                candidate.object_id.size());
    PutLE64(payload.data() + offset + 32, candidate.token_end);
    offset += kPrefixCandidateSize;
    previous_token_end = candidate.token_end;
  }

  SocketFD fd;
  if (!ConnectTCP(addr, &fd, error_message)) {
    return false;
  }
  if (!WriteP2PHeader(fd.get(), kP2PMessageLookupPrefix, request_id,
                      payload.size(),
                      ChecksumIEEE(payload.data(), payload.size())) ||
      !SendAll(fd.get(), payload.data(), payload.size())) {
    if (error_message != nullptr && error_message->empty()) {
      *error_message = "prefix lookup send failed";
    }
    return false;
  }
  return ReadPrefixLookupReply(fd.get(), request_id, candidates, result,
                               error_message);
}

bool ReleaseP2PPrefixLease(const std::string &addr, uint64_t lease_id,
                           std::string *error_message) {
  SocketFD fd;
  if (!ConnectTCP(addr, &fd, error_message)) {
    return false;
  }
  if (!WriteP2PHeader(fd.get(), kP2PMessageReleasePrefixLease, lease_id, 0,
                      0)) {
    if (error_message != nullptr && error_message->empty()) {
      *error_message = "prefix lease release send failed";
    }
    return false;
  }
  return ReadP2PReply(fd.get(), lease_id, error_message);
}

bool ProbeEndpoint(const std::string &addr) {
  SocketFD fd;
  std::string ignored;
  return ConnectTCP(addr, &fd, &ignored);
}

} // namespace

KVCacheConnector::KVCacheConnector(KVCacheConnectorOptions options)
    : options_(std::move(options)), next_seq_(1), connected_(false) {}

KVCacheConnector::~KVCacheConnector() { Close(); }

bool KVCacheConnector::Connect() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (connected_) {
    return true;
  }
  if (!options_.rdma_addr.empty() && ProbeEndpoint(options_.rdma_addr)) {
    connected_ = true;
    return true;
  }
  if (options_.enable_p2p_fallback && !options_.p2p_fallback_addr.empty() &&
      ProbeEndpoint(options_.p2p_fallback_addr)) {
    connected_ = true;
    return true;
  }
  return false;
}

void KVCacheConnector::Close() {
  std::lock_guard<std::mutex> lock(mutex_);
  connected_ = false;
}

uint64_t KVCacheConnector::NextSeq() {
  uint64_t seq = next_seq_.fetch_add(1, std::memory_order_relaxed);
  if (seq == 0) {
    seq = next_seq_.fetch_add(1, std::memory_order_relaxed);
  }
  return seq;
}

bool KVCacheConnector::PutBlock(uint64_t block_id, const void *data,
                                uint64_t length, KVCacheBlockMeta *meta) {
  if (block_id == 0 || data == nullptr || length == 0) {
    std::cerr << "[KVCacheConnector] Invalid block id, data, or length."
              << std::endl;
    return false;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!connected_) {
      std::cerr << "[KVCacheConnector] Connector is not connected."
                << std::endl;
      return false;
    }
  }

  const uint64_t seq = NextSeq();
  const uint32_t checksum = ChecksumIEEE(data, length);
  std::string error_message;
  std::string transport;

  bool ok = false;
  if (!options_.rdma_addr.empty()) {
    ok = SendRDMABlock(options_.rdma_addr, seq, block_id, data, length,
                       checksum, options_.wait_for_ack, &error_message);
    if (ok) {
      transport = "rdma";
    }
  }

  if (!ok && options_.enable_p2p_fallback &&
      !options_.p2p_fallback_addr.empty()) {
    if (!error_message.empty()) {
      std::cerr << "[KVCacheConnector] RDMA write failed for block " << block_id
                << ": " << error_message << "; trying P2P fallback."
                << std::endl;
      error_message.clear();
    }
    ok = SendP2PFallbackBlock(options_.p2p_fallback_addr, seq, block_id, data,
                              length, checksum, options_.wait_for_ack,
                              &error_message);
    if (ok) {
      transport = "p2p_fallback";
    }
  }

  if (!ok) {
    if (!error_message.empty()) {
      std::cerr << "[KVCacheConnector] Go daemon rejected block " << block_id
                << ": " << error_message << std::endl;
    }
    return false;
  }

  if (meta != nullptr) {
    meta->seq = seq;
    meta->block_id = block_id;
    meta->transport = transport;
    meta->length = length;
    meta->checksum = checksum;
  }
  return true;
}

bool KVCacheConnector::PutCacheObject(const KVCacheChunkKey &key,
                                      const void *data, uint64_t length,
                                      KVCacheBlockMeta *meta) {
  if (!options_.wait_for_ack) {
    std::cerr << "[KVCacheConnector] PutCacheObject requires ACK before "
                 "publishing semantic metadata."
              << std::endl;
    return false;
  }
  if (key.block_id == 0 || key.key.token_count == 0 ||
      key.key.token_count != key.token_end ||
      GetLE64(key.object_id.data()) != key.block_id) {
    std::cerr << "[KVCacheConnector] Invalid cache object key." << std::endl;
    return false;
  }
  KVCacheBlockMeta committed_meta;
  if (!PutBlock(key.block_id, data, length, &committed_meta)) {
    return false;
  }
  if (options_.p2p_fallback_addr.empty()) {
    std::cerr << "[KVCacheConnector] P2P address is required to commit "
                 "cache metadata."
              << std::endl;
    return false;
  }
  std::string error_message;
  if (!CommitP2PCacheObject(options_.p2p_fallback_addr, key, committed_meta,
                            &error_message)) {
    if (!error_message.empty()) {
      std::cerr << "[KVCacheConnector] Failed to commit cache object "
                << KVCacheDigestHex(key.object_id) << ": " << error_message
                << std::endl;
    }
    return false;
  }
  if (meta != nullptr) {
    *meta = std::move(committed_meta);
  }
  return true;
}

bool KVCacheConnector::GetBlock(uint64_t block_id, std::vector<uint8_t> *data,
                                KVCacheBlockMeta *meta) {
  return LookupBlock(block_id, data, meta) == KVCacheLookupResult::kFound;
}

KVCacheLookupResult KVCacheConnector::LookupBlock(uint64_t block_id,
                                                  std::vector<uint8_t> *data,
                                                  KVCacheBlockMeta *meta) {
  if (block_id == 0 || data == nullptr) {
    std::cerr << "[KVCacheConnector] Invalid block id or output buffer."
              << std::endl;
    return KVCacheLookupResult::kError;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!connected_) {
      std::cerr << "[KVCacheConnector] Connector is not connected."
                << std::endl;
      return KVCacheLookupResult::kError;
    }
  }
  if (options_.p2p_fallback_addr.empty()) {
    std::cerr << "[KVCacheConnector] P2P address is empty; cannot get block."
              << std::endl;
    return KVCacheLookupResult::kError;
  }

  std::string error_message;
  if (!FetchP2PBlock(options_.p2p_fallback_addr, block_id, data, meta,
                     &error_message)) {
    if (error_message == kP2PBlockNotFoundError) {
      return KVCacheLookupResult::kNotFound;
    }
    if (!error_message.empty()) {
      std::cerr << "[KVCacheConnector] Failed to get block " << block_id << ": "
                << error_message << std::endl;
    }
    return KVCacheLookupResult::kError;
  }
  return KVCacheLookupResult::kFound;
}

KVCacheLookupResult
KVCacheConnector::LookupPrefix(const KVCacheDigest &scope_digest,
                               const std::vector<KVCacheChunkKey> &candidates,
                               KVCachePrefixLookup *result) {
  if (result == nullptr || IsZeroDigest(scope_digest)) {
    std::cerr << "[KVCacheConnector] Invalid prefix lookup output or scope."
              << std::endl;
    return KVCacheLookupResult::kError;
  }
  *result = KVCachePrefixLookup{};
  if (candidates.empty()) {
    result->stop_reason = KVCachePrefixStopReason::kNotFound;
    return KVCacheLookupResult::kNotFound;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!connected_) {
      std::cerr << "[KVCacheConnector] Connector is not connected."
                << std::endl;
      return KVCacheLookupResult::kError;
    }
  }
  if (options_.p2p_fallback_addr.empty()) {
    std::cerr << "[KVCacheConnector] P2P address is required for prefix "
                 "lookup."
              << std::endl;
    return KVCacheLookupResult::kError;
  }
  std::string error_message;
  const uint64_t request_id = NextSeq();
  if (!LookupP2PPrefix(options_.p2p_fallback_addr, request_id, scope_digest,
                       candidates, result, &error_message)) {
    if (!error_message.empty()) {
      std::cerr << "[KVCacheConnector] Prefix lookup failed: " << error_message
                << std::endl;
    }
    return KVCacheLookupResult::kError;
  }
  return result->entries.empty() ? KVCacheLookupResult::kNotFound
                                 : KVCacheLookupResult::kFound;
}

bool KVCacheConnector::LoadPrefixEntry(const KVCachePrefixLocation &entry,
                                       std::vector<uint8_t> *data,
                                       KVCacheBlockMeta *meta) {
  if (entry.block_id == 0 || entry.length == 0 || entry.address.empty() ||
      data == nullptr || GetLE64(entry.object_id.data()) != entry.block_id) {
    std::cerr << "[KVCacheConnector] Invalid prefix location." << std::endl;
    return false;
  }
  KVCacheBlockMeta loaded_meta;
  std::string error_message;
  if (!FetchP2PBlock(entry.address, entry.block_id, data, &loaded_meta,
                     &error_message)) {
    if (!error_message.empty()) {
      std::cerr << "[KVCacheConnector] Failed to load prefix block "
                << entry.block_id << ": " << error_message << std::endl;
    }
    return false;
  }
  if (loaded_meta.length != entry.length ||
      loaded_meta.checksum != entry.checksum) {
    std::cerr << "[KVCacheConnector] Loaded prefix block metadata "
                 "mismatch."
              << std::endl;
    data->clear();
    return false;
  }
  if (meta != nullptr) {
    *meta = std::move(loaded_meta);
  }
  return true;
}

bool KVCacheConnector::ReleasePrefixLease(uint64_t lease_id) {
  if (lease_id == 0) {
    return true;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!connected_) {
      return false;
    }
  }
  std::string error_message;
  if (!ReleaseP2PPrefixLease(options_.p2p_fallback_addr, lease_id,
                             &error_message)) {
    if (!error_message.empty()) {
      std::cerr << "[KVCacheConnector] Failed to release prefix lease "
                << lease_id << ": " << error_message << std::endl;
    }
    return false;
  }
  return true;
}

bool KVCacheConnector::DeleteBlock(uint64_t block_id) {
  if (block_id == 0) {
    std::cerr << "[KVCacheConnector] Invalid block id." << std::endl;
    return false;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!connected_) {
      std::cerr << "[KVCacheConnector] Connector is not connected."
                << std::endl;
      return false;
    }
  }
  if (options_.p2p_fallback_addr.empty()) {
    std::cerr << "[KVCacheConnector] P2P address is empty; cannot delete block."
              << std::endl;
    return false;
  }

  std::string error_message;
  if (!DeleteP2PBlock(options_.p2p_fallback_addr, block_id, &error_message)) {
    if (!error_message.empty()) {
      std::cerr << "[KVCacheConnector] Failed to delete block " << block_id
                << ": " << error_message << std::endl;
    }
    return false;
  }
  return true;
}

bool KVCacheConnector::connected() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return connected_;
}
