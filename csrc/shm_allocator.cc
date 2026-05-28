#include "shm_allocator.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <limits>
#include <utility>

namespace {

constexpr uint64_t kAllocationAlignment = 64;
constexpr uint64_t kMaxShmNameLen = 255;

uint64_t AlignUp(uint64_t value, uint64_t alignment) {
    uint64_t rem = value % alignment;
    if (rem == 0) {
        return value;
    }
    return value + (alignment - rem);
}

bool ContainsNul(const std::string& value) {
    return value.find('\0') != std::string::npos;
}

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

}  // namespace

ShmAllocator::ShmAllocator(std::string shm_name, uint64_t size)
    : shm_name_(std::move(shm_name)),
      size_(size),
      cursor_(0),
      fd_(-1),
      base_(nullptr),
      initialized_(false) {}

ShmAllocator::~ShmAllocator() {
    Close();
}

bool ShmAllocator::NormalizeName(const std::string& name) {
    if (name.empty() || ContainsNul(name)) {
        std::cerr << "[ShmAllocator] Invalid shared memory name." << std::endl;
        return false;
    }

    std::string normalized = name;
    if (!normalized.empty() && normalized[0] == '/') {
        normalized.erase(0, 1);
    }
    if (normalized.empty() || normalized == "." || normalized == ".." ||
        normalized.size() > kMaxShmNameLen ||
        normalized.find('/') != std::string::npos) {
        std::cerr << "[ShmAllocator] Invalid shared memory name: " << name
                  << std::endl;
        return false;
    }

    shm_name_ = normalized;
    posix_name_ = "/" + normalized;
    return true;
}

bool ShmAllocator::Init() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (initialized_) {
        return true;
    }
    if (size_ == 0) {
        std::cerr << "[ShmAllocator] Shared memory size cannot be zero."
                  << std::endl;
        return false;
    }
    if (!NormalizeName(shm_name_)) {
        return false;
    }
    if (size_ > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
        std::cerr << "[ShmAllocator] Shared memory size is too large."
                  << std::endl;
        return false;
    }

    fd_ = shm_open(posix_name_.c_str(), O_CREAT | O_RDWR, 0660);
    if (fd_ == -1) {
        int err = errno;
        std::cerr << "[ShmAllocator] shm_open failed for " << posix_name_
                  << ": " << std::strerror(err) << std::endl;
        return false;
    }

    if (ftruncate(fd_, static_cast<off_t>(size_)) == -1) {
        int err = errno;
        std::cerr << "[ShmAllocator] ftruncate failed for " << posix_name_
                  << ": " << std::strerror(err) << std::endl;
        close(fd_);
        fd_ = -1;
        shm_unlink(posix_name_.c_str());
        return false;
    }

    void* mapped =
        mmap(nullptr, static_cast<size_t>(size_), PROT_READ | PROT_WRITE,
             MAP_SHARED, fd_, 0);
    if (mapped == MAP_FAILED) {
        int err = errno;
        std::cerr << "[ShmAllocator] mmap failed for " << posix_name_ << ": "
                  << std::strerror(err) << std::endl;
        close(fd_);
        fd_ = -1;
        shm_unlink(posix_name_.c_str());
        return false;
    }

    base_ = static_cast<uint8_t*>(mapped);
    cursor_ = 0;
    initialized_ = true;
    return true;
}

void ShmAllocator::Close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (base_ != nullptr) {
        if (munmap(base_, static_cast<size_t>(size_)) == -1) {
            int err = errno;
            std::cerr << "[ShmAllocator] munmap failed for " << posix_name_
                      << ": " << std::strerror(err) << std::endl;
        }
        base_ = nullptr;
    }
    if (fd_ != -1) {
        close(fd_);
        fd_ = -1;
    }
    if (!posix_name_.empty()) {
        if (shm_unlink(posix_name_.c_str()) == -1 && errno != ENOENT) {
            int err = errno;
            std::cerr << "[ShmAllocator] shm_unlink failed for " << posix_name_
                      << ": " << std::strerror(err) << std::endl;
        }
    }
    initialized_ = false;
    cursor_ = 0;
}

bool ShmAllocator::Allocate(uint64_t length, ShmAllocation* allocation) {
    if (allocation == nullptr) {
        std::cerr << "[ShmAllocator] Nil allocation output." << std::endl;
        return false;
    }
    if (length == 0) {
        std::cerr << "[ShmAllocator] Allocation length cannot be zero."
                  << std::endl;
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_ || base_ == nullptr) {
        std::cerr << "[ShmAllocator] Allocator is not initialized."
                  << std::endl;
        return false;
    }

    uint64_t offset = AlignUp(cursor_, kAllocationAlignment);
    if (offset > size_ || length > size_ - offset) {
        std::cerr << "[ShmAllocator] Shared memory pool exhausted: requested="
                  << length << " available=" << (size_ - cursor_) << std::endl;
        return false;
    }

    cursor_ = offset + length;
    allocation->shm_name = shm_name_;
    allocation->offset = offset;
    allocation->length = length;
    allocation->checksum = 0;
    allocation->data = base_ + offset;
    return true;
}

bool ShmAllocator::Write(const void* data,
                         uint64_t length,
                         ShmAllocation* allocation) {
    if (data == nullptr) {
        std::cerr << "[ShmAllocator] Nil source data." << std::endl;
        return false;
    }
    if (length == 0) {
        std::cerr << "[ShmAllocator] Allocation length cannot be zero."
                  << std::endl;
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_ || base_ == nullptr) {
        std::cerr << "[ShmAllocator] Allocator is not initialized."
                  << std::endl;
        return false;
    }

    uint64_t offset = AlignUp(cursor_, kAllocationAlignment);
    if (offset > size_ || length > size_ - offset) {
        std::cerr << "[ShmAllocator] Shared memory pool exhausted: requested="
                  << length << " available=" << (size_ - cursor_) << std::endl;
        return false;
    }

    ShmAllocation out;
    out.shm_name = shm_name_;
    out.offset = offset;
    out.length = length;
    out.checksum = 0;
    out.data = base_ + offset;

    std::memcpy(out.data, data, static_cast<size_t>(length));
    out.checksum = ChecksumIEEE(out.data, length);
    cursor_ = offset + length;
    if (allocation != nullptr) {
        *allocation = out;
    }
    return true;
}

void ShmAllocator::Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    cursor_ = 0;
}

uint64_t ShmAllocator::used() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cursor_;
}

bool ShmAllocator::initialized() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return initialized_;
}

uint32_t ShmAllocator::ChecksumIEEE(const void* data, uint64_t length) {
    if (data == nullptr || length == 0) {
        return 0;
    }

    const auto& table = Crc32Table();
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFFu;
    for (uint64_t i = 0; i < length; ++i) {
        crc = table[(crc ^ bytes[i]) & 0xFFu] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}
