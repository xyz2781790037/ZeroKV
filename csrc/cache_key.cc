#include "cache_key.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <iomanip>
#include <limits>
#include <sstream>
#include <utility>

namespace {

constexpr char kScopeDomain[] = "zerokv.kvcache.scope.v1";
constexpr char kPrefixDomain[] = "zerokv.kvcache.prefix.v1";
constexpr char kObjectDomain[] = "zerokv.kvcache.object.v1";

constexpr std::array<uint32_t, 64> kSHA256Constants = {
    0x428a2f98u, 0x71374491u, 0xb5c0fbcfu, 0xe9b5dba5u,
    0x3956c25bu, 0x59f111f1u, 0x923f82a4u, 0xab1c5ed5u,
    0xd807aa98u, 0x12835b01u, 0x243185beu, 0x550c7dc3u,
    0x72be5d74u, 0x80deb1feu, 0x9bdc06a7u, 0xc19bf174u,
    0xe49b69c1u, 0xefbe4786u, 0x0fc19dc6u, 0x240ca1ccu,
    0x2de92c6fu, 0x4a7484aau, 0x5cb0a9dcu, 0x76f988dau,
    0x983e5152u, 0xa831c66du, 0xb00327c8u, 0xbf597fc7u,
    0xc6e00bf3u, 0xd5a79147u, 0x06ca6351u, 0x14292967u,
    0x27b70a85u, 0x2e1b2138u, 0x4d2c6dfcu, 0x53380d13u,
    0x650a7354u, 0x766a0abbu, 0x81c2c92eu, 0x92722c85u,
    0xa2bfe8a1u, 0xa81a664bu, 0xc24b8b70u, 0xc76c51a3u,
    0xd192e819u, 0xd6990624u, 0xf40e3585u, 0x106aa070u,
    0x19a4c116u, 0x1e376c08u, 0x2748774cu, 0x34b0bcb5u,
    0x391c0cb3u, 0x4ed8aa4au, 0x5b9cca4fu, 0x682e6ff3u,
    0x748f82eeu, 0x78a5636fu, 0x84c87814u, 0x8cc70208u,
    0x90befffau, 0xa4506cebu, 0xbef9a3f7u, 0xc67178f2u,
};

uint32_t RotateRight(uint32_t value, uint32_t bits) {
    return (value >> bits) | (value << (32u - bits));
}

class SHA256 {
   public:
    SHA256()
        : state_ {0x6a09e667u, 0xbb67ae85u, 0x3c6ef372u, 0xa54ff53au,
                  0x510e527fu, 0x9b05688cu, 0x1f83d9abu, 0x5be0cd19u} {}

    void Update(const uint8_t* data, std::size_t length) {
        if (data == nullptr || length == 0) {
            return;
        }
        total_bytes_ += length;
        while (length > 0) {
            const std::size_t copied =
                std::min(length, block_.size() - block_size_);
            std::copy(data, data + copied, block_.begin() + block_size_);
            block_size_ += copied;
            data += copied;
            length -= copied;
            if (block_size_ == block_.size()) {
                Transform(block_.data());
                block_size_ = 0;
            }
        }
    }

    KVCacheDigest Final() {
        const uint64_t bit_length = total_bytes_ * 8u;
        block_[block_size_++] = 0x80u;
        if (block_size_ > 56) {
            std::fill(block_.begin() + block_size_, block_.end(), 0);
            Transform(block_.data());
            block_size_ = 0;
        }
        std::fill(block_.begin() + block_size_, block_.begin() + 56, 0);
        for (int i = 0; i < 8; ++i) {
            block_[56 + i] =
                static_cast<uint8_t>(bit_length >> (8 * (7 - i)));
        }
        Transform(block_.data());

        KVCacheDigest digest {};
        for (std::size_t i = 0; i < state_.size(); ++i) {
            digest[i * 4] = static_cast<uint8_t>(state_[i] >> 24);
            digest[i * 4 + 1] = static_cast<uint8_t>(state_[i] >> 16);
            digest[i * 4 + 2] = static_cast<uint8_t>(state_[i] >> 8);
            digest[i * 4 + 3] = static_cast<uint8_t>(state_[i]);
        }
        return digest;
    }

   private:
    void Transform(const uint8_t* block) {
        std::array<uint32_t, 64> words {};
        for (std::size_t i = 0; i < 16; ++i) {
            words[i] = (static_cast<uint32_t>(block[i * 4]) << 24) |
                       (static_cast<uint32_t>(block[i * 4 + 1]) << 16) |
                       (static_cast<uint32_t>(block[i * 4 + 2]) << 8) |
                       static_cast<uint32_t>(block[i * 4 + 3]);
        }
        for (std::size_t i = 16; i < words.size(); ++i) {
            const uint32_t s0 = RotateRight(words[i - 15], 7) ^
                                RotateRight(words[i - 15], 18) ^
                                (words[i - 15] >> 3);
            const uint32_t s1 = RotateRight(words[i - 2], 17) ^
                                RotateRight(words[i - 2], 19) ^
                                (words[i - 2] >> 10);
            words[i] = words[i - 16] + s0 + words[i - 7] + s1;
        }

        uint32_t a = state_[0];
        uint32_t b = state_[1];
        uint32_t c = state_[2];
        uint32_t d = state_[3];
        uint32_t e = state_[4];
        uint32_t f = state_[5];
        uint32_t g = state_[6];
        uint32_t h = state_[7];
        for (std::size_t i = 0; i < words.size(); ++i) {
            const uint32_t sigma1 =
                RotateRight(e, 6) ^ RotateRight(e, 11) ^ RotateRight(e, 25);
            const uint32_t choose = (e & f) ^ ((~e) & g);
            const uint32_t temp1 =
                h + sigma1 + choose + kSHA256Constants[i] + words[i];
            const uint32_t sigma0 =
                RotateRight(a, 2) ^ RotateRight(a, 13) ^ RotateRight(a, 22);
            const uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
            const uint32_t temp2 = sigma0 + majority;
            h = g;
            g = f;
            f = e;
            e = d + temp1;
            d = c;
            c = b;
            b = a;
            a = temp1 + temp2;
        }
        state_[0] += a;
        state_[1] += b;
        state_[2] += c;
        state_[3] += d;
        state_[4] += e;
        state_[5] += f;
        state_[6] += g;
        state_[7] += h;
    }

    std::array<uint32_t, 8> state_;
    std::array<uint8_t, 64> block_ {};
    std::size_t block_size_ = 0;
    uint64_t total_bytes_ = 0;
};

void SetError(std::string* error_message, const std::string& message) {
    if (error_message != nullptr) {
        *error_message = message;
    }
}

void AppendBytes(std::vector<uint8_t>* out,
                 const uint8_t* data,
                 std::size_t length) {
    out->insert(out->end(), data, data + length);
}

template <std::size_t N>
void AppendArray(std::vector<uint8_t>* out, const std::array<uint8_t, N>& value) {
    AppendBytes(out, value.data(), value.size());
}

void AppendLE16(std::vector<uint8_t>* out, uint16_t value) {
    out->push_back(static_cast<uint8_t>(value));
    out->push_back(static_cast<uint8_t>(value >> 8));
}

void AppendLE32(std::vector<uint8_t>* out, uint32_t value) {
    for (int i = 0; i < 4; ++i) {
        out->push_back(static_cast<uint8_t>(value >> (8 * i)));
    }
}

void AppendLE64(std::vector<uint8_t>* out, uint64_t value) {
    for (int i = 0; i < 8; ++i) {
        out->push_back(static_cast<uint8_t>(value >> (8 * i)));
    }
}

bool AppendString(std::vector<uint8_t>* out,
                  const std::string& value,
                  std::string* error_message) {
    if (value.size() > std::numeric_limits<uint32_t>::max()) {
        SetError(error_message, "KV cache key string is too long");
        return false;
    }
    AppendLE32(out, static_cast<uint32_t>(value.size()));
    AppendBytes(out, reinterpret_cast<const uint8_t*>(value.data()), value.size());
    return true;
}

void AppendDomain(std::vector<uint8_t>* out, const char* domain, std::size_t n) {
    AppendBytes(out, reinterpret_cast<const uint8_t*>(domain), n);
    out->push_back(0);
}

KVCacheDigest Hash(const std::vector<uint8_t>& bytes) {
    SHA256 sha;
    sha.Update(bytes.data(), bytes.size());
    return sha.Final();
}

bool ValidateScope(const KVCacheScope& scope, std::string* error_message) {
    if (scope.version != kKVCacheKeySchemaVersion) {
        SetError(error_message, "unsupported KV cache key schema version");
        return false;
    }
    if (scope.cache_namespace.empty() || scope.model_id.empty() ||
        scope.model_revision.empty()) {
        SetError(error_message,
                 "namespace, model id, and model revision must be non-empty");
        return false;
    }
    if (scope.chunk_size == 0 || scope.layout.version == 0 ||
        scope.layout.dtype.empty() || scope.layout.layers == 0 ||
        scope.layout.heads == 0 || scope.layout.head_dim == 0 ||
        scope.layout.tp_world_size == 0 ||
        scope.layout.tp_rank >= scope.layout.tp_world_size) {
        SetError(error_message, "invalid KV cache chunk or layout configuration");
        return false;
    }
    return true;
}

bool BuildScopeDigest(const KVCacheScope& scope,
                      KVCacheDigest* digest,
                      std::string* error_message) {
    if (!ValidateScope(scope, error_message)) {
        return false;
    }
    std::vector<uint8_t> encoded;
    AppendDomain(&encoded, kScopeDomain, sizeof(kScopeDomain) - 1);
    AppendLE16(&encoded, scope.version);
    if (!AppendString(&encoded, scope.cache_namespace, error_message) ||
        !AppendString(&encoded, scope.model_id, error_message) ||
        !AppendString(&encoded, scope.model_revision, error_message) ||
        !AppendString(&encoded, scope.adapter_id, error_message)) {
        return false;
    }
    AppendLE32(&encoded, scope.chunk_size);
    AppendLE16(&encoded, scope.layout.version);
    if (!AppendString(&encoded, scope.layout.dtype, error_message)) {
        return false;
    }
    AppendLE32(&encoded, scope.layout.layers);
    AppendLE32(&encoded, scope.layout.heads);
    AppendLE32(&encoded, scope.layout.head_dim);
    AppendLE32(&encoded, scope.layout.tp_world_size);
    AppendLE32(&encoded, scope.layout.tp_rank);
    *digest = Hash(encoded);
    return true;
}

KVCacheDigest BuildPrefixDigest(const KVCacheDigest& scope,
                                const KVCacheDigest& parent,
                                const uint32_t* tokens,
                                std::size_t token_count) {
    std::vector<uint8_t> encoded;
    AppendDomain(&encoded, kPrefixDomain, sizeof(kPrefixDomain) - 1);
    AppendArray(&encoded, scope);
    AppendArray(&encoded, parent);
    AppendLE32(&encoded, static_cast<uint32_t>(token_count));
    for (std::size_t i = 0; i < token_count; ++i) {
        AppendLE32(&encoded, tokens[i]);
    }
    return Hash(encoded);
}

KVCacheDigest BuildObjectDigest(const KVCacheKey& key) {
    std::vector<uint8_t> encoded;
    AppendDomain(&encoded, kObjectDomain, sizeof(kObjectDomain) - 1);
    AppendArray(&encoded, key.scope_digest);
    AppendArray(&encoded, key.prefix_digest);
    AppendLE64(&encoded, key.token_count);
    return Hash(encoded);
}

uint64_t BlockIDFromDigest(const KVCacheDigest& digest) {
    uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value |= static_cast<uint64_t>(digest[i]) << (8 * i);
    }
    return value;
}

}  // namespace

bool BuildKVCacheKeys(const KVCacheScope& scope,
                      const std::vector<uint32_t>& tokens,
                      std::vector<KVCacheChunkKey>* keys,
                      std::string* error_message) {
    if (error_message != nullptr) {
        error_message->clear();
    }
    if (keys == nullptr) {
        SetError(error_message, "nil KV cache key output");
        return false;
    }
    keys->clear();
    if (tokens.empty()) {
        SetError(error_message, "empty token sequence");
        return false;
    }

    KVCacheDigest scope_digest {};
    if (!BuildScopeDigest(scope, &scope_digest, error_message)) {
        return false;
    }

    const std::size_t chunk_size = scope.chunk_size;
    keys->reserve((tokens.size() + chunk_size - 1) / chunk_size);
    KVCacheDigest parent {};
    for (std::size_t begin = 0; begin < tokens.size(); begin += chunk_size) {
        const std::size_t end = std::min(tokens.size(), begin + chunk_size);
        const KVCacheDigest prefix = BuildPrefixDigest(
            scope_digest, parent, tokens.data() + begin, end - begin);
        KVCacheKey key;
        key.scope_digest = scope_digest;
        key.prefix_digest = prefix;
        key.token_count = end;
        const KVCacheDigest object_id = BuildObjectDigest(key);
        const uint64_t block_id = BlockIDFromDigest(object_id);
        if (block_id == 0) {
            keys->clear();
            SetError(error_message, "derived zero block id");
            return false;
        }
        keys->push_back(KVCacheChunkKey{
            key,
            object_id,
            block_id,
            static_cast<uint64_t>(begin),
            static_cast<uint64_t>(end),
        });
        parent = prefix;
    }
    return true;
}

std::string KVCacheDigestHex(const KVCacheDigest& digest) {
    std::ostringstream out;
    out << std::hex << std::setfill('0');
    for (uint8_t byte : digest) {
        out << std::setw(2) << static_cast<unsigned>(byte);
    }
    return out.str();
}
