#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <vector>

constexpr uint16_t kKVCacheKeySchemaVersion = 1;

using KVCacheDigest = std::array<uint8_t, 32>;

// KVCacheLayout identifies the byte representation of one cached KV chunk.
// The MVP stores K and V for all layers in one object.
struct KVCacheLayout {
    uint16_t version = 1;
    std::string dtype = "fp32";
    uint32_t layers = 1;
    uint32_t heads = 8;
    uint32_t head_dim = 16;
    uint32_t tp_world_size = 1;
    uint32_t tp_rank = 0;
};

// KVCacheScope contains every non-token input that can change the KV bytes.
// Placement, storage tier, checksum, and transport deliberately stay outside.
struct KVCacheScope {
    uint16_t version = kKVCacheKeySchemaVersion;
    std::string cache_namespace = "default";
    std::string model_id;
    std::string model_revision;
    std::string adapter_id;
    uint32_t chunk_size = 16;
    KVCacheLayout layout;
};

struct KVCacheKey {
    KVCacheDigest scope_digest {};
    KVCacheDigest prefix_digest {};
    uint64_t token_count = 0;
};

// KVCacheChunkKey bridges semantic identity to the existing uint64 BlockID
// understood by ZeroKV's physical storage and transport layers.
struct KVCacheChunkKey {
    KVCacheKey key;
    KVCacheDigest object_id {};
    uint64_t block_id = 0;
    uint64_t token_begin = 0;
    uint64_t token_end = 0;
};

bool BuildKVCacheKeys(const KVCacheScope& scope,
                      const std::vector<uint32_t>& tokens,
                      std::vector<KVCacheChunkKey>* keys,
                      std::string* error_message = nullptr);

std::string KVCacheDigestHex(const KVCacheDigest& digest);
