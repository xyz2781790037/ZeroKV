#include "cache_key.h"

#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

namespace {

bool ExpectEqual(const std::string& name,
                 const std::string& got,
                 const std::string& want) {
    if (got == want) {
        return true;
    }
    std::cerr << name << " = " << got << ", want " << want << "\n";
    return false;
}

}  // namespace

int main() {
    KVCacheScope scope;
    scope.cache_namespace = "tenant-a";
    scope.model_id = "qwen2.5-7b";
    scope.model_revision = "abc123";
    scope.adapter_id = "lora-a";
    scope.chunk_size = 4;
    scope.layout.version = 1;
    scope.layout.dtype = "fp32";
    scope.layout.layers = 2;
    scope.layout.heads = 4;
    scope.layout.head_dim = 8;
    scope.layout.tp_world_size = 1;
    scope.layout.tp_rank = 0;

    std::vector<KVCacheChunkKey> keys;
    std::string error;
    if (!BuildKVCacheKeys(scope, {101, 2023, 2003, 1037, 3231, 102},
                          &keys, &error)) {
        std::cerr << "BuildKVCacheKeys failed: " << error << "\n";
        return 1;
    }
    if (keys.size() != 2) {
        std::cerr << "key count = " << keys.size() << ", want 2\n";
        return 1;
    }

    bool ok = true;
    ok = ExpectEqual(
             "scope",
             KVCacheDigestHex(keys[0].key.scope_digest),
             "965a666ddfbc9f1440089bced64913d9ed8d8da784028617ea75042c34b94623") &&
         ok;
    ok = ExpectEqual(
             "prefix[0]",
             KVCacheDigestHex(keys[0].key.prefix_digest),
             "656e4d82346551ff6379c043819b1f3a947abea0f85fb64f8b14c42762675731") &&
         ok;
    ok = ExpectEqual(
             "object[0]",
             KVCacheDigestHex(keys[0].object_id),
             "94e8969feafa7b823322771145804d1e37df2a73ac9a47184357aa40c92eb7b7") &&
         ok;
    ok = ExpectEqual(
             "prefix[1]",
             KVCacheDigestHex(keys[1].key.prefix_digest),
             "f31e8f16663b1bccae2680d7a6632d2a4f18c78a57243c022205a66fd2622148") &&
         ok;
    ok = ExpectEqual(
             "object[1]",
             KVCacheDigestHex(keys[1].object_id),
             "761c050c27930384a5e85e59274c21905366d39480b94390a50bc5f183009b0f") &&
         ok;
    if (keys[0].block_id != 9402384532672800916ULL ||
        keys[1].block_id != 9512608633851288694ULL) {
        std::cerr << "unexpected block IDs: " << keys[0].block_id << ", "
                  << keys[1].block_id << "\n";
        ok = false;
    }

    std::vector<KVCacheChunkKey> other;
    if (!BuildKVCacheKeys(scope, {101, 2023, 2003, 1037, 9999, 102},
                          &other, &error)) {
        std::cerr << "second BuildKVCacheKeys failed: " << error << "\n";
        return 1;
    }
    if (keys[0].object_id != other[0].object_id ||
        keys[1].object_id == other[1].object_id) {
        std::cerr << "chained prefix identity invariant failed\n";
        ok = false;
    }
    return ok ? 0 : 1;
}
