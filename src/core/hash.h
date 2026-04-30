#ifndef KV_INDEX_SRC_CORE_HASH_H_
#define KV_INDEX_SRC_CORE_HASH_H_

#include <cstdint>

namespace kv_index::core {

std::uint64_t StableHash64(std::uint64_t primary_key, std::uint64_t seed,
                           std::uint32_t version) noexcept;

}  // namespace kv_index::core

#endif  // KV_INDEX_SRC_CORE_HASH_H_
