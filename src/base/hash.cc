#include "src/base/hash.h"

#include "kv_index/forward_index.h"

namespace kv_index::core {

namespace {

std::uint64_t SplitMix64(std::uint64_t value) noexcept {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

}  // namespace

std::uint64_t StableHash64(std::uint64_t primary_key, std::uint64_t seed,
                           std::uint32_t version) noexcept {
  const std::uint64_t version_mix =
      static_cast<std::uint64_t>(version) * 0xd6e8feb86659fd93ULL;
  return SplitMix64(primary_key ^ seed ^ version_mix);
}

}  // namespace kv_index::core

namespace kv_index {

std::uint64_t StableHash64(std::uint64_t primary_key, std::uint64_t seed,
                           std::uint32_t version) noexcept {
  return core::StableHash64(primary_key, seed, version);
}

}  // namespace kv_index
