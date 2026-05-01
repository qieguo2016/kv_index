#ifndef KV_INDEX_SRC_STORE_FROZEN_PRIMARY_KEY_INDEX_H_
#define KV_INDEX_SRC_STORE_FROZEN_PRIMARY_KEY_INDEX_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "kv_index/status.h"

namespace kv_index::internal::store {

struct FrozenPrimaryKeyIndexEntry {
  std::uint64_t primary_key = 0;
  std::uint64_t row_offset = 0;
};

struct FrozenPrimaryKeyIndexBuildOptions {
  std::uint64_t hash_seed = 0;
  std::uint32_t hash_version = 1;
  std::uint32_t group_width = 16;
};

struct FrozenPrimaryKeyIndexMetadata {
  std::uint64_t row_count = 0;
  std::uint64_t capacity = 0;
  std::uint64_t hash_seed = 0;
  std::uint32_t hash_version = 0;
  std::uint32_t group_width = 0;
  std::uint64_t control_offset = 0;
  std::uint64_t key_offset = 0;
  std::uint64_t row_offset_offset = 0;
  std::uint64_t total_size = 0;
};

class FrozenPrimaryKeyIndexView {
 public:
  static constexpr std::uint64_t kMagic = 0x315844494b50564bULL;
  static constexpr std::uint32_t kFormatVersion = 1;
  static constexpr std::uint32_t kDefaultGroupWidth = 16;

  static constexpr std::size_t kMagicOffset = 0;
  static constexpr std::size_t kFormatVersionOffset = 8;
  static constexpr std::size_t kHeaderSizeOffset = 12;
  static constexpr std::size_t kRowCountOffset = 16;
  static constexpr std::size_t kCapacityOffset = 24;
  static constexpr std::size_t kHashSeedOffset = 32;
  static constexpr std::size_t kHashVersionOffset = 40;
  static constexpr std::size_t kGroupWidthOffset = 44;
  static constexpr std::size_t kControlOffsetOffset = 48;
  static constexpr std::size_t kKeyOffsetOffset = 56;
  static constexpr std::size_t kRowOffsetOffsetOffset = 64;
  static constexpr std::size_t kTotalSizeOffset = 72;
  static constexpr std::size_t kSerializedHeaderSize = 80;

  static StatusOr<FrozenPrimaryKeyIndexView> Validate(
      std::span<const std::byte> bytes);

  StatusOr<std::optional<std::uint64_t>> Lookup(
      std::uint64_t primary_key) const;
  StatusOr<std::vector<FrozenPrimaryKeyIndexEntry>> Entries() const;

  const FrozenPrimaryKeyIndexMetadata& metadata() const noexcept {
    return metadata_;
  }

 private:
  FrozenPrimaryKeyIndexView(std::span<const std::byte> bytes,
                            FrozenPrimaryKeyIndexMetadata metadata);

  StatusOr<std::uint64_t> ReadKey(std::uint64_t slot) const;
  StatusOr<std::uint64_t> ReadRowOffset(std::uint64_t slot) const;

  std::span<const std::byte> bytes_;
  FrozenPrimaryKeyIndexMetadata metadata_;
};

StatusOr<std::vector<std::byte>> BuildFrozenPrimaryKeyIndex(
    std::span<const FrozenPrimaryKeyIndexEntry> entries,
    FrozenPrimaryKeyIndexBuildOptions options = {});

}  // namespace kv_index::internal::store

#endif  // KV_INDEX_SRC_STORE_FROZEN_PRIMARY_KEY_INDEX_H_
