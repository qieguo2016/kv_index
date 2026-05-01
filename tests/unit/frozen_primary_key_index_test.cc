#include "src/store/frozen_primary_key_index.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "src/base/hash.h"
#include "tests/test_support/test_macros.h"

namespace {

using kv_index::StatusCode;
using kv_index::internal::store::BuildFrozenPrimaryKeyIndex;
using kv_index::internal::store::FrozenPrimaryKeyIndexBuildOptions;
using kv_index::internal::store::FrozenPrimaryKeyIndexEntry;
using kv_index::internal::store::FrozenPrimaryKeyIndexView;

template <typename T>
void OverwriteLittleEndian(std::vector<std::byte>* bytes, std::size_t offset,
                           T value) {
  KV_INDEX_CHECK(bytes != nullptr);
  KV_INDEX_CHECK(offset <= bytes->size());
  KV_INDEX_CHECK(bytes->size() - offset >= sizeof(T));
  for (std::size_t i = 0; i < sizeof(T); ++i) {
    (*bytes)[offset + i] =
        static_cast<std::byte>((static_cast<std::uint64_t>(value) >> (i * 8)) &
                               0xffU);
  }
}

void EmptyIndexValidatesMetadataAndMisses() {
  const FrozenPrimaryKeyIndexBuildOptions options{
      .hash_seed = 0x123456789abcdef0ULL,
      .hash_version = 7,
  };

  const std::array<FrozenPrimaryKeyIndexEntry, 0> entries = {};
  auto bytes = BuildFrozenPrimaryKeyIndex(
      std::span<const FrozenPrimaryKeyIndexEntry>(entries.data(),
                                                  entries.size()),
      options);
  KV_INDEX_CHECK(bytes.ok());

  auto view = FrozenPrimaryKeyIndexView::Validate(
      std::span<const std::byte>(bytes->data(), bytes->size()));
  KV_INDEX_CHECK(view.ok());
  KV_INDEX_CHECK_EQ(view->metadata().row_count, 0U);
  KV_INDEX_CHECK_EQ(view->metadata().hash_seed, options.hash_seed);
  KV_INDEX_CHECK_EQ(view->metadata().hash_version, options.hash_version);
  KV_INDEX_CHECK_EQ(view->metadata().group_width, 16U);

  auto miss = view->Lookup(42);
  KV_INDEX_CHECK(miss.ok());
  KV_INDEX_CHECK(!miss->has_value());
}

void SingleAndMultiRowLookupsReturnRowOffsets() {
  const FrozenPrimaryKeyIndexBuildOptions options{
      .hash_seed = 11,
      .hash_version = 3,
  };
  const std::array<FrozenPrimaryKeyIndexEntry, 4> entries = {{
      {.primary_key = 100, .row_offset = 0},
      {.primary_key = 7, .row_offset = 56},
      {.primary_key = 999, .row_offset = 112},
      {.primary_key = 42, .row_offset = 168},
  }};

  auto bytes = BuildFrozenPrimaryKeyIndex(entries, options);
  KV_INDEX_CHECK(bytes.ok());
  auto view = FrozenPrimaryKeyIndexView::Validate(*bytes);
  KV_INDEX_CHECK(view.ok());
  KV_INDEX_CHECK_EQ(view->metadata().row_count, entries.size());

  for (const FrozenPrimaryKeyIndexEntry& entry : entries) {
    auto hit = view->Lookup(entry.primary_key);
    KV_INDEX_CHECK(hit.ok());
    KV_INDEX_CHECK(hit->has_value());
    KV_INDEX_CHECK_EQ(hit->value(), entry.row_offset);
  }

  auto miss = view->Lookup(123456);
  KV_INDEX_CHECK(miss.ok());
  KV_INDEX_CHECK(!miss->has_value());
}

void ProbeCollisionsStillFindEveryKey() {
  const FrozenPrimaryKeyIndexBuildOptions options{
      .hash_seed = 0x55aa,
      .hash_version = 5,
  };
  constexpr std::uint64_t kExpectedCapacity = 16;
  const std::uint64_t target_bucket =
      kv_index::internal::base::StableHash64(1, options.hash_seed,
                                   options.hash_version) &
      (kExpectedCapacity - 1);

  std::vector<FrozenPrimaryKeyIndexEntry> entries;
  for (std::uint64_t key = 1; entries.size() < 6; ++key) {
    const std::uint64_t bucket =
        kv_index::internal::base::StableHash64(key, options.hash_seed,
                                     options.hash_version) &
        (kExpectedCapacity - 1);
    if (bucket == target_bucket) {
      entries.push_back(FrozenPrimaryKeyIndexEntry{
          .primary_key = key,
          .row_offset = static_cast<std::uint64_t>(entries.size() * 64),
      });
    }
  }

  auto bytes = BuildFrozenPrimaryKeyIndex(entries, options);
  KV_INDEX_CHECK(bytes.ok());
  auto view = FrozenPrimaryKeyIndexView::Validate(*bytes);
  KV_INDEX_CHECK(view.ok());
  KV_INDEX_CHECK_EQ(view->metadata().capacity, kExpectedCapacity);

  for (const FrozenPrimaryKeyIndexEntry& entry : entries) {
    auto hit = view->Lookup(entry.primary_key);
    KV_INDEX_CHECK(hit.ok());
    KV_INDEX_CHECK(hit->has_value());
    KV_INDEX_CHECK_EQ(hit->value(), entry.row_offset);
  }
}

void DuplicatePrimaryKeysAreRejectedDuringBuild() {
  const std::array<FrozenPrimaryKeyIndexEntry, 2> entries = {{
      {.primary_key = 8, .row_offset = 0},
      {.primary_key = 8, .row_offset = 64},
  }};

  auto bytes = BuildFrozenPrimaryKeyIndex(entries);
  KV_INDEX_CHECK(!bytes.ok());
  KV_INDEX_CHECK_EQ(bytes.status().code(), StatusCode::kInvalidArgument);
}

void MalformedLayoutsAreRejectedByValidation() {
  const std::array<FrozenPrimaryKeyIndexEntry, 2> entries = {{
      {.primary_key = 1, .row_offset = 0},
      {.primary_key = 2, .row_offset = 64},
  }};
  auto bytes = BuildFrozenPrimaryKeyIndex(entries);
  KV_INDEX_CHECK(bytes.ok());

  std::vector<std::byte> bad_magic = *bytes;
  bad_magic[0] = static_cast<std::byte>(0x00);
  auto bad_magic_view = FrozenPrimaryKeyIndexView::Validate(bad_magic);
  KV_INDEX_CHECK(!bad_magic_view.ok());
  KV_INDEX_CHECK_EQ(bad_magic_view.status().code(),
                    StatusCode::kInvalidArgument);

  std::vector<std::byte> truncated = *bytes;
  truncated.pop_back();
  auto truncated_view = FrozenPrimaryKeyIndexView::Validate(truncated);
  KV_INDEX_CHECK(!truncated_view.ok());
  KV_INDEX_CHECK_EQ(truncated_view.status().code(),
                    StatusCode::kInvalidArgument);

  std::vector<std::byte> too_small_capacity = *bytes;
  OverwriteLittleEndian(&too_small_capacity,
                        FrozenPrimaryKeyIndexView::kCapacityOffset,
                        std::uint64_t{1});
  auto too_small_view =
      FrozenPrimaryKeyIndexView::Validate(too_small_capacity);
  KV_INDEX_CHECK(!too_small_view.ok());
  KV_INDEX_CHECK_EQ(too_small_view.status().code(),
                    StatusCode::kInvalidArgument);
}

}  // namespace

int main() {
  EmptyIndexValidatesMetadataAndMisses();
  SingleAndMultiRowLookupsReturnRowOffsets();
  ProbeCollisionsStillFindEveryKey();
  DuplicatePrimaryKeysAreRejectedDuringBuild();
  MalformedLayoutsAreRejectedByValidation();
  return 0;
}
