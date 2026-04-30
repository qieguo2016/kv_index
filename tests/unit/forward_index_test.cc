#include "kv_index/forward_index.h"
#include "kv_index/version.h"
#include "src/core/hash.h"
#include "test_support/test_macros.h"

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

void DefaultOptionsMatchDesign() {
  const kv_index::ForwardIndexOptions options;

  KV_INDEX_CHECK_EQ(options.shard_count, 128U);
  KV_INDEX_CHECK_EQ(options.hash_version, 1U);
  KV_INDEX_CHECK(!kv_index::VersionString().empty());
}

void RejectsInvalidShardCounts() {
  kv_index::ForwardIndexOptions zero_shards;
  zero_shards.shard_count = 0;
  KV_INDEX_CHECK_THROWS(
      [&] {
        kv_index::ForwardIndex index(zero_shards);
        (void)index;
      }(),
      std::invalid_argument);

  kv_index::ForwardIndexOptions non_power_of_two;
  non_power_of_two.shard_count = 3;
  KV_INDEX_CHECK_THROWS(
      [&] {
        kv_index::ForwardIndex index(non_power_of_two);
        (void)index;
      }(),
      std::invalid_argument);
}

void StableShardAssignmentIsBoundedAndRepeatable() {
  kv_index::ForwardIndexOptions options;
  options.shard_count = 8;
  options.hash_seed = 17;

  const kv_index::ForwardIndex first(options);
  const kv_index::ForwardIndex second(options);

  for (const std::uint64_t key : {0ULL, 1ULL, 42ULL, 999999ULL}) {
    KV_INDEX_CHECK_LT(first.ShardFor(key), options.shard_count);
    KV_INDEX_CHECK_EQ(first.ShardFor(key), second.ShardFor(key));
  }

  KV_INDEX_CHECK_EQ(first.ShardFor(42), 4U);
}

void StableHashGoldenValuesRemainStable() {
  KV_INDEX_CHECK_EQ(kv_index::StableHash64(0, 0, 1),
                    12085254679833835651ULL);
  KV_INDEX_CHECK_EQ(kv_index::StableHash64(1, 0, 1),
                    10858953248184931039ULL);
  KV_INDEX_CHECK_EQ(kv_index::StableHash64(42, 17, 1),
                    13083006257041843452ULL);
  KV_INDEX_CHECK_EQ(kv_index::core::StableHash64(42, 17, 1),
                    kv_index::StableHash64(42, 17, 1));
}

void EmptyIndexMissesPreserveMGetOrderAndShape() {
  const kv_index::ForwardIndex index(kv_index::ForwardIndexOptions{});
  const std::vector<std::uint64_t> keys = {9, 1, 9, 128, 2};

  const auto rows = index.MGet(keys);

  KV_INDEX_CHECK_EQ(rows.size(), keys.size());
  for (const auto& row : rows) {
    KV_INDEX_CHECK(!row.has_value());
  }
}

}  // namespace

int main() {
  DefaultOptionsMatchDesign();
  RejectsInvalidShardCounts();
  StableShardAssignmentIsBoundedAndRepeatable();
  StableHashGoldenValuesRemainStable();
  EmptyIndexMissesPreserveMGetOrderAndShape();
  return 0;
}
