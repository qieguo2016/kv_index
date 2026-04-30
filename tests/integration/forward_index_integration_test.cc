#include "kv_index/forward_index.h"
#include "test_support/test_macros.h"

#include <cstdint>
#include <vector>

int main() {
  kv_index::ForwardIndexOptions options;
  options.shard_count = 4;
  options.hash_seed = 20260426;

  kv_index::ForwardIndex index(options);
  const std::vector<std::uint64_t> keys = {10, 11, 12, 13, 10};

  const auto first_results = index.MGet(keys);
  const auto second_results = index.MGet(keys);

  KV_INDEX_CHECK_EQ(first_results.size(), keys.size());
  KV_INDEX_CHECK_EQ(second_results.size(), keys.size());
  for (std::size_t i = 0; i < keys.size(); ++i) {
    KV_INDEX_CHECK_EQ(first_results[i].has_value(),
                      second_results[i].has_value());
    KV_INDEX_CHECK_LT(index.ShardFor(keys[i]), options.shard_count);
  }

  const kv_index::LoadId first_load =
      index.LoadAsync(kv_index::LoadRequest{.artifact_uri = "artifact-a"});
  const kv_index::LoadId second_load =
      index.LoadAsync(kv_index::LoadRequest{.artifact_uri = "artifact-b"});

  KV_INDEX_CHECK_NE(first_load, kv_index::kInvalidLoadId);
  KV_INDEX_CHECK_NE(second_load, kv_index::kInvalidLoadId);
  KV_INDEX_CHECK_NE(first_load, second_load);
  KV_INDEX_CHECK(!index.CancelLoad(first_load));

  return 0;
}
