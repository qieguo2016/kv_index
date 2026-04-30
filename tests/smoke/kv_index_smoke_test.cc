#include "kv_index/kv_index.h"
#include "test_support/test_macros.h"

#include <cstdint>

int main() {
  kv_index::ForwardIndex index(kv_index::ForwardIndexOptions{});

  const auto missing = index.Get(12345);
  KV_INDEX_CHECK(!missing.has_value());

  const kv_index::LoadRequest request{
      .artifact_uri = "file:///tmp/kv_index/nonexistent-artifact",
      .artifact_id = "smoke-artifact",
  };
  const kv_index::LoadId load_id = index.LoadAsync(request);
  KV_INDEX_CHECK_NE(load_id, kv_index::kInvalidLoadId);

  const kv_index::LoadState state = index.GetLoadState(load_id);
  KV_INDEX_CHECK_EQ(state.id, load_id);
  KV_INDEX_CHECK_EQ(state.code, kv_index::LoadStateCode::kFailed);
  KV_INDEX_CHECK(state.terminal);
  KV_INDEX_CHECK(!state.message.empty());

  return 0;
}
