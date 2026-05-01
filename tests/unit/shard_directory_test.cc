#include "src/runtime/shard_directory.h"

#include <cstdint>
#include <memory>

#include "kv_index/status.h"
#include "src/runtime/shard_state.h"
#include "tests/test_support/test_macros.h"

namespace {

using kv_index::StatusCode;
using kv_index::internal::runtime::ShardDirectory;
using kv_index::internal::runtime::ShardState;

std::shared_ptr<const ShardState> MakeState(std::uint32_t shard_id,
                                            std::uint64_t generation) {
  return std::make_shared<const ShardState>(shard_id, generation);
}

void EmptyShardsLoadAsNull() {
  ShardDirectory directory(4);

  KV_INDEX_CHECK_EQ(directory.ShardCount(), 4U);
  auto loaded = directory.Load(2);
  KV_INDEX_CHECK(loaded.ok());
  KV_INDEX_CHECK(loaded->get() == nullptr);
}

void PublishReleaseStoresAndLoadAcquireLoadsState() {
  ShardDirectory directory(4);
  auto state = MakeState(2, 10);

  const auto status = directory.Publish(2, state);
  KV_INDEX_CHECK(status.ok());

  auto loaded = directory.Load(2);
  KV_INDEX_CHECK(loaded.ok());
  KV_INDEX_CHECK(loaded->get() == state.get());
  KV_INDEX_CHECK_EQ((*loaded)->ShardId(), 2U);
  KV_INDEX_CHECK_EQ((*loaded)->Generation(), 10U);
}

void RejectsInvalidShardIdsAndNullPublishes() {
  ShardDirectory directory(2);

  auto invalid_load = directory.Load(2);
  KV_INDEX_CHECK(!invalid_load.ok());
  KV_INDEX_CHECK_EQ(invalid_load.status().code(), StatusCode::kInvalidArgument);

  auto invalid_publish = directory.Publish(9, MakeState(9, 1));
  KV_INDEX_CHECK(!invalid_publish.ok());
  KV_INDEX_CHECK_EQ(invalid_publish.code(), StatusCode::kInvalidArgument);

  auto null_publish =
      directory.Publish(0, std::shared_ptr<const ShardState>());
  KV_INDEX_CHECK(!null_publish.ok());
  KV_INDEX_CHECK_EQ(null_publish.code(), StatusCode::kInvalidArgument);
}

void PreviouslyLoadedPinsSurviveRepublish() {
  ShardDirectory directory(1);
  auto first = MakeState(0, 10);
  auto second = MakeState(0, 11);

  KV_INDEX_CHECK(directory.Publish(0, first).ok());
  auto pinned_first = directory.Load(0);
  KV_INDEX_CHECK(pinned_first.ok());

  KV_INDEX_CHECK(directory.Publish(0, second).ok());
  auto current = directory.Load(0);
  KV_INDEX_CHECK(current.ok());

  KV_INDEX_CHECK_EQ((*pinned_first)->Generation(), 10U);
  KV_INDEX_CHECK_EQ((*current)->Generation(), 11U);
  KV_INDEX_CHECK(pinned_first->get() == first.get());
  KV_INDEX_CHECK(current->get() == second.get());
}

}  // namespace

int main() {
  EmptyShardsLoadAsNull();
  PublishReleaseStoresAndLoadAcquireLoadsState();
  RejectsInvalidShardIdsAndNullPublishes();
  PreviouslyLoadedPinsSurviveRepublish();
  return 0;
}
