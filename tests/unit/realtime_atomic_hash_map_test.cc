#include "src/store/realtime_delta.h"

#include <cstdint>
#include <memory>

#include "kv_index/status.h"
#include "kv_index/types.h"
#include "src/model/row_storage.h"
#include "tests/test_support/test_macros.h"

namespace {

using kv_index::SourcePosition;
using kv_index::StatusCode;
using kv_index::core::RealtimeAtomicHashMap;
using kv_index::core::RealtimeRowRef;

RealtimeRowRef MakeRef(std::uint64_t primary_key, SourcePosition position) {
  return RealtimeRowRef{
      .primary_key = primary_key,
      .position = position,
      .encoded = std::make_shared<const kv_index::internal::EncodedRow>(),
      .row_slot_bytes = 8,
      .payload_pool_bytes = 0,
  };
}

void PublishesAndLooksUpVisibleRowRef() {
  RealtimeAtomicHashMap map(
      RealtimeAtomicHashMap::Options{.capacity = 4,
                                     .hash_seed = 17,
                                     .hash_version = 2});
  auto row = MakeRef(10, SourcePosition{.partition = 0, .offset = 1});

  KV_INDEX_CHECK(map.Publish(10, &row).ok());

  auto result = map.Get(10);
  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK(result->has_value());
  KV_INDEX_CHECK_EQ(result->value(), &row);
  KV_INDEX_CHECK_EQ(map.unique_key_count(), 1U);
  KV_INDEX_CHECK_EQ(map.capacity(), 4U);
}

void HigherOffsetOverwritesExistingKey() {
  RealtimeAtomicHashMap map(RealtimeAtomicHashMap::Options{.capacity = 4});
  auto old_row = MakeRef(10, SourcePosition{.partition = 1, .offset = 1});
  auto new_row = MakeRef(10, SourcePosition{.partition = 1, .offset = 2});

  KV_INDEX_CHECK(map.Publish(10, &old_row).ok());
  KV_INDEX_CHECK(map.Publish(10, &new_row).ok());

  auto result = map.Get(10);
  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK_EQ(result->value(), &new_row);
  KV_INDEX_CHECK_EQ(map.unique_key_count(), 1U);
}

void EqualAndLowerOffsetsDoNotOverwriteExistingKey() {
  RealtimeAtomicHashMap map(RealtimeAtomicHashMap::Options{.capacity = 4});
  auto visible = MakeRef(10, SourcePosition{.partition = 1, .offset = 5});
  auto equal = MakeRef(10, SourcePosition{.partition = 1, .offset = 5});
  auto stale = MakeRef(10, SourcePosition{.partition = 1, .offset = 4});

  KV_INDEX_CHECK(map.Publish(10, &visible).ok());
  KV_INDEX_CHECK(map.Publish(10, &equal).ok());
  KV_INDEX_CHECK(map.Publish(10, &stale).ok());

  auto result = map.Get(10);
  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK_EQ(result->value(), &visible);
}

void CrossPartitionUpdateFailsClosedAndKeepsVisibleRow() {
  RealtimeAtomicHashMap map(RealtimeAtomicHashMap::Options{.capacity = 4});
  auto visible = MakeRef(10, SourcePosition{.partition = 1, .offset = 5});
  auto invalid = MakeRef(10, SourcePosition{.partition = 2, .offset = 6});

  KV_INDEX_CHECK(map.Publish(10, &visible).ok());
  auto status = map.Publish(10, &invalid);

  KV_INDEX_CHECK(!status.ok());
  KV_INDEX_CHECK_EQ(status.code(), StatusCode::kFailedPrecondition);
  auto result = map.Get(10);
  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK_EQ(result->value(), &visible);
}

void ReservedSlotReturnsMissImmediately() {
  RealtimeAtomicHashMap map(RealtimeAtomicHashMap::Options{.capacity = 4});

  KV_INDEX_CHECK(map.ReserveSlotForTesting(10).ok());

  auto result = map.Get(10);
  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK(!result->has_value());
}

void CapacityExhaustionFailsClosedWithoutResize() {
  RealtimeAtomicHashMap map(RealtimeAtomicHashMap::Options{.capacity = 1});
  auto first = MakeRef(10, SourcePosition{.partition = 0, .offset = 1});
  auto second = MakeRef(20, SourcePosition{.partition = 0, .offset = 1});

  KV_INDEX_CHECK(map.Publish(10, &first).ok());
  auto status = map.Publish(20, &second);

  KV_INDEX_CHECK(!status.ok());
  KV_INDEX_CHECK_EQ(status.code(), StatusCode::kFailedPrecondition);
  KV_INDEX_CHECK_EQ(map.capacity(), 1U);
  KV_INDEX_CHECK_EQ(map.unique_key_count(), 1U);
  auto missing = map.Get(20);
  KV_INDEX_CHECK(missing.ok());
  KV_INDEX_CHECK(!missing->has_value());
}

void InvalidCandidatePositionFailsClosed() {
  RealtimeAtomicHashMap map(RealtimeAtomicHashMap::Options{.capacity = 4});
  auto invalid = MakeRef(10, SourcePosition{.partition = -1, .offset = 1});

  auto status = map.Publish(10, &invalid);

  KV_INDEX_CHECK(!status.ok());
  KV_INDEX_CHECK_EQ(status.code(), StatusCode::kInvalidArgument);
  KV_INDEX_CHECK_EQ(map.unique_key_count(), 0U);
}

}  // namespace

int main() {
  PublishesAndLooksUpVisibleRowRef();
  HigherOffsetOverwritesExistingKey();
  EqualAndLowerOffsetsDoNotOverwriteExistingKey();
  CrossPartitionUpdateFailsClosedAndKeepsVisibleRow();
  ReservedSlotReturnsMissImmediately();
  CapacityExhaustionFailsClosedWithoutResize();
  InvalidCandidatePositionFailsClosed();
  return 0;
}
