#include "kv_index/types.h"

#include "kv_index/status.h"
#include "tests/test_support/test_macros.h"

namespace {

using kv_index::SourcePosition;
using kv_index::SourcePositionUpdateDecision;
using kv_index::StatusCode;

void ValidPositionsRequireNonNegativePartitionAndOffset() {
  KV_INDEX_CHECK(kv_index::IsValidSourcePosition(
      SourcePosition{.partition = 0, .offset = 0}));
  KV_INDEX_CHECK(!kv_index::IsValidSourcePosition(
      SourcePosition{.partition = -1, .offset = 0}));
  KV_INDEX_CHECK(!kv_index::IsValidSourcePosition(
      SourcePosition{.partition = 0, .offset = -1}));
}

void SamePartitionHigherOffsetWins() {
  auto decision = kv_index::ClassifySourcePositionUpdate(
      SourcePosition{.partition = 3, .offset = 10},
      SourcePosition{.partition = 3, .offset = 11});

  KV_INDEX_CHECK(decision.ok());
  KV_INDEX_CHECK_EQ(decision.value(), SourcePositionUpdateDecision::kNewer);
}

void SamePartitionEqualOffsetIsIdempotentNoOp() {
  auto decision = kv_index::ClassifySourcePositionUpdate(
      SourcePosition{.partition = 3, .offset = 10},
      SourcePosition{.partition = 3, .offset = 10});

  KV_INDEX_CHECK(decision.ok());
  KV_INDEX_CHECK_EQ(decision.value(),
                    SourcePositionUpdateDecision::kIdempotent);
}

void SamePartitionLowerOffsetIsStale() {
  auto decision = kv_index::ClassifySourcePositionUpdate(
      SourcePosition{.partition = 3, .offset = 10},
      SourcePosition{.partition = 3, .offset = 9});

  KV_INDEX_CHECK(decision.ok());
  KV_INDEX_CHECK_EQ(decision.value(), SourcePositionUpdateDecision::kStale);
}

void InvalidPositionsFailClosed() {
  auto invalid_current = kv_index::ClassifySourcePositionUpdate(
      SourcePosition{.partition = -1, .offset = 10},
      SourcePosition{.partition = 0, .offset = 11});
  KV_INDEX_CHECK(!invalid_current.ok());
  KV_INDEX_CHECK_EQ(invalid_current.status().code(),
                    StatusCode::kInvalidArgument);

  auto invalid_candidate = kv_index::ClassifySourcePositionUpdate(
      SourcePosition{.partition = 0, .offset = 10},
      SourcePosition{.partition = 0, .offset = -1});
  KV_INDEX_CHECK(!invalid_candidate.ok());
  KV_INDEX_CHECK_EQ(invalid_candidate.status().code(),
                    StatusCode::kInvalidArgument);
}

void SameKeyCrossPartitionOrderingFailsClosed() {
  auto decision = kv_index::ClassifySourcePositionUpdate(
      SourcePosition{.partition = 3, .offset = 10},
      SourcePosition{.partition = 4, .offset = 11});

  KV_INDEX_CHECK(!decision.ok());
  KV_INDEX_CHECK_EQ(decision.status().code(), StatusCode::kFailedPrecondition);
}

}  // namespace

int main() {
  ValidPositionsRequireNonNegativePartitionAndOffset();
  SamePartitionHigherOffsetWins();
  SamePartitionEqualOffsetIsIdempotentNoOp();
  SamePartitionLowerOffsetIsStale();
  InvalidPositionsFailClosed();
  SameKeyCrossPartitionOrderingFailsClosed();
  return 0;
}
