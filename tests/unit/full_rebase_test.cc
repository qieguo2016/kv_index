#include "src/rebuild/full_rebase.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "kv_index/row.h"
#include "kv_index/schema.h"
#include "kv_index/status.h"
#include "kv_index/types.h"
#include "src/store/realtime_delta.h"
#include "src/model/row_storage.h"
#include "src/runtime/shard_state.h"
#include "src/store/snapshot.h"
#include "src/store/snapshot_builder.h"
#include "tests/test_support/test_macros.h"

namespace {

using kv_index::CompiledRowLayout;
using kv_index::FieldEncoding;
using kv_index::FieldLayout;
using kv_index::FieldSpec;
using kv_index::FieldType;
using kv_index::Row;
using kv_index::RuntimeSchema;
using kv_index::SourcePosition;
using kv_index::StatusCode;
using kv_index::core::BuildRebasedFullSnapshot;
using kv_index::core::CompactDeltaSnapshot;
using kv_index::core::FinishFullRebase;
using kv_index::core::FinishFullRebaseRequest;
using kv_index::core::FullRebaseBuildRequest;
using kv_index::core::FullSnapshotView;
using kv_index::core::OwnedSnapshotBacking;
using kv_index::core::RealtimeDeltaAtomicTable;
using kv_index::core::ShardState;
using kv_index::core::SnapshotBuildOptions;
using kv_index::core::SnapshotBuilder;
namespace storage = kv_index::internal;

FieldSpec Scalar(kv_index::FieldId field_id, std::string name,
                 FieldType type) {
  return FieldSpec{
      .field_id = field_id,
      .name = std::move(name),
      .type = type,
      .is_list = false,
      .nullable = true,
      .encoding = type == FieldType::kString ? FieldEncoding::kArena
                                             : FieldEncoding::kFixed,
  };
}

CompiledRowLayout Compile(RuntimeSchema schema) {
  auto layout = CompiledRowLayout::Compile(schema);
  KV_INDEX_CHECK(layout.ok());
  return layout.value();
}

const FieldLayout& Field(const CompiledRowLayout& layout,
                         kv_index::FieldId field_id) {
  const FieldLayout* field = layout.FindField(field_id);
  KV_INDEX_CHECK(field != nullptr);
  return *field;
}

std::shared_ptr<const CompiledRowLayout> MakeLayout(
    std::uint64_t schema_version = 710) {
  RuntimeSchema schema(schema_version);
  KV_INDEX_CHECK(schema.AddField(Scalar(1, "score", FieldType::kInt32)).ok());
  KV_INDEX_CHECK(schema.AddField(Scalar(2, "title", FieldType::kString)).ok());
  return std::make_shared<const CompiledRowLayout>(Compile(std::move(schema)));
}

storage::EncodedRow MakeRow(const CompiledRowLayout& layout,
                            std::int32_t score,
                            std::optional<std::string> title) {
  auto encoded = storage::CreateEncodedRow(layout);
  KV_INDEX_CHECK(
      storage::WriteScalarField(Field(layout, 1), score, &encoded).ok());
  if (title.has_value()) {
    KV_INDEX_CHECK(
        storage::WriteArenaStringField(Field(layout, 2), *title, &encoded)
            .ok());
  }
  return encoded;
}

std::shared_ptr<const OwnedSnapshotBacking> BuildSnapshot(
    std::shared_ptr<const CompiledRowLayout> layout,
    const std::vector<std::pair<std::uint64_t, storage::EncodedRow>>& rows,
    SnapshotBuildOptions options = {}) {
  SnapshotBuilder builder(layout, options);
  for (const auto& [primary_key, encoded] : rows) {
    KV_INDEX_CHECK(builder.AddRow(primary_key, encoded).ok());
  }
  auto backing = builder.Seal();
  KV_INDEX_CHECK(backing.ok());
  return backing.value();
}

std::shared_ptr<RealtimeDeltaAtomicTable> MakeRealtime(
    std::shared_ptr<const CompiledRowLayout> layout) {
  return std::make_shared<RealtimeDeltaAtomicTable>(
      RealtimeDeltaAtomicTable::Options{
          .layout = std::move(layout),
          .capacity = 16,
      });
}

Row VisibleRow(const FullSnapshotView& snapshot, std::uint64_t primary_key) {
  auto row = snapshot.Get(primary_key);
  KV_INDEX_CHECK(row.ok());
  KV_INDEX_CHECK(row->has_value());
  return std::move(row->value());
}

void RejectsIneligibleRebaseAndExternalLoadConflict() {
  auto layout = MakeLayout();
  ShardState missing_full(0, 1);
  auto no_full = BuildRebasedFullSnapshot(FullRebaseBuildRequest{
      .state = missing_full,
  });
  KV_INDEX_CHECK(!no_full.ok());
  KV_INDEX_CHECK_EQ(no_full.status().code(), StatusCode::kFailedPrecondition);

  ShardState with_full(
      0, 1,
      ShardState::Layers{
          .full_snapshot = FullSnapshotView(BuildSnapshot(
              layout, {{100, MakeRow(*layout, 10, std::string("full"))}})),
      });
  auto external_conflict = BuildRebasedFullSnapshot(FullRebaseBuildRequest{
      .state = with_full,
      .external_async_load_active = true,
  });
  KV_INDEX_CHECK(!external_conflict.ok());
  KV_INDEX_CHECK_EQ(external_conflict.status().code(),
                    StatusCode::kFailedPrecondition);

  auto other_layout = MakeLayout(711);
  ShardState mismatched_compact(
      0, 1,
      ShardState::Layers{
          .compact_delta = CompactDeltaSnapshot(BuildSnapshot(
              other_layout,
              {{100, MakeRow(*other_layout, 20, std::string("compact"))}})),
          .full_snapshot = FullSnapshotView(BuildSnapshot(
              layout, {{100, MakeRow(*layout, 10, std::string("full"))}})),
      });
  auto mismatch = BuildRebasedFullSnapshot(FullRebaseBuildRequest{
      .state = mismatched_compact,
  });
  KV_INDEX_CHECK(!mismatch.ok());
  KV_INDEX_CHECK_EQ(mismatch.status().code(),
                    StatusCode::kFailedPrecondition);

  ShardState mismatched_hash(
      0, 1,
      ShardState::Layers{
          .compact_delta = CompactDeltaSnapshot(BuildSnapshot(
              layout,
              {{100, MakeRow(*layout, 20, std::string("compact"))}},
              SnapshotBuildOptions{.hash_seed = 99, .hash_version = 1})),
          .full_snapshot = FullSnapshotView(BuildSnapshot(
              layout, {{100, MakeRow(*layout, 10, std::string("full"))}},
              SnapshotBuildOptions{.hash_seed = 0, .hash_version = 1})),
      });
  auto hash_mismatch = BuildRebasedFullSnapshot(FullRebaseBuildRequest{
      .state = mismatched_hash,
  });
  KV_INDEX_CHECK(!hash_mismatch.ok());
  KV_INDEX_CHECK_EQ(hash_mismatch.status().code(),
                    StatusCode::kFailedPrecondition);
}

void RebaseBuildsCompactOverlayFullAndCutoverKeepsRebaseRealtime() {
  auto layout = MakeLayout();
  auto full = BuildSnapshot(
      layout, {{100, MakeRow(*layout, 10, std::string("full-overwritten"))},
               {200, MakeRow(*layout, 20, std::string("full-only"))}});
  auto compact = BuildSnapshot(
      layout, {{100, MakeRow(*layout, 30, std::string("compact"))},
               {300, MakeRow(*layout, 40, std::string("compact-only"))}});
  ShardState state(
      0, 7,
      ShardState::Layers{
          .compact_delta = CompactDeltaSnapshot(compact),
          .full_snapshot = FullSnapshotView(full),
      });

  auto rebased = BuildRebasedFullSnapshot(FullRebaseBuildRequest{
      .state = state,
  });
  KV_INDEX_CHECK(rebased.ok());
  FullSnapshotView rebased_view(rebased.value());
  KV_INDEX_CHECK_EQ(
      VisibleRow(rebased_view, 100).Get<std::int32_t>(1).value(), 30);
  KV_INDEX_CHECK_EQ(
      VisibleRow(rebased_view, 200).Get<std::string>(2).value(), "full-only");
  KV_INDEX_CHECK_EQ(
      VisibleRow(rebased_view, 300).Get<std::int32_t>(1).value(), 40);

  auto rebase_realtime = MakeRealtime(layout);
  KV_INDEX_CHECK(rebase_realtime
                     ->Publish(100, SourcePosition{.partition = 0, .offset = 9},
                               MakeRow(*layout, 90, std::string("during")))
                     .ok());
  auto successor = FinishFullRebase(FinishFullRebaseRequest{
      .previous = state,
      .successor_generation = 8,
      .rebase_realtime = rebase_realtime,
      .full_backing = rebased.value(),
  });
  KV_INDEX_CHECK(successor.ok());
  KV_INDEX_CHECK(!successor.value()->compact_delta().has_value());
  auto visible = successor.value()->Get(100);
  KV_INDEX_CHECK(visible.ok());
  KV_INDEX_CHECK(visible->has_value());
  KV_INDEX_CHECK_EQ(visible->value().Get<std::int32_t>(1).value(), 90);
  KV_INDEX_CHECK_EQ(visible->value().Get<std::string>(2).value(), "during");
  auto full_only = successor.value()->Get(200);
  KV_INDEX_CHECK(full_only.ok());
  KV_INDEX_CHECK_EQ(full_only->value().Get<std::int32_t>(1).value(), 20);
}

void FinishFullRebaseRejectsVisiblePreviousRealtimeToAvoidDataLoss() {
  auto layout = MakeLayout();
  auto full = BuildSnapshot(
      layout, {{200, MakeRow(*layout, 20, std::string("full-only"))}});
  auto previous_realtime = MakeRealtime(layout);
  KV_INDEX_CHECK(previous_realtime
                     ->Publish(100, SourcePosition{.partition = 0, .offset = 5},
                               MakeRow(*layout, 70,
                                       std::string("previous-realtime-only")))
                     .ok());
  ShardState previous(
      0, 1,
      ShardState::Layers{
          .realtime_delta = previous_realtime,
          .full_snapshot = FullSnapshotView(full),
      });

  auto rebased = BuildRebasedFullSnapshot(FullRebaseBuildRequest{
      .state = previous,
  });
  KV_INDEX_CHECK(rebased.ok());

  auto successor = FinishFullRebase(FinishFullRebaseRequest{
      .previous = previous,
      .successor_generation = 2,
      .rebase_realtime = MakeRealtime(layout),
      .full_backing = rebased.value(),
  });
  KV_INDEX_CHECK(!successor.ok());
  KV_INDEX_CHECK_EQ(successor.status().code(),
                    StatusCode::kFailedPrecondition);

  auto still_visible = previous.Get(100);
  KV_INDEX_CHECK(still_visible.ok());
  KV_INDEX_CHECK(still_visible->has_value());
  KV_INDEX_CHECK_EQ(still_visible->value().Get<std::int32_t>(1).value(), 70);
  KV_INDEX_CHECK_EQ(still_visible->value().Get<std::string>(2).value(),
                    "previous-realtime-only");
}

void FinishFullRebaseAllowsEmptyPreviousRealtime() {
  auto layout = MakeLayout();
  auto full = BuildSnapshot(
      layout, {{200, MakeRow(*layout, 20, std::string("full-only"))}});
  ShardState previous(
      0, 1,
      ShardState::Layers{
          .realtime_delta = MakeRealtime(layout),
          .full_snapshot = FullSnapshotView(full),
      });

  auto rebased = BuildRebasedFullSnapshot(FullRebaseBuildRequest{
      .state = previous,
  });
  KV_INDEX_CHECK(rebased.ok());

  auto successor = FinishFullRebase(FinishFullRebaseRequest{
      .previous = previous,
      .successor_generation = 2,
      .rebase_realtime = MakeRealtime(layout),
      .full_backing = rebased.value(),
  });
  KV_INDEX_CHECK(successor.ok());
  KV_INDEX_CHECK_EQ(successor.value()->Generation(), 2);
  auto full_only = successor.value()->Get(200);
  KV_INDEX_CHECK(full_only.ok());
  KV_INDEX_CHECK(full_only->has_value());
  KV_INDEX_CHECK_EQ(full_only->value().Get<std::int32_t>(1).value(), 20);
}

}  // namespace

int main() {
  RejectsIneligibleRebaseAndExternalLoadConflict();
  RebaseBuildsCompactOverlayFullAndCutoverKeepsRebaseRealtime();
  FinishFullRebaseRejectsVisiblePreviousRealtimeToAvoidDataLoss();
  FinishFullRebaseAllowsEmptyPreviousRealtime();
  return 0;
}
