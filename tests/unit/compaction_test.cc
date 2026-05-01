#include "src/rebuild/compaction.h"

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
using kv_index::rebuild::BuildCompactedDeltaSnapshot;
using kv_index::store::CompactDeltaSnapshot;
using kv_index::rebuild::CompactionBuildRequest;
using kv_index::rebuild::FinishDeltaCompaction;
using kv_index::rebuild::FinishDeltaCompactionRequest;
using kv_index::store::FullSnapshotView;
using kv_index::store::OwnedSnapshotBacking;
using kv_index::store::RealtimeDeltaAtomicTable;
using kv_index::store::RealtimeDeltaBoundary;
using kv_index::runtime::ShardState;
using kv_index::store::SnapshotBuilder;
namespace storage = kv_index::model;

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

std::shared_ptr<const CompiledRowLayout> MakeLayout() {
  RuntimeSchema schema(700);
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
    const std::vector<std::pair<std::uint64_t, storage::EncodedRow>>& rows) {
  SnapshotBuilder builder(layout);
  for (const auto& [primary_key, encoded] : rows) {
    KV_INDEX_CHECK(builder.AddRow(primary_key, encoded).ok());
  }
  auto backing = builder.Seal();
  KV_INDEX_CHECK(backing.ok());
  return backing.value();
}

std::shared_ptr<RealtimeDeltaAtomicTable> MakeRealtime(
    std::shared_ptr<const CompiledRowLayout> layout, std::size_t capacity = 16) {
  return std::make_shared<RealtimeDeltaAtomicTable>(
      RealtimeDeltaAtomicTable::Options{
          .layout = std::move(layout),
          .capacity = capacity,
      });
}

Row VisibleRow(const CompactDeltaSnapshot& snapshot,
               std::uint64_t primary_key) {
  auto row = snapshot.Get(primary_key);
  KV_INDEX_CHECK(row.ok());
  KV_INDEX_CHECK(row->has_value());
  return std::move(row->value());
}

void CheckNoRow(const CompactDeltaSnapshot& snapshot,
                std::uint64_t primary_key) {
  auto row = snapshot.Get(primary_key);
  KV_INDEX_CHECK(row.ok());
  KV_INDEX_CHECK(!row->has_value());
}

void CompactionScansOnlyVisibleRowsAtBoundaryAndOverlaysOldCompact() {
  auto layout = MakeLayout();
  auto realtime = MakeRealtime(layout, 2);
  KV_INDEX_CHECK(realtime
                     ->Publish(100, SourcePosition{.partition = 0, .offset = 5},
                               MakeRow(*layout, 50, std::string("visible")))
                     .ok());
  KV_INDEX_CHECK(realtime
                     ->Publish(200, SourcePosition{.partition = 0, .offset = 6},
                               MakeRow(*layout, 60, std::string("other")))
                     .ok());
  KV_INDEX_CHECK(realtime
                     ->Publish(100, SourcePosition{.partition = 0, .offset = 4},
                               MakeRow(*layout, 40, std::string("stale")))
                     .ok());
  KV_INDEX_CHECK(realtime
                     ->Publish(100, SourcePosition{.partition = 0, .offset = 5},
                               MakeRow(*layout, 55, std::string("idempotent")))
                     .ok());
  auto failed = realtime->Publish(
      300, SourcePosition{.partition = 0, .offset = 7},
      MakeRow(*layout, 70, std::string("failed-capacity")));
  KV_INDEX_CHECK(!failed.ok());
  KV_INDEX_CHECK_EQ(failed.code(), StatusCode::kFailedPrecondition);

  RealtimeDeltaBoundary boundary = realtime->CaptureCompactionBoundary();

  KV_INDEX_CHECK(realtime
                     ->Publish(200, SourcePosition{.partition = 0, .offset = 8},
                               MakeRow(*layout, 80, std::string("after")))
                     .ok());

  auto old_compact = BuildSnapshot(
      layout, {{50, MakeRow(*layout, 5, std::string("old-only"))},
               {100, MakeRow(*layout, 10, std::string("old-overwritten"))}});
  auto full = BuildSnapshot(
      layout, {{999, MakeRow(*layout, 999, std::string("full-only"))}});
  ShardState state(
      0, 42,
      ShardState::Layers{
          .realtime_delta = realtime,
          .compact_delta = CompactDeltaSnapshot(old_compact),
          .full_snapshot = FullSnapshotView(full),
      });

  auto compact = BuildCompactedDeltaSnapshot(CompactionBuildRequest{
      .state = state,
      .boundary = boundary,
  });

  KV_INDEX_CHECK(compact.ok());
  CompactDeltaSnapshot compact_view(compact.value());
  Row realtime_row = VisibleRow(compact_view, 100);
  KV_INDEX_CHECK_EQ(realtime_row.Get<std::int32_t>(1).value(), 50);
  KV_INDEX_CHECK_EQ(realtime_row.Get<std::string>(2).value(), "visible");
  Row old_row = VisibleRow(compact_view, 50);
  KV_INDEX_CHECK_EQ(old_row.Get<std::int32_t>(1).value(), 5);
  Row boundary_row = VisibleRow(compact_view, 200);
  KV_INDEX_CHECK_EQ(boundary_row.Get<std::int32_t>(1).value(), 60);
  KV_INDEX_CHECK_EQ(boundary_row.Get<std::string>(2).value(), "other");
  CheckNoRow(compact_view, 300);
  CheckNoRow(compact_view, 999);
}

void CompactionCutoverUsesSuccessorRealtimeForPostBoundaryUpdates() {
  auto layout = MakeLayout();
  auto old_realtime = MakeRealtime(layout);
  auto successor_realtime = MakeRealtime(layout);
  KV_INDEX_CHECK(old_realtime
                     ->Publish(100, SourcePosition{.partition = 0, .offset = 1},
                               MakeRow(*layout, 10, std::string("sealed")))
                     .ok());
  RealtimeDeltaBoundary boundary =
      old_realtime->CaptureCompactionBoundary();
  KV_INDEX_CHECK(successor_realtime
                     ->Publish(100, SourcePosition{.partition = 0, .offset = 2},
                               MakeRow(*layout, 20, std::string("routed")))
                     .ok());

  ShardState state(0, 1,
                   ShardState::Layers{.realtime_delta = old_realtime});
  auto compact = BuildCompactedDeltaSnapshot(CompactionBuildRequest{
      .state = state,
      .boundary = boundary,
  });
  KV_INDEX_CHECK(compact.ok());

  auto successor = FinishDeltaCompaction(FinishDeltaCompactionRequest{
      .previous = state,
      .successor_generation = 2,
      .successor_realtime = successor_realtime,
      .compact_backing = compact.value(),
  });
  KV_INDEX_CHECK(successor.ok());
  auto row = successor.value()->Get(100);
  KV_INDEX_CHECK(row.ok());
  KV_INDEX_CHECK(row->has_value());
  KV_INDEX_CHECK_EQ(row->value().Get<std::int32_t>(1).value(), 20);
  KV_INDEX_CHECK_EQ(row->value().Get<std::string>(2).value(), "routed");
}

}  // namespace

int main() {
  CompactionScansOnlyVisibleRowsAtBoundaryAndOverlaysOldCompact();
  CompactionCutoverUsesSuccessorRealtimeForPostBoundaryUpdates();
  return 0;
}
