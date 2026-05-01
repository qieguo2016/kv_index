#include "src/runtime/shard_state.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "kv_index/row.h"
#include "kv_index/schema.h"
#include "kv_index/types.h"
#include "src/store/frozen_primary_key_index.h"
#include "src/store/realtime_delta.h"
#include "src/model/row_storage.h"
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
using kv_index::internal::store::BuildFrozenPrimaryKeyIndex;
using kv_index::internal::store::CompactDeltaSnapshot;
using kv_index::internal::store::FrozenPrimaryKeyIndexEntry;
using kv_index::internal::store::FullSnapshotView;
using kv_index::internal::store::OwnedSnapshotBacking;
using kv_index::internal::store::OwnedSnapshotRowPayload;
using kv_index::internal::store::RealtimeDeltaAtomicTable;
using kv_index::internal::runtime::ShardState;
using kv_index::internal::store::SnapshotBuilder;
namespace storage = kv_index::internal::model;

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
  RuntimeSchema schema(300);
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

std::shared_ptr<const OwnedSnapshotBacking> BuildCorruptSnapshotForKey(
    std::shared_ptr<const CompiledRowLayout> layout,
    std::uint64_t primary_key) {
  const std::array<FrozenPrimaryKeyIndexEntry, 1> entries = {{
      {.primary_key = primary_key,
       .row_offset = static_cast<std::uint64_t>(layout->row_slot_size() * 4)},
  }};
  auto index_bytes = BuildFrozenPrimaryKeyIndex(entries);
  KV_INDEX_CHECK(index_bytes.ok());

  std::vector<std::byte> row_slot_bytes(layout->row_slot_size());
  std::vector<OwnedSnapshotRowPayload> payloads(1);
  return std::make_shared<OwnedSnapshotBacking>(
      layout, std::move(index_bytes).value(), std::move(row_slot_bytes),
      std::move(payloads));
}

std::shared_ptr<RealtimeDeltaAtomicTable> BuildRealtime(
    std::shared_ptr<const CompiledRowLayout> layout,
    const std::vector<std::pair<std::uint64_t, storage::EncodedRow>>& rows) {
  auto realtime = std::make_shared<RealtimeDeltaAtomicTable>(
      RealtimeDeltaAtomicTable::Options{.layout = layout, .capacity = 16});
  std::int64_t offset = 1;
  for (const auto& [primary_key, encoded] : rows) {
    KV_INDEX_CHECK(realtime
                       ->Publish(primary_key,
                                 SourcePosition{.partition = 0,
                                                .offset = offset++},
                                 encoded)
                       .ok());
  }
  return realtime;
}

void EmptyStateMissesAndExposesIdentity() {
  ShardState state(7, 42);

  KV_INDEX_CHECK_EQ(state.ShardId(), 7U);
  KV_INDEX_CHECK_EQ(state.Generation(), 42U);

  auto result = state.Get(100);
  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK(!result->has_value());
}

void FullSnapshotServesRowsAfterRealtimeAndCompactMiss() {
  auto layout = MakeLayout();
  ShardState state(
      1, 10,
      ShardState::Layers{
          .full_snapshot = FullSnapshotView(BuildSnapshot(
              layout, {{100, MakeRow(*layout, 7, std::string("full"))}})),
      });

  auto result = state.Get(100);
  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK(result->has_value());
  KV_INDEX_CHECK_EQ(result->value().Get<std::int32_t>(1).value(), 7);
  KV_INDEX_CHECK_EQ(result->value().Get<std::string>(2).value(),
                    std::string("full"));
}

void RealtimeDeltaOverridesCompactAndFullAsWholeRow() {
  auto layout = MakeLayout();
  ShardState state(
      2, 11,
      ShardState::Layers{
          .realtime_delta = BuildRealtime(
              layout, {{100, MakeRow(*layout, 11, std::string("realtime"))}}),
          .compact_delta = CompactDeltaSnapshot(BuildSnapshot(
              layout, {{100, MakeRow(*layout, 9, std::nullopt)}})),
          .full_snapshot = FullSnapshotView(BuildSnapshot(
              layout, {{100, MakeRow(*layout, 7, std::string("full"))}})),
      });

  auto result = state.Get(100);
  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK(result->has_value());
  KV_INDEX_CHECK_EQ(result->value().Get<std::int32_t>(1).value(), 11);
  KV_INDEX_CHECK_EQ(result->value().Get<std::string>(2).value(),
                    std::string("realtime"));
}

void ReservedRealtimeMissFallsThroughToCompactSnapshot() {
  auto layout = MakeLayout();
  auto realtime = std::make_shared<RealtimeDeltaAtomicTable>(
      RealtimeDeltaAtomicTable::Options{.layout = layout, .capacity = 16});
  KV_INDEX_CHECK(realtime->ReserveSlotForTesting(100).ok());
  ShardState state(
      2, 12,
      ShardState::Layers{
          .realtime_delta = realtime,
          .compact_delta = CompactDeltaSnapshot(BuildSnapshot(
              layout, {{100, MakeRow(*layout, 9, std::nullopt)}})),
          .full_snapshot = FullSnapshotView(BuildSnapshot(
              layout, {{100, MakeRow(*layout, 7, std::string("full"))}})),
      });

  auto result = state.Get(100);
  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK(result->has_value());
  KV_INDEX_CHECK_EQ(result->value().Get<std::int32_t>(1).value(), 9);
  KV_INDEX_CHECK(!result->value().Get<std::string>(2).has_value());
}

void CompactSnapshotOverridesFullSnapshotAsWholeRow() {
  auto layout = MakeLayout();
  ShardState state(
      2, 11,
      ShardState::Layers{
          .compact_delta = CompactDeltaSnapshot(BuildSnapshot(
              layout, {{100, MakeRow(*layout, 9, std::nullopt)}})),
          .full_snapshot = FullSnapshotView(BuildSnapshot(
              layout, {{100, MakeRow(*layout, 7, std::string("full"))}})),
      });

  auto result = state.Get(100);
  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK(result->has_value());
  KV_INDEX_CHECK_EQ(result->value().Get<std::int32_t>(1).value(), 9);
  KV_INDEX_CHECK(!result->value().Get<std::string>(2).has_value());
}

void CompactErrorsFailClosedWithoutFullFallback() {
  auto layout = MakeLayout();
  ShardState state(
      3, 12,
      ShardState::Layers{
          .compact_delta =
              CompactDeltaSnapshot(BuildCorruptSnapshotForKey(layout, 100)),
          .full_snapshot = FullSnapshotView(BuildSnapshot(
              layout, {{100, MakeRow(*layout, 7, std::string("full"))}})),
      });

  auto result = state.Get(100);
  KV_INDEX_CHECK(!result.ok());
  KV_INDEX_CHECK_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

void RealtimeErrorsFailClosedWithoutFullFallback() {
  auto layout = MakeLayout();
  auto bad_realtime = std::make_shared<RealtimeDeltaAtomicTable>(
      RealtimeDeltaAtomicTable::Options{.layout = nullptr, .capacity = 16});
  ShardState state(
      3, 13,
      ShardState::Layers{
          .realtime_delta = bad_realtime,
          .full_snapshot = FullSnapshotView(BuildSnapshot(
              layout, {{100, MakeRow(*layout, 7, std::string("full"))}})),
      });

  auto result = state.Get(100);
  KV_INDEX_CHECK(!result.ok());
  KV_INDEX_CHECK_EQ(result.status().code(), StatusCode::kFailedPrecondition);
}

void MGetPreservesShardLocalOrderAndDuplicatePositions() {
  auto layout = MakeLayout();
  auto realtime = BuildRealtime(
      layout, {{20, MakeRow(*layout, 20, std::string("realtime-twenty"))}});
  ShardState state(
      4, 14,
      ShardState::Layers{
          .realtime_delta = realtime,
          .full_snapshot = FullSnapshotView(BuildSnapshot(
              layout, {{10, MakeRow(*layout, 1, std::string("ten"))},
                       {20, MakeRow(*layout, 2, std::string("twenty"))}})),
      });

  const std::vector<std::uint64_t> keys = {20, 10, 20, 99};
  auto result = state.MGet(keys);
  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK_EQ(result->size(), keys.size());
  KV_INDEX_CHECK_EQ((*result)[0]->Get<std::int32_t>(1).value(), 20);
  KV_INDEX_CHECK_EQ((*result)[1]->Get<std::int32_t>(1).value(), 1);
  KV_INDEX_CHECK_EQ((*result)[2]->Get<std::int32_t>(1).value(), 20);
  KV_INDEX_CHECK(!(*result)[3].has_value());
}

}  // namespace

int main() {
  EmptyStateMissesAndExposesIdentity();
  FullSnapshotServesRowsAfterRealtimeAndCompactMiss();
  RealtimeDeltaOverridesCompactAndFullAsWholeRow();
  ReservedRealtimeMissFallsThroughToCompactSnapshot();
  CompactSnapshotOverridesFullSnapshotAsWholeRow();
  CompactErrorsFailClosedWithoutFullFallback();
  RealtimeErrorsFailClosedWithoutFullFallback();
  MGetPreservesShardLocalOrderAndDuplicatePositions();
  return 0;
}
