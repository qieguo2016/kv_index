#include "src/store/realtime_delta.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "kv_index/row.h"
#include "kv_index/schema.h"
#include "kv_index/status.h"
#include "kv_index/types.h"
#include "src/model/row_storage.h"
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
using kv_index::ThresholdConfig;
using kv_index::store::RealtimeDeltaAtomicTable;
using kv_index::store::ShouldCompactRealtimeDelta;
namespace storage = kv_index::model;

FieldSpec Scalar(kv_index::FieldId field_id, std::string name, FieldType type,
                 bool nullable = true) {
  return FieldSpec{
      .field_id = field_id,
      .name = std::move(name),
      .type = type,
      .is_list = false,
      .nullable = nullable,
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
  RuntimeSchema schema(400);
  KV_INDEX_CHECK(
      schema.AddField(Scalar(1, "score", FieldType::kInt32, false)).ok());
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

void CheckRow(const Row& row, std::int32_t score,
              std::optional<std::string> title) {
  KV_INDEX_CHECK_EQ(row.Get<std::int32_t>(1).value(), score);
  if (title.has_value()) {
    KV_INDEX_CHECK_EQ(row.Get<std::string>(2).value(), *title);
  } else {
    KV_INDEX_CHECK(!row.Get<std::string>(2).has_value());
  }
}

void PublishesCompleteRowsAndMaterializesOnGet() {
  auto layout = MakeLayout();
  RealtimeDeltaAtomicTable table(
      RealtimeDeltaAtomicTable::Options{.layout = layout, .capacity = 8});

  KV_INDEX_CHECK(table
                     .Publish(100, SourcePosition{.partition = 0, .offset = 1},
                              MakeRow(*layout, 7, std::string("live")))
                     .ok());

  auto result = table.Get(100);
  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK(result->has_value());
  CheckRow(result->value(), 7, std::string("live"));
}

void ReturnedRowsOutliveTableAndPublishInputTemporaries() {
  Row pinned;
  {
    auto layout = MakeLayout();
    RealtimeDeltaAtomicTable table(
        RealtimeDeltaAtomicTable::Options{.layout = layout, .capacity = 8});
    KV_INDEX_CHECK(table
                       .Publish(100,
                                SourcePosition{.partition = 0, .offset = 1},
                                MakeRow(*layout, 7, std::string("live")))
                       .ok());
    auto result = table.Get(100);
    KV_INDEX_CHECK(result.ok());
    KV_INDEX_CHECK(result->has_value());
    pinned = std::move(result->value());
  }

  CheckRow(pinned, 7, std::string("live"));
}

void MalformedEncodedRowsFailClosedBeforePublication() {
  auto layout = MakeLayout();
  RealtimeDeltaAtomicTable table(
      RealtimeDeltaAtomicTable::Options{.layout = layout, .capacity = 8});

  auto bad_schema = MakeRow(*layout, 1, std::nullopt);
  bad_schema.schema_version += 1;
  auto bad_schema_status = table.Publish(
      100, SourcePosition{.partition = 0, .offset = 1}, std::move(bad_schema));
  KV_INDEX_CHECK(!bad_schema_status.ok());
  KV_INDEX_CHECK_EQ(bad_schema_status.code(),
                    StatusCode::kFailedPrecondition);

  auto short_slot = MakeRow(*layout, 2, std::nullopt);
  short_slot.row_slot.pop_back();
  auto short_slot_status = table.Publish(
      100, SourcePosition{.partition = 0, .offset = 2}, std::move(short_slot));
  KV_INDEX_CHECK(!short_slot_status.ok());
  KV_INDEX_CHECK_EQ(short_slot_status.code(), StatusCode::kInvalidArgument);

  auto missing_required = storage::CreateEncodedRow(*layout);
  auto missing_status =
      table.Publish(100, SourcePosition{.partition = 0, .offset = 3},
                    std::move(missing_required));
  KV_INDEX_CHECK(!missing_status.ok());
  KV_INDEX_CHECK_EQ(missing_status.code(), StatusCode::kFailedPrecondition);

  auto result = table.Get(100);
  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK(!result->has_value());
}

void OlderEqualAndCrossPartitionPublishesDoNotOverwriteVisibleRow() {
  auto layout = MakeLayout();
  RealtimeDeltaAtomicTable table(
      RealtimeDeltaAtomicTable::Options{.layout = layout, .capacity = 8});

  KV_INDEX_CHECK(table
                     .Publish(100, SourcePosition{.partition = 0, .offset = 5},
                              MakeRow(*layout, 5, std::string("current")))
                     .ok());
  KV_INDEX_CHECK(table
                     .Publish(100, SourcePosition{.partition = 0, .offset = 5},
                              MakeRow(*layout, 6, std::string("equal")))
                     .ok());
  KV_INDEX_CHECK(table
                     .Publish(100, SourcePosition{.partition = 0, .offset = 4},
                              MakeRow(*layout, 4, std::string("stale")))
                     .ok());
  auto cross_partition = table.Publish(
      100, SourcePosition{.partition = 1, .offset = 6},
      MakeRow(*layout, 6, std::string("invalid")));
  KV_INDEX_CHECK(!cross_partition.ok());
  KV_INDEX_CHECK_EQ(cross_partition.code(), StatusCode::kFailedPrecondition);

  auto result = table.Get(100);
  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK(result->has_value());
  CheckRow(result->value(), 5, std::string("current"));
}

void CapacityExhaustionFailsClosedWithoutPublishingSecondKey() {
  auto layout = MakeLayout();
  RealtimeDeltaAtomicTable table(
      RealtimeDeltaAtomicTable::Options{.layout = layout, .capacity = 1});

  KV_INDEX_CHECK(table
                     .Publish(100, SourcePosition{.partition = 0, .offset = 1},
                              MakeRow(*layout, 1, std::nullopt))
                     .ok());
  auto status = table.Publish(200, SourcePosition{.partition = 0, .offset = 2},
                              MakeRow(*layout, 2, std::nullopt));

  KV_INDEX_CHECK(!status.ok());
  KV_INDEX_CHECK_EQ(status.code(), StatusCode::kFailedPrecondition);
  auto result = table.Get(200);
  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK(!result->has_value());
}

void StatsAndThresholdHelpersReflectAppendOnlyStorage() {
  auto layout = MakeLayout();
  RealtimeDeltaAtomicTable table(
      RealtimeDeltaAtomicTable::Options{.layout = layout, .capacity = 4});

  KV_INDEX_CHECK(table
                     .Publish(100, SourcePosition{.partition = 0, .offset = 1},
                              MakeRow(*layout, 1, std::string("abc")))
                     .ok());
  KV_INDEX_CHECK(table
                     .Publish(200, SourcePosition{.partition = 0, .offset = 2},
                              MakeRow(*layout, 2, std::string("defgh")))
                     .ok());

  auto stats = table.stats();
  KV_INDEX_CHECK_EQ(stats.hash_capacity, 4U);
  KV_INDEX_CHECK_EQ(stats.unique_visible_keys, 2U);
  KV_INDEX_CHECK_EQ(stats.published_row_count, 2U);
  KV_INDEX_CHECK_EQ(stats.row_slot_bytes, layout->row_slot_size() * 2U);
  KV_INDEX_CHECK_EQ(stats.payload_pool_bytes, 8U);
  KV_INDEX_CHECK_EQ(stats.load_factor, 0.5);
  KV_INDEX_CHECK_EQ(stats.UniqueKeyRatio(20), 0.1);

  ThresholdConfig thresholds;
  thresholds.realtime_delta_load_factor = 0.50;
  thresholds.realtime_delta_unique_key_ratio = 1.0;
  thresholds.realtime_delta_row_arena_bytes = 1024;
  thresholds.realtime_delta_payload_pool_bytes = 1024;
  KV_INDEX_CHECK(ShouldCompactRealtimeDelta(stats, 20, thresholds));

  thresholds.realtime_delta_load_factor = 0.90;
  thresholds.realtime_delta_unique_key_ratio = 0.10;
  KV_INDEX_CHECK(ShouldCompactRealtimeDelta(stats, 20, thresholds));

  thresholds.realtime_delta_unique_key_ratio = 1.0;
  thresholds.realtime_delta_row_arena_bytes = stats.row_slot_bytes;
  KV_INDEX_CHECK(ShouldCompactRealtimeDelta(stats, 20, thresholds));

  thresholds.realtime_delta_row_arena_bytes = 1024;
  thresholds.realtime_delta_payload_pool_bytes = stats.payload_pool_bytes;
  KV_INDEX_CHECK(ShouldCompactRealtimeDelta(stats, 20, thresholds));
}

}  // namespace

int main() {
  PublishesCompleteRowsAndMaterializesOnGet();
  ReturnedRowsOutliveTableAndPublishInputTemporaries();
  MalformedEncodedRowsFailClosedBeforePublication();
  OlderEqualAndCrossPartitionPublishesDoNotOverwriteVisibleRow();
  CapacityExhaustionFailsClosedWithoutPublishingSecondKey();
  StatsAndThresholdHelpersReflectAppendOnlyStorage();
  return 0;
}
