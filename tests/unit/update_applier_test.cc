#include "src/ingest/update_applier.h"

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
#include "kv_index/status.h"
#include "kv_index/types.h"
#include "src/store/realtime_delta.h"
#include "tests/test_support/test_macros.h"

namespace {

using kv_index::CompiledRowLayout;
using kv_index::FieldEncoding;
using kv_index::FieldLayout;
using kv_index::FieldSpec;
using kv_index::FieldType;
using kv_index::KafkaPartition;
using kv_index::KafkaUpsertMessage;
using kv_index::Row;
using kv_index::RuntimeSchema;
using kv_index::Status;
using kv_index::StatusCode;
using kv_index::internal::store::RealtimeDeltaAtomicTable;
using kv_index::internal::ingest::UpdateApplier;
using kv_index::internal::ingest::UpdateApplierOptions;
using kv_index::internal::ingest::UpdateGenerationRole;
using kv_index::internal::ingest::UpdateTargetRoute;

enum class WireKind : std::uint8_t {
  kNull = 0,
  kInt8 = 1,
  kInt32 = 2,
  kInt64 = 3,
  kUInt64 = 4,
  kBool = 5,
  kString = 6,
};

template <typename T>
void AppendLittleEndian(T value, std::vector<std::byte>* bytes) {
  for (std::size_t i = 0; i < sizeof(T); ++i) {
    bytes->push_back(static_cast<std::byte>(
        (static_cast<std::uint64_t>(value) >> (i * 8U)) & 0xffU));
  }
}

void AppendFieldHeader(kv_index::FieldId field_id, WireKind kind,
                       std::uint32_t byte_length,
                       std::vector<std::byte>* bytes) {
  AppendLittleEndian<std::uint32_t>(field_id, bytes);
  bytes->push_back(static_cast<std::byte>(kind));
  AppendLittleEndian<std::uint32_t>(byte_length, bytes);
}

std::vector<std::byte> BeginPayload(std::uint32_t field_count) {
  std::vector<std::byte> bytes;
  bytes.push_back(std::byte{'K'});
  bytes.push_back(std::byte{'V'});
  bytes.push_back(std::byte{'I'});
  bytes.push_back(std::byte{'U'});
  AppendLittleEndian<std::uint16_t>(1, &bytes);
  AppendLittleEndian<std::uint16_t>(0, &bytes);
  AppendLittleEndian<std::uint32_t>(field_count, &bytes);
  return bytes;
}

void AddInt32(kv_index::FieldId field_id, std::int32_t value,
              std::vector<std::byte>* bytes) {
  AppendFieldHeader(field_id, WireKind::kInt32, sizeof(value), bytes);
  AppendLittleEndian<std::uint32_t>(static_cast<std::uint32_t>(value), bytes);
}

void AddString(kv_index::FieldId field_id, std::string_view value,
               std::vector<std::byte>* bytes) {
  AppendFieldHeader(field_id, WireKind::kString,
                    static_cast<std::uint32_t>(value.size()), bytes);
  bytes->insert(bytes->end(), reinterpret_cast<const std::byte*>(value.data()),
                reinterpret_cast<const std::byte*>(value.data()) +
                    value.size());
}

void AddNull(kv_index::FieldId field_id, std::vector<std::byte>* bytes) {
  AppendFieldHeader(field_id, WireKind::kNull, 0, bytes);
}

std::vector<std::byte> PayloadScoreTitle(std::int32_t score,
                                         std::string_view title) {
  auto bytes = BeginPayload(2);
  AddInt32(1, score, &bytes);
  AddString(2, title, &bytes);
  return bytes;
}

std::vector<std::byte> PayloadScoreOnly(std::int32_t score) {
  auto bytes = BeginPayload(1);
  AddInt32(1, score, &bytes);
  return bytes;
}

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

std::shared_ptr<const CompiledRowLayout> LayoutScoreOnly(
    std::uint64_t schema_version = 100) {
  RuntimeSchema schema(schema_version);
  KV_INDEX_CHECK(
      schema.AddField(Scalar(1, "score", FieldType::kInt32, false)).ok());
  return std::make_shared<const CompiledRowLayout>(Compile(std::move(schema)));
}

std::shared_ptr<const CompiledRowLayout> LayoutScoreTitle(
    std::uint64_t schema_version = 200) {
  RuntimeSchema schema(schema_version);
  KV_INDEX_CHECK(
      schema.AddField(Scalar(1, "score", FieldType::kInt32, false)).ok());
  KV_INDEX_CHECK(schema.AddField(Scalar(2, "title", FieldType::kString)).ok());
  return std::make_shared<const CompiledRowLayout>(Compile(std::move(schema)));
}

std::shared_ptr<const CompiledRowLayout> LayoutRequiresScoreAndTimestamp() {
  RuntimeSchema schema(300);
  KV_INDEX_CHECK(
      schema.AddField(Scalar(1, "score", FieldType::kInt32, false)).ok());
  KV_INDEX_CHECK(
      schema.AddField(Scalar(3, "updated_at", FieldType::kInt64, false)).ok());
  return std::make_shared<const CompiledRowLayout>(Compile(std::move(schema)));
}

std::shared_ptr<RealtimeDeltaAtomicTable> MakeTable(
    std::shared_ptr<const CompiledRowLayout> layout, std::size_t capacity = 8) {
  return std::make_shared<RealtimeDeltaAtomicTable>(
      RealtimeDeltaAtomicTable::Options{
          .layout = std::move(layout),
          .capacity = capacity,
      });
}

UpdateTargetRoute Route(
    UpdateGenerationRole role, std::uint64_t generation_id,
    std::shared_ptr<const CompiledRowLayout> layout,
    std::shared_ptr<RealtimeDeltaAtomicTable> table) {
  return UpdateTargetRoute{
      .role = role,
      .generation_id = generation_id,
      .shard_count = 1,
      .hash_seed = 0,
      .hash_version = 1,
      .layout = std::move(layout),
      .realtime_shards = {std::move(table)},
  };
}

KafkaUpsertMessage Message(std::uint64_t primary_key, std::int32_t partition,
                           std::int64_t offset,
                           std::vector<std::byte> payload,
                           std::string topic = "updates") {
  return KafkaUpsertMessage{
      .metadata =
          {
              .partition =
                  KafkaPartition{
                      .topic = std::move(topic),
                      .partition = partition,
                  },
              .offset = offset,
              .key = std::to_string(primary_key),
          },
      .primary_key = primary_key,
      .payload = std::move(payload),
  };
}

UpdateApplier ApplierWith(std::vector<UpdateTargetRoute> routes) {
  return UpdateApplier(UpdateApplierOptions{
      .logical_topic = "updates",
      .targets = std::move(routes),
  });
}

Row VisibleRow(const std::shared_ptr<RealtimeDeltaAtomicTable>& table,
               std::uint64_t primary_key) {
  auto result = table->Get(primary_key);
  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK(result->has_value());
  return std::move(result->value());
}

void CheckNoRow(const std::shared_ptr<RealtimeDeltaAtomicTable>& table,
                std::uint64_t primary_key) {
  auto result = table->Get(primary_key);
  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK(!result->has_value());
}

void DecodesCompleteRowAndPublishesToRealtime() {
  auto layout = LayoutScoreTitle();
  auto table = MakeTable(layout);
  auto applier = ApplierWith(
      {Route(UpdateGenerationRole::kActive, 1, layout, table)});

  Status status = applier.Apply(Message(100, 0, 7, PayloadScoreTitle(42, "red")));

  KV_INDEX_CHECK(status.ok());
  Row row = VisibleRow(table, 100);
  KV_INDEX_CHECK_EQ(row.Get<std::int32_t>(1).value(), 42);
  KV_INDEX_CHECK_EQ(row.Get<std::string>(2).value(), "red");
}

void RoutesAllGenerationRolesAndOldLayoutsIgnoreUnknownFields() {
  auto old_layout = LayoutScoreOnly(10);
  auto new_layout = LayoutScoreTitle(20);
  auto active = MakeTable(old_layout);
  auto rebuild = MakeTable(new_layout);
  auto rebase = MakeTable(new_layout);
  auto compaction = MakeTable(new_layout);
  auto applier = ApplierWith({
      Route(UpdateGenerationRole::kActive, 10, old_layout, active),
      Route(UpdateGenerationRole::kRebuild, 20, new_layout, rebuild),
      Route(UpdateGenerationRole::kRebase, 21, new_layout, rebase),
      Route(UpdateGenerationRole::kCompaction, 22, new_layout, compaction),
  });

  Status status =
      applier.Apply(Message(101, 0, 8, PayloadScoreTitle(9, "new-field")));

  KV_INDEX_CHECK(status.ok());
  Row active_row = VisibleRow(active, 101);
  KV_INDEX_CHECK_EQ(active_row.Get<std::int32_t>(1).value(), 9);
  KV_INDEX_CHECK(!active_row.Has(2));
  KV_INDEX_CHECK_EQ(VisibleRow(rebuild, 101).Get<std::string>(2).value(),
                    "new-field");
  KV_INDEX_CHECK_EQ(VisibleRow(rebase, 101).Get<std::string>(2).value(),
                    "new-field");
  KV_INDEX_CHECK_EQ(VisibleRow(compaction, 101).Get<std::string>(2).value(),
                    "new-field");
}

void ValidatesEveryTargetBeforePublishingAnyTarget() {
  auto old_layout = LayoutScoreOnly(10);
  auto strict_layout = LayoutRequiresScoreAndTimestamp();
  auto active = MakeTable(old_layout);
  auto rebuild = MakeTable(strict_layout);
  auto applier = ApplierWith({
      Route(UpdateGenerationRole::kActive, 10, old_layout, active),
      Route(UpdateGenerationRole::kRebuild, 30, strict_layout, rebuild),
  });

  Status status = applier.Apply(Message(102, 0, 9, PayloadScoreOnly(5)));

  KV_INDEX_CHECK(!status.ok());
  KV_INDEX_CHECK_EQ(status.code(), StatusCode::kFailedPrecondition);
  CheckNoRow(active, 102);
  CheckNoRow(rebuild, 102);
}

void MalformedPayloadsFailClosedWithoutPublication() {
  auto layout = LayoutScoreTitle();
  auto expect_fail = [&](std::vector<std::byte> payload,
                         StatusCode expected_code) {
    auto table = MakeTable(layout);
    auto applier = ApplierWith(
        {Route(UpdateGenerationRole::kActive, 1, layout, table)});
    Status status = applier.Apply(Message(103, 0, 10, std::move(payload)));
    KV_INDEX_CHECK(!status.ok());
    KV_INDEX_CHECK_EQ(status.code(), expected_code);
    CheckNoRow(table, 103);
  };

  expect_fail({std::byte{'b'}, std::byte{'a'}, std::byte{'d'}},
              StatusCode::kInvalidArgument);

  auto duplicate = BeginPayload(2);
  AddInt32(1, 1, &duplicate);
  AddInt32(1, 2, &duplicate);
  expect_fail(std::move(duplicate), StatusCode::kInvalidArgument);

  auto type_mismatch = BeginPayload(1);
  AddString(1, "not-an-int", &type_mismatch);
  expect_fail(std::move(type_mismatch), StatusCode::kInvalidArgument);

  auto explicit_null = BeginPayload(1);
  AddNull(2, &explicit_null);
  expect_fail(std::move(explicit_null), StatusCode::kInvalidArgument);
}

void W04SourceOrderingControlsReplayAndCrossPartitionFailures() {
  auto layout = LayoutScoreOnly();
  auto table = MakeTable(layout);
  auto applier = ApplierWith(
      {Route(UpdateGenerationRole::kActive, 1, layout, table)});

  KV_INDEX_CHECK(applier.Apply(Message(104, 0, 5, PayloadScoreOnly(5))).ok());
  KV_INDEX_CHECK(applier.Apply(Message(104, 0, 4, PayloadScoreOnly(4))).ok());
  Row row = VisibleRow(table, 104);
  KV_INDEX_CHECK_EQ(row.Get<std::int32_t>(1).value(), 5);

  Status cross_partition =
      applier.Apply(Message(104, 1, 6, PayloadScoreOnly(6)));
  KV_INDEX_CHECK(!cross_partition.ok());
  KV_INDEX_CHECK_EQ(cross_partition.code(), StatusCode::kFailedPrecondition);
  KV_INDEX_CHECK_EQ(VisibleRow(table, 104).Get<std::int32_t>(1).value(), 5);
}

void InvalidKafkaMetadataFailsClosed() {
  auto layout = LayoutScoreOnly();
  auto table = MakeTable(layout);
  auto applier = ApplierWith(
      {Route(UpdateGenerationRole::kActive, 1, layout, table)});

  Status wrong_topic =
      applier.Apply(Message(105, 0, 1, PayloadScoreOnly(1), "other"));
  KV_INDEX_CHECK(!wrong_topic.ok());
  KV_INDEX_CHECK_EQ(wrong_topic.code(), StatusCode::kFailedPrecondition);

  Status bad_partition =
      applier.Apply(Message(105, -1, 1, PayloadScoreOnly(1)));
  KV_INDEX_CHECK(!bad_partition.ok());
  KV_INDEX_CHECK_EQ(bad_partition.code(), StatusCode::kInvalidArgument);

  Status bad_offset =
      applier.Apply(Message(105, 0, -1, PayloadScoreOnly(1)));
  KV_INDEX_CHECK(!bad_offset.ok());
  KV_INDEX_CHECK_EQ(bad_offset.code(), StatusCode::kInvalidArgument);

  CheckNoRow(table, 105);
}

}  // namespace

int main() {
  DecodesCompleteRowAndPublishesToRealtime();
  RoutesAllGenerationRolesAndOldLayoutsIgnoreUnknownFields();
  ValidatesEveryTargetBeforePublishingAnyTarget();
  MalformedPayloadsFailClosedWithoutPublication();
  W04SourceOrderingControlsReplayAndCrossPartitionFailures();
  InvalidKafkaMetadataFailsClosed();
  return 0;
}
