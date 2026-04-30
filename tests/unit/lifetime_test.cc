#include "kv_index/forward_index.h"
#include "kv_index/row.h"
#include "kv_index/schema.h"
#include "src/core/realtime_delta.h"
#include "src/core/row_storage.h"
#include "src/core/shard_state.h"
#include "src/core/snapshot.h"
#include "src/core/snapshot_builder.h"
#include "src/core/test_peer.h"
#include "tests/test_support/test_macros.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace {

using kv_index::CompiledRowLayout;
using kv_index::FieldEncoding;
using kv_index::FieldLayout;
using kv_index::FieldSpec;
using kv_index::FieldType;
using kv_index::ForwardIndex;
using kv_index::ForwardIndexOptions;
using kv_index::RuntimeSchema;
using kv_index::SourcePosition;
using kv_index::core::CompactDeltaSnapshot;
using kv_index::core::ForwardIndexTestPeer;
using kv_index::core::FullSnapshotView;
using kv_index::core::OwnedSnapshotBacking;
using kv_index::core::RealtimeDeltaAtomicTable;
using kv_index::core::ShardState;
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

FieldSpec List(kv_index::FieldId field_id, std::string name,
               FieldType type) {
  return FieldSpec{
      .field_id = field_id,
      .name = std::move(name),
      .type = type,
      .is_list = true,
      .nullable = true,
      .encoding = FieldEncoding::kArena,
  };
}

std::shared_ptr<const CompiledRowLayout> MakeLayout() {
  RuntimeSchema schema(890);
  KV_INDEX_CHECK(schema.AddField(Scalar(1, "score", FieldType::kInt32)).ok());
  KV_INDEX_CHECK(schema.AddField(Scalar(2, "title", FieldType::kString)).ok());
  KV_INDEX_CHECK(schema.AddField(List(3, "scores", FieldType::kInt32)).ok());
  auto layout = CompiledRowLayout::Compile(schema);
  KV_INDEX_CHECK(layout.ok());
  return std::make_shared<const CompiledRowLayout>(std::move(layout).value());
}

const FieldLayout& Field(const CompiledRowLayout& layout,
                         kv_index::FieldId field_id) {
  const FieldLayout* field = layout.FindField(field_id);
  KV_INDEX_CHECK(field != nullptr);
  return *field;
}

storage::EncodedRow MakeRow(const CompiledRowLayout& layout,
                            std::int32_t score, std::string title,
                            std::vector<std::int32_t> scores) {
  auto encoded = storage::CreateEncodedRow(layout);
  KV_INDEX_CHECK(
      storage::WriteScalarField(Field(layout, 1), score, &encoded).ok());
  KV_INDEX_CHECK(
      storage::WriteArenaStringField(Field(layout, 2), title, &encoded).ok());
  KV_INDEX_CHECK(storage::WriteArenaListField(
                     Field(layout, 3),
                     std::span<const std::int32_t>(scores.data(),
                                                   scores.size()),
                     &encoded)
                     .ok());
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

std::shared_ptr<RealtimeDeltaAtomicTable> RealtimeWithRow(
    std::shared_ptr<const CompiledRowLayout> layout, std::uint64_t primary_key,
    std::int32_t score, std::string title) {
  auto realtime = std::make_shared<RealtimeDeltaAtomicTable>(
      RealtimeDeltaAtomicTable::Options{.layout = layout, .capacity = 16});
  KV_INDEX_CHECK(realtime
                     ->Publish(primary_key,
                               SourcePosition{.partition = 0, .offset = 1},
                               MakeRow(*layout, score, std::move(title),
                                       {score, score + 1}))
                     .ok());
  return realtime;
}

void CheckPinnedRow(const kv_index::Row& row, std::int32_t score,
                    const std::string& title) {
  KV_INDEX_CHECK_EQ(row.Get<std::int32_t>(1).value(), score);
  KV_INDEX_CHECK_EQ(row.Get<std::string>(2).value(), title);
  const auto scores = row.GetList<std::int32_t>(3);
  KV_INDEX_CHECK_EQ(scores.size(), 2U);
  KV_INDEX_CHECK_EQ(scores[0], score);
  KV_INDEX_CHECK_EQ(scores[1], score + 1);
}

void OldFullRowPinSurvivesFullReplacementCutover() {
  ForwardIndexOptions options;
  options.shard_count = 1;
  ForwardIndex index(options);
  auto layout = MakeLayout();
  auto old_state = std::make_shared<const ShardState>(
      0, 1,
      ShardState::Layers{
          .full_snapshot = FullSnapshotView(BuildSnapshot(
              layout,
              {{100, MakeRow(*layout, 10, "old-full", {10, 11})}})),
      });
  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(index, old_state).ok());
  auto pinned = index.Get(100);
  KV_INDEX_CHECK(pinned.has_value());

  auto new_state = std::make_shared<const ShardState>(
      0, 2,
      ShardState::Layers{
          .full_snapshot = FullSnapshotView(BuildSnapshot(
              layout,
              {{100, MakeRow(*layout, 20, "new-full", {20, 21})}})),
      });
  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(index, new_state).ok());

  CheckPinnedRow(*pinned, 10, "old-full");
  CheckPinnedRow(*index.Get(100), 20, "new-full");
}

void OldCompactRowPinSurvivesCompactReplacementCutover() {
  ForwardIndexOptions options;
  options.shard_count = 1;
  ForwardIndex index(options);
  auto layout = MakeLayout();
  auto old_state = std::make_shared<const ShardState>(
      0, 3,
      ShardState::Layers{
          .compact_delta = CompactDeltaSnapshot(BuildSnapshot(
              layout, {{100, MakeRow(*layout, 30, "old-compact", {30, 31})}})),
      });
  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(index, old_state).ok());
  auto pinned = index.Get(100);
  KV_INDEX_CHECK(pinned.has_value());

  auto new_state = std::make_shared<const ShardState>(
      0, 4,
      ShardState::Layers{
          .compact_delta = CompactDeltaSnapshot(BuildSnapshot(
              layout, {{100, MakeRow(*layout, 40, "new-compact", {40, 41})}})),
      });
  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(index, new_state).ok());

  CheckPinnedRow(*pinned, 30, "old-compact");
  CheckPinnedRow(*index.Get(100), 40, "new-compact");
}

void OldRealtimeRowPinSurvivesRealtimeReplacementCutover() {
  ForwardIndexOptions options;
  options.shard_count = 1;
  ForwardIndex index(options);
  auto layout = MakeLayout();
  auto old_state = std::make_shared<const ShardState>(
      0, 5,
      ShardState::Layers{
          .realtime_delta = RealtimeWithRow(layout, 100, 50, "old-realtime"),
      });
  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(index, old_state).ok());
  auto pinned = index.Get(100);
  KV_INDEX_CHECK(pinned.has_value());

  auto new_state = std::make_shared<const ShardState>(
      0, 6,
      ShardState::Layers{
          .full_snapshot = FullSnapshotView(BuildSnapshot(
              layout,
              {{100, MakeRow(*layout, 60, "new-full", {60, 61})}})),
      });
  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(index, new_state).ok());

  CheckPinnedRow(*pinned, 50, "old-realtime");
  CheckPinnedRow(*index.Get(100), 60, "new-full");
}

}  // namespace

int main() {
  OldFullRowPinSurvivesFullReplacementCutover();
  OldCompactRowPinSurvivesCompactReplacementCutover();
  OldRealtimeRowPinSurvivesRealtimeReplacementCutover();
  return 0;
}
