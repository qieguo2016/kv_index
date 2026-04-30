#include "kv_index/forward_index.h"
#include "kv_index/schema.h"
#include "src/core/realtime_delta.h"
#include "src/core/row_storage.h"
#include "src/core/shard_state.h"
#include "src/core/snapshot.h"
#include "src/core/snapshot_builder.h"
#include "src/core/test_peer.h"
#include "tests/test_support/test_macros.h"

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
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

std::shared_ptr<const CompiledRowLayout> MakeLayout() {
  RuntimeSchema schema(900);
  KV_INDEX_CHECK(schema.AddField(Scalar(1, "score", FieldType::kInt32)).ok());
  KV_INDEX_CHECK(schema.AddField(Scalar(2, "title", FieldType::kString)).ok());
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

std::shared_ptr<const OwnedSnapshotBacking> Snapshot(
    std::shared_ptr<const CompiledRowLayout> layout,
    const std::vector<std::pair<std::uint64_t, storage::EncodedRow>>& rows) {
  SnapshotBuilder builder(layout);
  for (const auto& [key, row] : rows) {
    KV_INDEX_CHECK(builder.AddRow(key, row).ok());
  }
  auto backing = builder.Seal();
  KV_INDEX_CHECK(backing.ok());
  return backing.value();
}

std::shared_ptr<RealtimeDeltaAtomicTable> Realtime(
    std::shared_ptr<const CompiledRowLayout> layout, std::uint64_t key,
    std::int32_t score, std::string title) {
  auto realtime = std::make_shared<RealtimeDeltaAtomicTable>(
      RealtimeDeltaAtomicTable::Options{.layout = layout, .capacity = 16});
  KV_INDEX_CHECK(realtime
                     ->Publish(key, SourcePosition{.partition = 0, .offset = 1},
                               MakeRow(*layout, score, std::move(title)))
                     .ok());
  return realtime;
}

std::uint64_t KeyForShard(const ForwardIndex& index, std::uint32_t shard_id,
                          std::uint64_t start = 1) {
  for (std::uint64_t key = start; key < start + 100000; ++key) {
    if (index.ShardFor(key) == shard_id) {
      return key;
    }
  }
  std::abort();
}

void CheckRow(const std::optional<kv_index::Row>& row, std::int32_t score,
              std::optional<std::string> title) {
  KV_INDEX_CHECK(row.has_value());
  KV_INDEX_CHECK_EQ(row->Get<std::int32_t>(1).value(), score);
  if (title.has_value()) {
    KV_INDEX_CHECK_EQ(row->Get<std::string>(2).value(), *title);
  } else {
    KV_INDEX_CHECK(!row->Get<std::string>(2).has_value());
  }
}

}  // namespace

int main() {
  ForwardIndexOptions options;
  options.shard_count = 4;
  ForwardIndex index(options);
  auto layout = MakeLayout();

  const std::uint64_t layered = KeyForShard(index, 0);
  const std::uint64_t compact_only = KeyForShard(index, 1);
  const std::uint64_t full_only = KeyForShard(index, 2);
  const std::uint64_t realtime_only = KeyForShard(index, 3);
  const std::uint64_t missing = KeyForShard(index, 0, layered + 1);

  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(
                     index,
                     std::make_shared<const ShardState>(
                         0, 10,
                         ShardState::Layers{
                             .realtime_delta =
                                 Realtime(layout, layered, 30, "realtime"),
                             .compact_delta = CompactDeltaSnapshot(Snapshot(
                                 layout,
                                 {{layered,
                                   MakeRow(*layout, 20, std::nullopt)}})),
                             .full_snapshot = FullSnapshotView(Snapshot(
                                 layout,
                                 {{layered,
                                   MakeRow(*layout, 10, "full")}})),
                         }))
                     .ok());
  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(
                     index,
                     std::make_shared<const ShardState>(
                         1, 11,
                         ShardState::Layers{
                             .compact_delta = CompactDeltaSnapshot(Snapshot(
                                 layout,
                                 {{compact_only,
                                   MakeRow(*layout, 40, std::nullopt)}})),
                             .full_snapshot = FullSnapshotView(Snapshot(
                                 layout,
                                 {{compact_only,
                                   MakeRow(*layout, 35, "full")}})),
                         }))
                     .ok());
  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(
                     index,
                     std::make_shared<const ShardState>(
                         2, 12,
                         ShardState::Layers{
                             .full_snapshot = FullSnapshotView(Snapshot(
                                 layout,
                                 {{full_only,
                                   MakeRow(*layout, 50, "full-only")}})),
                         }))
                     .ok());
  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(
                     index,
                     std::make_shared<const ShardState>(
                         3, 13,
                         ShardState::Layers{
                             .realtime_delta = Realtime(
                                 layout, realtime_only, 60, "rt-only"),
                         }))
                     .ok());

  CheckRow(index.Get(layered), 30, "realtime");
  CheckRow(index.Get(compact_only), 40, std::nullopt);
  CheckRow(index.Get(full_only), 50, "full-only");

  const std::vector<std::uint64_t> keys = {
      realtime_only, missing, compact_only, layered, realtime_only, full_only};
  const auto rows = index.MGet(keys);
  KV_INDEX_CHECK_EQ(rows.size(), keys.size());
  CheckRow(rows[0], 60, "rt-only");
  KV_INDEX_CHECK(!rows[1].has_value());
  CheckRow(rows[2], 40, std::nullopt);
  CheckRow(rows[3], 30, "realtime");
  CheckRow(rows[4], 60, "rt-only");
  CheckRow(rows[5], 50, "full-only");
  return 0;
}
