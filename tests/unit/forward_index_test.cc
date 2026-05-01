#include "kv_index/forward_index.h"
#include "kv_index/version.h"
#include "src/store/frozen_primary_key_index.h"
#include "src/base/hash.h"
#include "src/model/row_storage.h"
#include "src/runtime/shard_state.h"
#include "src/store/snapshot.h"
#include "src/store/snapshot_builder.h"
#include "src/testing/test_peer.h"
#include "test_support/test_macros.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <stdexcept>
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
using kv_index::internal::store::CompactDeltaSnapshot;
using kv_index::internal::testing::ForwardIndexTestPeer;
using kv_index::internal::store::FullSnapshotView;
using kv_index::internal::store::OwnedSnapshotBacking;
using kv_index::internal::store::OwnedSnapshotRowPayload;
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
  RuntimeSchema schema(400);
  KV_INDEX_CHECK(schema.AddField(Scalar(1, "score", FieldType::kInt32)).ok());
  return std::make_shared<const CompiledRowLayout>(Compile(std::move(schema)));
}

storage::EncodedRow MakeRow(const CompiledRowLayout& layout,
                            std::int32_t score) {
  auto encoded = storage::CreateEncodedRow(layout);
  KV_INDEX_CHECK(
      storage::WriteScalarField(Field(layout, 1), score, &encoded).ok());
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
  const std::array<kv_index::internal::store::FrozenPrimaryKeyIndexEntry, 1> entries = {{
      {.primary_key = primary_key,
       .row_offset = static_cast<std::uint64_t>(layout->row_slot_size() * 4)},
  }};
  auto index_bytes = kv_index::internal::store::BuildFrozenPrimaryKeyIndex(entries);
  KV_INDEX_CHECK(index_bytes.ok());

  std::vector<std::byte> row_slot_bytes(layout->row_slot_size());
  std::vector<OwnedSnapshotRowPayload> payloads(1);
  return std::make_shared<OwnedSnapshotBacking>(
      layout, std::move(index_bytes).value(), std::move(row_slot_bytes),
      std::move(payloads));
}

std::uint64_t FindKeyForShard(const ForwardIndex& index,
                              std::uint32_t shard_id,
                              std::uint64_t start_at = 1) {
  for (std::uint64_t key = start_at; key < start_at + 100000; ++key) {
    if (index.ShardFor(key) == shard_id) {
      return key;
    }
  }
  std::abort();
}

std::shared_ptr<const ShardState> MakeFullState(
    std::uint32_t shard_id, std::uint64_t generation,
    std::shared_ptr<const CompiledRowLayout> layout,
    const std::vector<std::pair<std::uint64_t, std::int32_t>>& rows) {
  std::vector<std::pair<std::uint64_t, storage::EncodedRow>> encoded_rows;
  encoded_rows.reserve(rows.size());
  for (const auto& [primary_key, score] : rows) {
    encoded_rows.push_back({primary_key, MakeRow(*layout, score)});
  }
  return std::make_shared<const ShardState>(
      shard_id, generation,
      ShardState::Layers{
          .full_snapshot =
              FullSnapshotView(BuildSnapshot(layout, encoded_rows)),
      });
}

void DefaultOptionsMatchDesign() {
  const kv_index::ForwardIndexOptions options;

  KV_INDEX_CHECK_EQ(options.shard_count, 128U);
  KV_INDEX_CHECK_EQ(options.hash_version, 1U);
  KV_INDEX_CHECK(!kv_index::VersionString().empty());
}

void RejectsInvalidShardCounts() {
  kv_index::ForwardIndexOptions zero_shards;
  zero_shards.shard_count = 0;
  KV_INDEX_CHECK_THROWS(
      [&] {
        kv_index::ForwardIndex index(zero_shards);
        (void)index;
      }(),
      std::invalid_argument);

  kv_index::ForwardIndexOptions non_power_of_two;
  non_power_of_two.shard_count = 3;
  KV_INDEX_CHECK_THROWS(
      [&] {
        kv_index::ForwardIndex index(non_power_of_two);
        (void)index;
      }(),
      std::invalid_argument);
}

void StableShardAssignmentIsBoundedAndRepeatable() {
  kv_index::ForwardIndexOptions options;
  options.shard_count = 8;
  options.hash_seed = 17;

  const kv_index::ForwardIndex first(options);
  const kv_index::ForwardIndex second(options);

  for (const std::uint64_t key : {0ULL, 1ULL, 42ULL, 999999ULL}) {
    KV_INDEX_CHECK_LT(first.ShardFor(key), options.shard_count);
    KV_INDEX_CHECK_EQ(first.ShardFor(key), second.ShardFor(key));
  }

  KV_INDEX_CHECK_EQ(first.ShardFor(42), 4U);
}

void StableHashGoldenValuesRemainStable() {
  KV_INDEX_CHECK_EQ(kv_index::StableHash64(0, 0, 1),
                    12085254679833835651ULL);
  KV_INDEX_CHECK_EQ(kv_index::StableHash64(1, 0, 1),
                    10858953248184931039ULL);
  KV_INDEX_CHECK_EQ(kv_index::StableHash64(42, 17, 1),
                    13083006257041843452ULL);
  KV_INDEX_CHECK_EQ(kv_index::internal::base::StableHash64(42, 17, 1),
                    kv_index::StableHash64(42, 17, 1));
}

void EmptyIndexMissesPreserveMGetOrderAndShape() {
  const kv_index::ForwardIndex index(kv_index::ForwardIndexOptions{});
  const std::vector<std::uint64_t> keys = {9, 1, 9, 128, 2};

  const auto rows = index.MGet(keys);

  KV_INDEX_CHECK_EQ(rows.size(), keys.size());
  for (const auto& row : rows) {
    KV_INDEX_CHECK(!row.has_value());
  }
}

void PublicGetRoutesThroughInstalledShardState() {
  ForwardIndexOptions options;
  options.shard_count = 4;
  ForwardIndex index(options);
  auto layout = MakeLayout();
  const std::uint64_t key = FindKeyForShard(index, 2);
  const std::uint64_t unpublished_key = FindKeyForShard(index, 3);

  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(
                     index, MakeFullState(2, 100, layout, {{key, 77}}))
                     .ok());

  auto hit = index.Get(key);
  KV_INDEX_CHECK(hit.has_value());
  KV_INDEX_CHECK_EQ(hit->Get<std::int32_t>(1).value(), 77);
  KV_INDEX_CHECK(!index.Get(unpublished_key).has_value());
}

void PublicMGetGroupsByShardAndPreservesOrderAndDuplicates() {
  ForwardIndexOptions options;
  options.shard_count = 4;
  ForwardIndex index(options);
  auto layout = MakeLayout();
  const std::uint64_t key_a = FindKeyForShard(index, 0);
  const std::uint64_t key_b = FindKeyForShard(index, 1);
  const std::uint64_t missing = FindKeyForShard(index, 0, key_a + 1);

  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(
                     index, MakeFullState(0, 10, layout, {{key_a, 10}}))
                     .ok());
  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(
                     index, MakeFullState(1, 20, layout, {{key_b, 20}}))
                     .ok());

  const std::vector<std::uint64_t> keys = {key_b, missing, key_a,
                                           key_b, key_a};
  const auto rows = index.MGet(keys);

  KV_INDEX_CHECK_EQ(rows.size(), keys.size());
  KV_INDEX_CHECK_EQ(rows[0]->Get<std::int32_t>(1).value(), 20);
  KV_INDEX_CHECK(!rows[1].has_value());
  KV_INDEX_CHECK_EQ(rows[2]->Get<std::int32_t>(1).value(), 10);
  KV_INDEX_CHECK_EQ(rows[3]->Get<std::int32_t>(1).value(), 20);
  KV_INDEX_CHECK_EQ(rows[4]->Get<std::int32_t>(1).value(), 10);
}

void PublicGetThrowsOnInternalShardStatus() {
  ForwardIndexOptions options;
  options.shard_count = 4;
  ForwardIndex index(options);
  auto layout = MakeLayout();
  const std::uint64_t key = FindKeyForShard(index, 0);

  auto state = std::make_shared<const ShardState>(
      0, 30,
      ShardState::Layers{
          .compact_delta =
              CompactDeltaSnapshot(BuildCorruptSnapshotForKey(layout, key)),
      });
  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(index, state).ok());

  KV_INDEX_CHECK_THROWS(index.Get(key), std::runtime_error);
}

}  // namespace

int main() {
  DefaultOptionsMatchDesign();
  RejectsInvalidShardCounts();
  StableShardAssignmentIsBoundedAndRepeatable();
  StableHashGoldenValuesRemainStable();
  EmptyIndexMissesPreserveMGetOrderAndShape();
  PublicGetRoutesThroughInstalledShardState();
  PublicMGetGroupsByShardAndPreservesOrderAndDuplicates();
  PublicGetThrowsOnInternalShardStatus();
  return 0;
}
