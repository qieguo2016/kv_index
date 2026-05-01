#include "kv_index/forward_index.h"
#include "kv_index/schema.h"
#include "src/model/row_storage.h"
#include "src/runtime/shard_state.h"
#include "src/store/snapshot.h"
#include "src/store/snapshot_builder.h"
#include "src/testing/test_peer.h"
#include "test_support/test_macros.h"

#include <cstdint>
#include <cstdlib>
#include <memory>
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
using kv_index::RuntimeSchema;
using kv_index::internal::testing::ForwardIndexTestPeer;
using kv_index::internal::store::FullSnapshotView;
using kv_index::internal::store::OwnedSnapshotBacking;
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
      .encoding = FieldEncoding::kFixed,
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
  RuntimeSchema schema(500);
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
    std::shared_ptr<const CompiledRowLayout> layout, std::uint64_t primary_key,
    std::int32_t score) {
  SnapshotBuilder builder(layout);
  KV_INDEX_CHECK(builder.AddRow(primary_key, MakeRow(*layout, score)).ok());
  auto backing = builder.Seal();
  KV_INDEX_CHECK(backing.ok());
  return backing.value();
}

std::uint64_t FindKeyForShard(const ForwardIndex& index,
                              std::uint32_t shard_id) {
  for (std::uint64_t key = 1; key < 100000; ++key) {
    if (index.ShardFor(key) == shard_id) {
      return key;
    }
  }
  std::abort();
}

}  // namespace

int main() {
  kv_index::ForwardIndexOptions options;
  options.shard_count = 4;
  options.hash_seed = 20260426;

  kv_index::ForwardIndex index(options);
  const std::vector<std::uint64_t> keys = {10, 11, 12, 13, 10};

  const auto first_results = index.MGet(keys);
  const auto second_results = index.MGet(keys);

  KV_INDEX_CHECK_EQ(first_results.size(), keys.size());
  KV_INDEX_CHECK_EQ(second_results.size(), keys.size());
  for (std::size_t i = 0; i < keys.size(); ++i) {
    KV_INDEX_CHECK_EQ(first_results[i].has_value(),
                      second_results[i].has_value());
    KV_INDEX_CHECK_LT(index.ShardFor(keys[i]), options.shard_count);
  }

  auto layout = MakeLayout();
  const std::uint64_t installed_key = FindKeyForShard(index, 2);
  const std::uint64_t missing_key = installed_key + 1;
  auto state = std::make_shared<const ShardState>(
      2, 100,
      ShardState::Layers{
          .full_snapshot =
              FullSnapshotView(BuildSnapshot(layout, installed_key, 123)),
  });
  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(index, state).ok());
  const auto installed_results =
      index.MGet({installed_key, missing_key, installed_key});
  KV_INDEX_CHECK_EQ(installed_results.size(), 3U);
  KV_INDEX_CHECK_EQ(installed_results[0]->Get<std::int32_t>(1).value(), 123);
  KV_INDEX_CHECK(!installed_results[1].has_value());
  KV_INDEX_CHECK_EQ(installed_results[2]->Get<std::int32_t>(1).value(), 123);

  const kv_index::LoadId first_load =
      index.LoadAsync(kv_index::LoadRequest{.artifact_uri = "artifact-a"});
  const kv_index::LoadId second_load =
      index.LoadAsync(kv_index::LoadRequest{.artifact_uri = "artifact-b"});

  KV_INDEX_CHECK_NE(first_load, kv_index::kInvalidLoadId);
  KV_INDEX_CHECK_NE(second_load, kv_index::kInvalidLoadId);
  KV_INDEX_CHECK_NE(first_load, second_load);
  KV_INDEX_CHECK(!index.CancelLoad(first_load));

  return 0;
}
