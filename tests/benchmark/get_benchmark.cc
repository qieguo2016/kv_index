#include "kv_index/forward_index.h"
#include "kv_index/schema.h"
#include "src/model/row_storage.h"
#include "src/runtime/shard_state.h"
#include "src/store/snapshot.h"
#include "src/store/snapshot_builder.h"
#include "src/testing/test_peer.h"

#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>

namespace {

using kv_index::CompiledRowLayout;
using kv_index::FieldEncoding;
using kv_index::FieldSpec;
using kv_index::FieldType;
namespace storage = kv_index::internal::model;

std::shared_ptr<const CompiledRowLayout> Layout() {
  kv_index::RuntimeSchema schema(1000);
  (void)schema.AddField(FieldSpec{.field_id = 1,
                                  .name = "score",
                                  .type = FieldType::kInt32,
                                  .is_list = false,
                                  .nullable = false,
                                  .encoding = FieldEncoding::kFixed});
  auto layout = CompiledRowLayout::Compile(schema);
  return std::make_shared<const CompiledRowLayout>(std::move(layout).value());
}

storage::EncodedRow Row(const CompiledRowLayout& layout, std::int32_t score) {
  auto row = storage::CreateEncodedRow(layout);
  (void)storage::WriteScalarField(*layout.FindField(1), score, &row);
  return row;
}

}  // namespace

int main() {
  kv_index::ForwardIndexOptions options;
  options.shard_count = 1;
  kv_index::ForwardIndex index(options);
  auto layout = Layout();
  kv_index::internal::store::SnapshotBuilder builder(layout);
  for (std::uint64_t key = 1; key <= 1024; ++key) {
    (void)builder.AddRow(key, Row(*layout, static_cast<std::int32_t>(key)));
  }
  auto backing = std::move(builder).Seal().value();
  (void)kv_index::internal::testing::ForwardIndexTestPeer::PublishShard(
      index, std::make_shared<const kv_index::internal::runtime::ShardState>(
                 0, 1,
                 kv_index::internal::runtime::ShardState::Layers{
                     .full_snapshot =
                         kv_index::internal::store::FullSnapshotView(backing)}));

  constexpr std::uint64_t kIterations = 200000;
  std::uint64_t checksum = 0;
  const auto start = std::chrono::steady_clock::now();
  for (std::uint64_t i = 0; i < kIterations; ++i) {
    auto row = index.Get((i % 1024) + 1);
    checksum += static_cast<std::uint64_t>(row->Get<std::int32_t>(1).value());
  }
  const auto elapsed = std::chrono::steady_clock::now() - start;
  const auto micros =
      std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
  std::cout << "get iterations=" << kIterations << " micros=" << micros
            << " checksum=" << checksum << "\n";
  return 0;
}
