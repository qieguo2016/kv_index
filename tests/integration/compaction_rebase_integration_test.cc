#include "kv_index/forward_index.h"
#include "kv_index/row.h"
#include "kv_index/schema.h"
#include "kv_index/types.h"
#include "src/rebuild/compaction.h"
#include "src/rebuild/full_rebase.h"
#include "src/store/realtime_delta.h"
#include "src/model/row_storage.h"
#include "src/runtime/shard_state.h"
#include "src/store/snapshot.h"
#include "src/store/snapshot_builder.h"
#include "src/testing/test_peer.h"
#include "src/ingest/update_applier.h"
#include "tests/test_support/test_macros.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using kv_index::CompiledRowLayout;
using kv_index::FieldEncoding;
using kv_index::FieldLayout;
using kv_index::FieldSpec;
using kv_index::FieldType;
using kv_index::ForwardIndex;
using kv_index::KafkaPartition;
using kv_index::KafkaUpsertMessage;
using kv_index::RuntimeSchema;
using kv_index::SourcePosition;
using kv_index::rebuild::BuildCompactedDeltaSnapshot;
using kv_index::rebuild::BuildRebasedFullSnapshot;
using kv_index::rebuild::CompactionBuildRequest;
using kv_index::rebuild::FinishDeltaCompaction;
using kv_index::rebuild::FinishDeltaCompactionRequest;
using kv_index::rebuild::FinishFullRebase;
using kv_index::rebuild::FinishFullRebaseRequest;
using kv_index::testing::ForwardIndexTestPeer;
using kv_index::rebuild::FullRebaseBuildRequest;
using kv_index::store::FullSnapshotView;
using kv_index::store::OwnedSnapshotBacking;
using kv_index::store::RealtimeDeltaAtomicTable;
using kv_index::runtime::ShardState;
using kv_index::store::SnapshotBuildOptions;
using kv_index::store::SnapshotBuilder;
using kv_index::ingest::UpdateApplier;
using kv_index::ingest::UpdateApplierOptions;
using kv_index::ingest::UpdateGenerationRole;
using kv_index::ingest::UpdateTargetRoute;
namespace storage = kv_index::model;

enum class WireKind : std::uint8_t {
  kInt32 = 2,
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

std::vector<std::byte> PayloadScoreTitle(std::int32_t score,
                                         std::string_view title) {
  std::vector<std::byte> bytes;
  bytes.push_back(std::byte{'K'});
  bytes.push_back(std::byte{'V'});
  bytes.push_back(std::byte{'I'});
  bytes.push_back(std::byte{'U'});
  AppendLittleEndian<std::uint16_t>(1, &bytes);
  AppendLittleEndian<std::uint16_t>(0, &bytes);
  AppendLittleEndian<std::uint32_t>(2, &bytes);
  AppendFieldHeader(1, WireKind::kInt32, sizeof(std::int32_t), &bytes);
  AppendLittleEndian<std::uint32_t>(static_cast<std::uint32_t>(score), &bytes);
  AppendFieldHeader(2, WireKind::kString,
                    static_cast<std::uint32_t>(title.size()), &bytes);
  bytes.insert(bytes.end(), reinterpret_cast<const std::byte*>(title.data()),
               reinterpret_cast<const std::byte*>(title.data()) +
                   title.size());
  return bytes;
}

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
  RuntimeSchema schema(720);
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
    SnapshotBuildOptions options) {
  SnapshotBuilder builder(layout, options);
  for (const auto& [primary_key, encoded] : rows) {
    KV_INDEX_CHECK(builder.AddRow(primary_key, encoded).ok());
  }
  auto backing = builder.Seal();
  KV_INDEX_CHECK(backing.ok());
  return backing.value();
}

std::shared_ptr<RealtimeDeltaAtomicTable> MakeRealtime(
    std::shared_ptr<const CompiledRowLayout> layout,
    const kv_index::ForwardIndexOptions& options) {
  return std::make_shared<RealtimeDeltaAtomicTable>(
      RealtimeDeltaAtomicTable::Options{
          .layout = std::move(layout),
          .capacity = 16,
          .hash_seed = options.hash_seed,
          .hash_version = options.hash_version,
      });
}

UpdateTargetRoute Route(
    UpdateGenerationRole role, std::uint64_t generation_id,
    std::shared_ptr<const CompiledRowLayout> layout,
    std::shared_ptr<RealtimeDeltaAtomicTable> table,
    const kv_index::ForwardIndexOptions& options) {
  return UpdateTargetRoute{
      .role = role,
      .generation_id = generation_id,
      .shard_count = options.shard_count,
      .hash_seed = options.hash_seed,
      .hash_version = options.hash_version,
      .layout = std::move(layout),
      .realtime_shards = {std::move(table)},
  };
}

KafkaUpsertMessage Message(std::uint64_t primary_key, std::int64_t offset,
                           std::int32_t score, std::string_view title) {
  return KafkaUpsertMessage{
      .metadata =
          {
              .partition = KafkaPartition{.topic = "updates", .partition = 0},
              .offset = offset,
              .key = std::to_string(primary_key),
          },
      .primary_key = primary_key,
      .payload = PayloadScoreTitle(score, title),
  };
}

UpdateApplier ApplierWith(UpdateTargetRoute route) {
  return UpdateApplier(UpdateApplierOptions{
      .logical_topic = "updates",
      .targets = {std::move(route)},
  });
}

void CheckIndexRow(const ForwardIndex& index, std::uint64_t primary_key,
                   std::int32_t score, std::string_view title) {
  auto row = index.Get(primary_key);
  KV_INDEX_CHECK(row.has_value());
  KV_INDEX_CHECK_EQ(row->Get<std::int32_t>(1).value(), score);
  KV_INDEX_CHECK_EQ(row->Get<std::string>(2).value(), std::string(title));
}

}  // namespace

int main() {
  kv_index::ForwardIndexOptions options;
  options.shard_count = 1;
  options.hash_seed = 20260430;
  options.hash_version = 1;
  ForwardIndex index(options);
  auto layout = MakeLayout();
  const SnapshotBuildOptions snapshot_options{
      .hash_seed = options.hash_seed,
      .hash_version = options.hash_version,
  };

  auto full = BuildSnapshot(
      layout, {{200, MakeRow(*layout, 20, std::string("full"))}},
      snapshot_options);
  auto active_realtime = MakeRealtime(layout, options);
  auto active_applier = ApplierWith(Route(UpdateGenerationRole::kActive, 1,
                                          layout, active_realtime, options));
  KV_INDEX_CHECK(
      active_applier.Apply(Message(100, 1, 10, "active-sealed")).ok());
  ShardState active_state(
      0, 1,
      ShardState::Layers{
          .realtime_delta = active_realtime,
          .full_snapshot = FullSnapshotView(full),
      });
  KV_INDEX_CHECK(
      ForwardIndexTestPeer::PublishShard(
          index, std::make_shared<const ShardState>(active_state))
          .ok());

  auto boundary = active_realtime->CaptureCompactionBoundary();
  auto compaction_realtime = MakeRealtime(layout, options);
  auto compaction_applier = ApplierWith(Route(
      UpdateGenerationRole::kCompaction, 2, layout, compaction_realtime,
      options));
  KV_INDEX_CHECK(
      compaction_applier.Apply(Message(100, 2, 30, "during-compaction")).ok());
  auto compact = BuildCompactedDeltaSnapshot(CompactionBuildRequest{
      .state = active_state,
      .boundary = boundary,
      .build_options = snapshot_options,
  });
  KV_INDEX_CHECK(compact.ok());
  auto compacted_state = FinishDeltaCompaction(FinishDeltaCompactionRequest{
      .previous = active_state,
      .successor_generation = 2,
      .successor_realtime = compaction_realtime,
      .compact_backing = compact.value(),
  });
  KV_INDEX_CHECK(compacted_state.ok());
  KV_INDEX_CHECK(
      ForwardIndexTestPeer::PublishShard(index, compacted_state.value()).ok());
  CheckIndexRow(index, 100, 30, "during-compaction");
  CheckIndexRow(index, 200, 20, "full");

  auto drain_boundary = compaction_realtime->CaptureCompactionBoundary();
  auto drained_realtime = MakeRealtime(layout, options);
  auto drained_compact = BuildCompactedDeltaSnapshot(CompactionBuildRequest{
      .state = *compacted_state.value(),
      .boundary = drain_boundary,
      .build_options = snapshot_options,
  });
  KV_INDEX_CHECK(drained_compact.ok());
  auto rebase_ready_state = FinishDeltaCompaction(FinishDeltaCompactionRequest{
      .previous = *compacted_state.value(),
      .successor_generation = 3,
      .successor_realtime = drained_realtime,
      .compact_backing = drained_compact.value(),
  });
  KV_INDEX_CHECK(rebase_ready_state.ok());
  KV_INDEX_CHECK(
      ForwardIndexTestPeer::PublishShard(index, rebase_ready_state.value())
          .ok());
  CheckIndexRow(index, 100, 30, "during-compaction");
  CheckIndexRow(index, 200, 20, "full");

  auto rebased = BuildRebasedFullSnapshot(FullRebaseBuildRequest{
      .state = *rebase_ready_state.value(),
      .build_options = snapshot_options,
  });
  KV_INDEX_CHECK(rebased.ok());
  auto rebase_realtime = MakeRealtime(layout, options);
  auto rebase_applier = ApplierWith(Route(UpdateGenerationRole::kRebase, 4,
                                          layout, rebase_realtime, options));
  KV_INDEX_CHECK(
      rebase_applier.Apply(Message(100, 3, 40, "during-rebase")).ok());
  KV_INDEX_CHECK(
      rebase_applier.Apply(Message(300, 4, 50, "new-rebase")).ok());
  auto rebase_state = FinishFullRebase(FinishFullRebaseRequest{
      .previous = *rebase_ready_state.value(),
      .successor_generation = 4,
      .rebase_realtime = rebase_realtime,
      .full_backing = rebased.value(),
  });
  KV_INDEX_CHECK(rebase_state.ok());
  KV_INDEX_CHECK(!rebase_state.value()->compact_delta().has_value());
  KV_INDEX_CHECK(
      ForwardIndexTestPeer::PublishShard(index, rebase_state.value()).ok());

  CheckIndexRow(index, 100, 40, "during-rebase");
  CheckIndexRow(index, 200, 20, "full");
  CheckIndexRow(index, 300, 50, "new-rebase");
  return 0;
}
