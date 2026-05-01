#include "kv_index/forward_index.h"
#include "kv_index/schema.h"
#include "src/rebuild/async_load.h"
#include "src/model/row_storage.h"
#include "src/runtime/shard_state.h"
#include "src/store/snapshot.h"
#include "src/store/snapshot_builder.h"
#include "src/testing/test_peer.h"
#include "tests/test_support/artifact_writer.h"
#include "tests/test_support/test_macros.h"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>

namespace {

using kv_index::CompiledRowLayout;
using kv_index::FieldEncoding;
using kv_index::FieldLayout;
using kv_index::FieldSpec;
using kv_index::FieldType;
using kv_index::ForwardIndex;
using kv_index::ForwardIndexOptions;
using kv_index::KafkaProgress;
using kv_index::LoadRequest;
using kv_index::LoadStateCode;
using kv_index::RuntimeSchema;
using kv_index::Status;
using kv_index::StatusOr;
using kv_index::core::AsyncCatchUpRequest;
using kv_index::core::AsyncCatchUpRunner;
using kv_index::core::AsyncCatchUpRunnerFactory;
using kv_index::core::ForwardIndexTestPeer;
using kv_index::core::FullSnapshotView;
using kv_index::core::OwnedSnapshotBacking;
using kv_index::core::SetAsyncCatchUpRunnerFactoryForTesting;
using kv_index::core::ShardState;
using kv_index::core::SnapshotBuilder;
using kv_index::test_support::ArtifactShardSpec;
using kv_index::test_support::TestArtifactSpec;
using kv_index::test_support::TestSourceProgress;
using kv_index::test_support::WriteTestArtifact;
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

std::shared_ptr<const CompiledRowLayout> MakeLayout(
    std::uint64_t schema_version, bool include_title) {
  RuntimeSchema schema(schema_version);
  KV_INDEX_CHECK(schema.AddField(Scalar(1, "score", FieldType::kInt32)).ok());
  if (include_title) {
    KV_INDEX_CHECK(
        schema.AddField(Scalar(2, "title", FieldType::kString)).ok());
  }
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
    std::shared_ptr<const CompiledRowLayout> layout, std::uint64_t key,
    storage::EncodedRow row) {
  SnapshotBuilder builder(layout);
  KV_INDEX_CHECK(builder.AddRow(key, std::move(row)).ok());
  auto backing = builder.Seal();
  KV_INDEX_CHECK(backing.ok());
  return backing.value();
}

std::string TempPath(const std::string& name) {
  const char* tmpdir = std::getenv("TEST_TMPDIR");
  if (tmpdir == nullptr) {
    tmpdir = "/tmp";
  }
  return std::string(tmpdir) + "/kv_index_w08_" + name;
}

class NoopCatchUpRunner final : public AsyncCatchUpRunner {
 public:
  Status Start(const AsyncCatchUpRequest& request) override {
    progress_ = request.safe_progress;
    return Status::Ok();
  }

  StatusOr<KafkaProgress> CaptureSafeProgress() override { return progress_; }
  Status PollApplyCommitOnce() override { return Status::Ok(); }

 private:
  KafkaProgress progress_;
};

class ScopedCatchUpFactory {
 public:
  ScopedCatchUpFactory()
      : previous_(SetAsyncCatchUpRunnerFactoryForTesting(
            [](const ForwardIndexOptions&)
                -> StatusOr<std::unique_ptr<AsyncCatchUpRunner>> {
              return std::make_unique<NoopCatchUpRunner>();
            })) {}

  ~ScopedCatchUpFactory() {
    (void)SetAsyncCatchUpRunnerFactoryForTesting(std::move(previous_));
  }

 private:
  AsyncCatchUpRunnerFactory previous_;
};

void WaitForTerminal(const ForwardIndex& index, kv_index::LoadId load_id) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (std::chrono::steady_clock::now() < deadline) {
    if (index.GetLoadState(load_id).terminal) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  KV_INDEX_CHECK(index.GetLoadState(load_id).terminal);
}

}  // namespace

int main() {
  ForwardIndexOptions options;
  options.shard_count = 1;
  ForwardIndex index(options);
  const std::uint64_t key = 100;
  auto old_layout = MakeLayout(919, false);
  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(
                     index,
                     std::make_shared<const ShardState>(
                         0, 1,
                         ShardState::Layers{
                             .full_snapshot = FullSnapshotView(Snapshot(
                                 old_layout, key,
                                 MakeRow(*old_layout, 10, std::nullopt))),
                         }))
                     .ok());
  auto pinned_old = index.Get(key);
  KV_INDEX_CHECK(pinned_old.has_value());
  KV_INDEX_CHECK_EQ(pinned_old->Get<std::int32_t>(1).value(), 10);
  KV_INDEX_CHECK(!pinned_old->Get<std::string>(2).has_value());

  auto new_layout = MakeLayout(920, true);
  TestArtifactSpec spec;
  spec.artifact_id = "schema-v2";
  spec.shard_count = options.shard_count;
  spec.hash_seed = options.hash_seed;
  spec.hash_version = options.hash_version;
  spec.layout = new_layout;
  spec.source_progress.push_back(TestSourceProgress{
      .topic = "updates",
      .partition = 0,
      .checkpoint_next_offset = 7,
      .high_watermark = 7,
  });
  spec.shards.push_back(ArtifactShardSpec{
      .shard_id = 0,
      .rows = {{.primary_key = key,
                .encoded = MakeRow(*new_layout, 20, "schema-v2")}},
  });
  const std::string path = TempPath("schema_evolution.kvi");
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  ScopedCatchUpFactory factory;
  const kv_index::LoadId load_id = index.LoadAsync(
      LoadRequest{.artifact_uri = path, .artifact_id = spec.artifact_id});
  WaitForTerminal(index, load_id);
  KV_INDEX_CHECK_EQ(index.GetLoadState(load_id).code, LoadStateCode::kSucceeded);

  auto row = index.Get(key);
  KV_INDEX_CHECK(row.has_value());
  KV_INDEX_CHECK_EQ(row->Get<std::int32_t>(1).value(), 20);
  KV_INDEX_CHECK_EQ(row->Get<std::string>(2).value(), "schema-v2");
  KV_INDEX_CHECK_EQ(index.GetRuntimeStatus().shards[0].schema_version, 920U);
  KV_INDEX_CHECK_EQ(pinned_old->Get<std::int32_t>(1).value(), 10);
  KV_INDEX_CHECK(!pinned_old->Get<std::string>(2).has_value());
  return 0;
}
