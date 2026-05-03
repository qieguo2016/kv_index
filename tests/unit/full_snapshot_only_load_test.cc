#include "kv_index/forward_index.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "kv_index/row.h"
#include "kv_index/schema.h"
#include "kv_index/status.h"
#include "src/model/row_storage.h"
#include "src/rebuild/async_load.h"
#include "tests/test_support/artifact_writer.h"
#include "tests/test_support/test_macros.h"

namespace {

namespace storage = kv_index::model;

using kv_index::CompiledRowLayout;
using kv_index::FieldEncoding;
using kv_index::FieldLayout;
using kv_index::FieldSpec;
using kv_index::FieldType;
using kv_index::ForwardIndex;
using kv_index::ForwardIndexMode;
using kv_index::ForwardIndexOptions;
using kv_index::LoadRequest;
using kv_index::LoadStateCode;
using kv_index::RuntimeSchema;
using kv_index::Status;
using kv_index::StatusOr;
using kv_index::rebuild::AsyncCatchUpRunner;
using kv_index::rebuild::AsyncCatchUpRunnerFactory;
using kv_index::test_support::ArtifactShardSpec;
using kv_index::test_support::TestArtifactSpec;
using kv_index::test_support::TestArtifactRow;
using kv_index::test_support::WriteTestArtifact;

std::string TempPath(const std::string& name) {
  const char* tmpdir = std::getenv("TEST_TMPDIR");
  if (tmpdir == nullptr) {
    tmpdir = "/tmp";
  }
  return std::string(tmpdir) + "/kv_index_" + name;
}

FieldSpec Scalar(kv_index::FieldId field_id, std::string name,
                 FieldType type) {
  return FieldSpec{
      .field_id = field_id,
      .name = std::move(name),
      .type = type,
      .is_list = false,
      .nullable = false,
      .encoding = type == FieldType::kString ? FieldEncoding::kArena
                                             : FieldEncoding::kFixed,
  };
}

std::shared_ptr<const CompiledRowLayout> MakeLayout(
    std::uint64_t schema_version = 900) {
  RuntimeSchema schema(schema_version);
  KV_INDEX_CHECK(schema.AddField(Scalar(1, "score", FieldType::kInt32)).ok());
  auto layout = CompiledRowLayout::Compile(schema);
  KV_INDEX_CHECK(layout.ok());
  return std::make_shared<const CompiledRowLayout>(std::move(layout).value());
}

std::shared_ptr<const CompiledRowLayout> MakeLayoutWithTitle(
    std::uint64_t schema_version) {
  RuntimeSchema schema(schema_version);
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
                            std::int32_t score) {
  auto encoded = storage::CreateEncodedRow(layout);
  KV_INDEX_CHECK(
      storage::WriteScalarField(Field(layout, 1), score, &encoded).ok());
  return encoded;
}

storage::EncodedRow MakeRow(const CompiledRowLayout& layout,
                            std::int32_t score,
                            std::optional<std::string> title) {
  auto encoded = MakeRow(layout, score);
  if (title.has_value()) {
    KV_INDEX_CHECK(
        storage::WriteArenaStringField(Field(layout, 2), *title, &encoded)
            .ok());
  }
  return encoded;
}

class ScopedCatchUpRunnerFactory {
 public:
  explicit ScopedCatchUpRunnerFactory(AsyncCatchUpRunnerFactory factory)
      : previous_(kv_index::rebuild::SetAsyncCatchUpRunnerFactoryForTesting(
            std::move(factory))) {}

  ~ScopedCatchUpRunnerFactory() {
    (void)kv_index::rebuild::SetAsyncCatchUpRunnerFactoryForTesting(
        std::move(previous_));
  }

 private:
  AsyncCatchUpRunnerFactory previous_;
};

void WaitForTerminalState(const ForwardIndex& index,
                          kv_index::LoadId load_id) {
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

TestArtifactSpec FullOnlySpec(const ForwardIndexOptions& options,
                              std::string artifact_id,
                              std::shared_ptr<const CompiledRowLayout> layout,
                              std::vector<TestArtifactRow> rows) {
  TestArtifactSpec spec;
  spec.artifact_id = std::move(artifact_id);
  spec.shard_count = options.shard_count;
  spec.hash_seed = options.hash_seed;
  spec.hash_version = options.hash_version;
  spec.layout = std::move(layout);
  spec.include_source_progress_section = false;
  spec.shards.push_back(ArtifactShardSpec{
      .shard_id = 0,
      .rows = std::move(rows),
  });
  return spec;
}

TestArtifactSpec FullOnlySpec(const ForwardIndexOptions& options,
                              std::uint64_t key, std::int32_t score) {
  auto layout = MakeLayout();
  return FullOnlySpec(
      options, "full-only-no-progress", layout,
      {{.primary_key = key, .encoded = MakeRow(*layout, score)}});
}

void FullOnlyLoadPublishesNoSourceProgressArtifactWithoutCatchUp() {
  ForwardIndexOptions options;
  options.mode = ForwardIndexMode::kFullSnapshotOnly;
  options.shard_count = 1;
  options.hash_seed = 23;
  options.hash_version = 1;
  ForwardIndex index(options);

  const std::uint64_t key = 42;
  const std::int32_t score = 1234;
  const std::string path = TempPath("full_only_no_source_progress.kvi");
  const auto spec = FullOnlySpec(options, key, score);
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  std::atomic_bool catch_up_factory_called = false;
  ScopedCatchUpRunnerFactory factory(
      [&catch_up_factory_called](const ForwardIndexOptions&)
          -> StatusOr<std::unique_ptr<AsyncCatchUpRunner>> {
        catch_up_factory_called.store(true);
        return Status::Internal(
            "catch-up should not be created in full-only mode");
      });

  const kv_index::LoadId load_id = index.LoadAsync(LoadRequest{
      .artifact_uri = path,
      .artifact_id = spec.artifact_id,
  });
  WaitForTerminalState(index, load_id);
  const kv_index::LoadState state = index.GetLoadState(load_id);

  KV_INDEX_CHECK_NE(load_id, kv_index::kInvalidLoadId);
  KV_INDEX_CHECK_EQ(state.id, load_id);
  KV_INDEX_CHECK_EQ(state.code, LoadStateCode::kSucceeded);
  KV_INDEX_CHECK(state.terminal);
  KV_INDEX_CHECK_EQ(state.artifact_id, spec.artifact_id);
  KV_INDEX_CHECK_EQ(state.artifact_uri, path);
  KV_INDEX_CHECK_EQ(state.total_shard_count, 1U);
  KV_INDEX_CHECK_EQ(state.loaded_shard_count, 1U);
  KV_INDEX_CHECK_EQ(state.prewarmed_shard_count, 1U);
  KV_INDEX_CHECK_EQ(state.cutover_shard_count, 1U);
  KV_INDEX_CHECK(state.source_progress.partitions.empty());
  KV_INDEX_CHECK_EQ(state.shards.size(), 1U);
  KV_INDEX_CHECK(state.shards[0].loaded);
  KV_INDEX_CHECK(state.shards[0].prewarmed);
  KV_INDEX_CHECK(state.shards[0].cutover);
  KV_INDEX_CHECK(!catch_up_factory_called.load());

  const auto row = index.Get(key);
  KV_INDEX_CHECK(row.has_value());
  KV_INDEX_CHECK_EQ(row->Get<std::int32_t>(1).value(), score);
}

void FullOnlyLoadReplacesPreviousFullSnapshot() {
  ForwardIndexOptions options;
  options.mode = ForwardIndexMode::kFullSnapshotOnly;
  options.shard_count = 1;
  options.hash_seed = 23;
  options.hash_version = 1;
  ForwardIndex index(options);

  auto layout = MakeLayout();
  const std::uint64_t old_key = 100;
  const std::uint64_t new_key = 200;
  const std::string old_path = TempPath("full_only_replace_old.kvi");
  const auto old_spec = FullOnlySpec(
      options, "full-only-replace-a", layout,
      {{.primary_key = old_key, .encoded = MakeRow(*layout, 10)}});
  KV_INDEX_CHECK(WriteTestArtifact(old_path, old_spec).ok());

  const kv_index::LoadId old_load_id = index.LoadAsync(LoadRequest{
      .artifact_uri = old_path,
      .artifact_id = old_spec.artifact_id,
  });
  WaitForTerminalState(index, old_load_id);
  KV_INDEX_CHECK_EQ(index.GetLoadState(old_load_id).code,
                    LoadStateCode::kSucceeded);
  auto old_row = index.Get(old_key);
  KV_INDEX_CHECK(old_row.has_value());
  KV_INDEX_CHECK_EQ(old_row->Get<std::int32_t>(1).value(), 10);

  const std::string new_path = TempPath("full_only_replace_new.kvi");
  const auto new_spec = FullOnlySpec(
      options, "full-only-replace-b", layout,
      {{.primary_key = new_key, .encoded = MakeRow(*layout, 20)}});
  KV_INDEX_CHECK(WriteTestArtifact(new_path, new_spec).ok());

  const kv_index::LoadId new_load_id = index.LoadAsync(LoadRequest{
      .artifact_uri = new_path,
      .artifact_id = new_spec.artifact_id,
  });
  WaitForTerminalState(index, new_load_id);
  KV_INDEX_CHECK_EQ(index.GetLoadState(new_load_id).code,
                    LoadStateCode::kSucceeded);

  KV_INDEX_CHECK(!index.Get(old_key).has_value());
  const auto new_row = index.Get(new_key);
  KV_INDEX_CHECK(new_row.has_value());
  KV_INDEX_CHECK_EQ(new_row->Get<std::int32_t>(1).value(), 20);
}

void FullOnlyRuntimeStatusReportsOnlyMmapFullSnapshot() {
  ForwardIndexOptions options;
  options.mode = ForwardIndexMode::kFullSnapshotOnly;
  options.shard_count = 1;
  options.hash_seed = 23;
  options.hash_version = 1;
  ForwardIndex index(options);

  auto layout = MakeLayout(901);
  const std::string path = TempPath("full_only_runtime_status.kvi");
  const auto spec = FullOnlySpec(
      options, "full-only-runtime-status", layout,
      {{.primary_key = 100, .encoded = MakeRow(*layout, 10)},
       {.primary_key = 200, .encoded = MakeRow(*layout, 20)}});
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  const kv_index::LoadId load_id = index.LoadAsync(LoadRequest{
      .artifact_uri = path,
      .artifact_id = spec.artifact_id,
  });
  WaitForTerminalState(index, load_id);
  KV_INDEX_CHECK_EQ(index.GetLoadState(load_id).code,
                    LoadStateCode::kSucceeded);

  const auto status = index.GetRuntimeStatus();
  KV_INDEX_CHECK_EQ(status.shard_count, 1U);
  KV_INDEX_CHECK_EQ(status.shards.size(), 1U);
  const auto& shard = status.shards[0];
  KV_INDEX_CHECK_EQ(shard.shard_id, 0U);
  KV_INDEX_CHECK(shard.has_full_snapshot);
  KV_INDEX_CHECK(!shard.has_realtime_delta);
  KV_INDEX_CHECK(!shard.has_compact_delta);
  KV_INDEX_CHECK_EQ(shard.full_row_count, 2U);
  KV_INDEX_CHECK_EQ(shard.compact_row_count, 0U);
  KV_INDEX_CHECK_EQ(shard.artifact_id, spec.artifact_id);
  KV_INDEX_CHECK_EQ(shard.schema_version, layout->schema_version());
  KV_INDEX_CHECK_EQ(shard.layout_fingerprint, layout->layout_fingerprint());
}

void FullOnlySchemaCutoverKeepsPinnedOldRowReadable() {
  ForwardIndexOptions options;
  options.mode = ForwardIndexMode::kFullSnapshotOnly;
  options.shard_count = 1;
  options.hash_seed = 23;
  options.hash_version = 1;
  ForwardIndex index(options);

  const std::uint64_t key = 100;
  auto old_layout = MakeLayout(910);
  const std::string old_path = TempPath("full_only_schema_old.kvi");
  const auto old_spec = FullOnlySpec(
      options, "full-only-schema-v1", old_layout,
      {{.primary_key = key, .encoded = MakeRow(*old_layout, 10)}});
  KV_INDEX_CHECK(WriteTestArtifact(old_path, old_spec).ok());

  const kv_index::LoadId old_load_id = index.LoadAsync(LoadRequest{
      .artifact_uri = old_path,
      .artifact_id = old_spec.artifact_id,
  });
  WaitForTerminalState(index, old_load_id);
  KV_INDEX_CHECK_EQ(index.GetLoadState(old_load_id).code,
                    LoadStateCode::kSucceeded);
  auto pinned_old = index.Get(key);
  KV_INDEX_CHECK(pinned_old.has_value());
  KV_INDEX_CHECK_EQ(pinned_old->Get<std::int32_t>(1).value(), 10);
  KV_INDEX_CHECK(!pinned_old->Has(2));
  KV_INDEX_CHECK(!pinned_old->Get<std::string>(2).has_value());

  auto new_layout = MakeLayoutWithTitle(old_layout->schema_version() + 1);
  const std::string new_path = TempPath("full_only_schema_new.kvi");
  const auto new_spec = FullOnlySpec(
      options, "full-only-schema-v2", new_layout,
      {{.primary_key = key,
        .encoded = MakeRow(*new_layout, 20, std::string("schema-v2"))}});
  KV_INDEX_CHECK(WriteTestArtifact(new_path, new_spec).ok());

  const kv_index::LoadId new_load_id = index.LoadAsync(LoadRequest{
      .artifact_uri = new_path,
      .artifact_id = new_spec.artifact_id,
  });
  WaitForTerminalState(index, new_load_id);
  KV_INDEX_CHECK_EQ(index.GetLoadState(new_load_id).code,
                    LoadStateCode::kSucceeded);

  const auto fresh = index.Get(key);
  KV_INDEX_CHECK(fresh.has_value());
  KV_INDEX_CHECK_EQ(fresh->Get<std::int32_t>(1).value(), 20);
  KV_INDEX_CHECK_EQ(fresh->Get<std::string>(2).value(),
                    std::string("schema-v2"));
  const auto status = index.GetRuntimeStatus();
  KV_INDEX_CHECK_EQ(status.shards[0].schema_version,
                    new_layout->schema_version());
  KV_INDEX_CHECK_EQ(status.shards[0].layout_fingerprint,
                    new_layout->layout_fingerprint());
  KV_INDEX_CHECK_EQ(status.shards[0].artifact_id, new_spec.artifact_id);

  KV_INDEX_CHECK_EQ(pinned_old->Get<std::int32_t>(1).value(), 10);
  KV_INDEX_CHECK(!pinned_old->Has(2));
  KV_INDEX_CHECK(!pinned_old->Get<std::string>(2).has_value());
}

int Main() {
  FullOnlyLoadPublishesNoSourceProgressArtifactWithoutCatchUp();
  FullOnlyLoadReplacesPreviousFullSnapshot();
  FullOnlyRuntimeStatusReportsOnlyMmapFullSnapshot();
  FullOnlySchemaCutoverKeepsPinnedOldRowReadable();
  return 0;
}

}  // namespace

int main() { return Main(); }
