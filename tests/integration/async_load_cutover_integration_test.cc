#include "kv_index/forward_index.h"
#include "kv_index/schema.h"
#include "src/core/async_load.h"
#include "src/core/row_storage.h"
#include "src/core/shard_state.h"
#include "src/core/snapshot.h"
#include "src/core/snapshot_builder.h"
#include "src/core/test_peer.h"
#include "tests/test_support/artifact_writer.h"
#include "tests/test_support/test_macros.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
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
    std::uint64_t schema_version = 910) {
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
                            std::int32_t score, std::string title) {
  auto encoded = storage::CreateEncodedRow(layout);
  KV_INDEX_CHECK(
      storage::WriteScalarField(Field(layout, 1), score, &encoded).ok());
  KV_INDEX_CHECK(
      storage::WriteArenaStringField(Field(layout, 2), title, &encoded).ok());
  return encoded;
}

std::shared_ptr<const OwnedSnapshotBacking> Snapshot(
    std::shared_ptr<const CompiledRowLayout> layout, std::uint64_t key,
    std::int32_t score, std::string title) {
  SnapshotBuilder builder(layout);
  KV_INDEX_CHECK(builder.AddRow(key, MakeRow(*layout, score, std::move(title)))
                     .ok());
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

KafkaProgress Progress(std::int64_t committed, std::int64_t high_watermark) {
  KafkaProgress progress;
  progress.partitions.push_back({
      .partition = {.topic = "updates", .partition = 0},
      .committed_next_offset = committed,
      .high_watermark = high_watermark,
      .lag = std::max<std::int64_t>(0, high_watermark - committed),
  });
  return progress;
}

struct Control {
  std::mutex mu;
  std::condition_variable cv;
  bool poll_entered = false;
  bool release_poll = false;
  std::uint32_t capture_calls = 0;
};

class BlockingCatchUpRunner final : public AsyncCatchUpRunner {
 public:
  explicit BlockingCatchUpRunner(std::shared_ptr<Control> control)
      : control_(std::move(control)) {}

  Status Start(const AsyncCatchUpRequest&) override { return Status::Ok(); }

  StatusOr<KafkaProgress> CaptureSafeProgress() override {
    std::lock_guard<std::mutex> lock(control_->mu);
    ++control_->capture_calls;
    if (control_->capture_calls == 1) {
      return Progress(5, 8);
    }
    return Progress(8, 8);
  }

  Status PollApplyCommitOnce() override {
    std::unique_lock<std::mutex> lock(control_->mu);
    control_->poll_entered = true;
    control_->cv.notify_all();
    control_->cv.wait_for(lock, std::chrono::seconds(5),
                          [&] { return control_->release_poll; });
    return Status::Ok();
  }

 private:
  std::shared_ptr<Control> control_;
};

class ScopedCatchUpFactory {
 public:
  explicit ScopedCatchUpFactory(std::shared_ptr<Control> control)
      : previous_(SetAsyncCatchUpRunnerFactoryForTesting(
            [control = std::move(control)](const ForwardIndexOptions&)
                -> StatusOr<std::unique_ptr<AsyncCatchUpRunner>> {
              return std::make_unique<BlockingCatchUpRunner>(control);
            })) {}

  ~ScopedCatchUpFactory() {
    (void)SetAsyncCatchUpRunnerFactoryForTesting(std::move(previous_));
  }

 private:
  AsyncCatchUpRunnerFactory previous_;
};

void WaitForPoll(std::shared_ptr<Control> control) {
  std::unique_lock<std::mutex> lock(control->mu);
  KV_INDEX_CHECK(control->cv.wait_for(lock, std::chrono::seconds(5), [&] {
    return control->poll_entered;
  }));
}

void ReleasePoll(std::shared_ptr<Control> control) {
  std::lock_guard<std::mutex> lock(control->mu);
  control->release_poll = true;
  control->cv.notify_all();
}

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

void CheckRow(const ForwardIndex& index, std::uint64_t key,
              std::int32_t score, const std::string& title) {
  auto row = index.Get(key);
  KV_INDEX_CHECK(row.has_value());
  KV_INDEX_CHECK_EQ(row->Get<std::int32_t>(1).value(), score);
  KV_INDEX_CHECK_EQ(row->Get<std::string>(2).value(), title);
}

}  // namespace

int main() {
  ForwardIndexOptions options;
  options.shard_count = 1;
  ForwardIndex index(options);
  const std::uint64_t key = 100;
  auto layout = MakeLayout();
  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(
                     index,
                     std::make_shared<const ShardState>(
                         0, 1,
                         ShardState::Layers{
                             .full_snapshot = FullSnapshotView(
                                 Snapshot(layout, key, 10, "old")),
                         }))
                     .ok());

  TestArtifactSpec spec;
  spec.artifact_id = "async-cutover";
  spec.shard_count = options.shard_count;
  spec.hash_seed = options.hash_seed;
  spec.hash_version = options.hash_version;
  spec.layout = layout;
  spec.source_progress.push_back(TestSourceProgress{
      .topic = "updates",
      .partition = 0,
      .checkpoint_next_offset = 5,
      .high_watermark = 8,
  });
  spec.shards.push_back(ArtifactShardSpec{
      .shard_id = 0,
      .rows = {{.primary_key = key,
                .encoded = MakeRow(*layout, 20, "new-artifact")}},
  });
  const std::string path = TempPath("async_cutover.kvi");
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  auto control = std::make_shared<Control>();
  ScopedCatchUpFactory factory(control);
  const kv_index::LoadId load_id = index.LoadAsync(
      LoadRequest{.artifact_uri = path, .artifact_id = spec.artifact_id});
  WaitForPoll(control);

  KV_INDEX_CHECK_EQ(index.GetLoadState(load_id).code, LoadStateCode::kRunning);
  CheckRow(index, key, 10, "old");

  ReleasePoll(control);
  WaitForTerminal(index, load_id);
  const auto state = index.GetLoadState(load_id);
  KV_INDEX_CHECK_EQ(state.code, LoadStateCode::kSucceeded);
  KV_INDEX_CHECK_EQ(state.cutover_shard_count, 1U);
  CheckRow(index, key, 20, "new-artifact");
  return 0;
}
