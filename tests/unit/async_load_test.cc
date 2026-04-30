#include "kv_index/forward_index.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "kv_index/row.h"
#include "kv_index/schema.h"
#include "src/core/async_load.h"
#include "src/core/row_storage.h"
#include "src/core/shard_state.h"
#include "src/core/snapshot.h"
#include "src/core/snapshot_builder.h"
#include "src/core/test_peer.h"
#include "tests/test_support/artifact_writer.h"
#include "tests/test_support/test_macros.h"

namespace {

namespace storage = kv_index::internal;

using kv_index::CompiledRowLayout;
using kv_index::FieldEncoding;
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
using kv_index::core::ShardState;
using kv_index::core::SnapshotBuilder;
using kv_index::test_support::ArtifactShardSpec;
using kv_index::test_support::TestArtifactSpec;
using kv_index::test_support::TestSourceProgress;
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
    std::uint64_t schema_version = 800) {
  RuntimeSchema schema(schema_version);
  KV_INDEX_CHECK(schema.AddField(Scalar(1, "score", FieldType::kInt32)).ok());
  auto layout = CompiledRowLayout::Compile(schema);
  KV_INDEX_CHECK(layout.ok());
  return std::make_shared<const CompiledRowLayout>(std::move(layout).value());
}

storage::EncodedRow MakeRow(const CompiledRowLayout& layout,
                            std::int32_t score) {
  auto encoded = storage::CreateEncodedRow(layout);
  const auto* field = layout.FindField(1);
  KV_INDEX_CHECK(field != nullptr);
  KV_INDEX_CHECK(storage::WriteScalarField(*field, score, &encoded).ok());
  return encoded;
}

std::shared_ptr<const OwnedSnapshotBacking> Snapshot(
    std::shared_ptr<const CompiledRowLayout> layout, std::uint64_t key,
    std::int32_t score) {
  SnapshotBuilder builder(layout);
  KV_INDEX_CHECK(builder.AddRow(key, MakeRow(*layout, score)).ok());
  auto backing = builder.Seal();
  KV_INDEX_CHECK(backing.ok());
  return backing.value();
}

std::shared_ptr<const ShardState> FullState(
    std::uint32_t shard_id, std::uint64_t generation,
    std::shared_ptr<const CompiledRowLayout> layout, std::uint64_t key,
    std::int32_t score) {
  return std::make_shared<const ShardState>(
      shard_id, generation,
      ShardState::Layers{
          .full_snapshot = FullSnapshotView(Snapshot(layout, key, score)),
      });
}

class ScopedCatchUpRunnerFactory {
 public:
  explicit ScopedCatchUpRunnerFactory(AsyncCatchUpRunnerFactory factory)
      : previous_(kv_index::core::SetAsyncCatchUpRunnerFactoryForTesting(
            std::move(factory))) {}

  ~ScopedCatchUpRunnerFactory() {
    (void)kv_index::core::SetAsyncCatchUpRunnerFactoryForTesting(
        std::move(previous_));
  }

 private:
  AsyncCatchUpRunnerFactory previous_;
};

struct FakeCatchUpControl {
  std::mutex mu;
  std::condition_variable cv;
  bool started = false;
  bool poll_entered = false;
  bool release_poll = false;
  Status capture_status = Status::Ok();
  Status poll_status = Status::Ok();
  KafkaProgress final_progress;
  std::vector<KafkaProgress> progress_observations;
  AsyncCatchUpRequest request;
  std::uint32_t capture_calls = 0;
  std::uint32_t poll_calls = 0;
};

KafkaProgress SinglePartitionProgress(std::int64_t committed_next_offset,
                                      std::int64_t high_watermark) {
  KafkaProgress progress;
  progress.partitions.push_back({
      .partition = {.topic = "updates", .partition = 0},
      .committed_next_offset = committed_next_offset,
      .high_watermark = high_watermark,
      .lag = std::max<std::int64_t>(0,
                                    high_watermark - committed_next_offset),
  });
  return progress;
}

class FakeCatchUpRunner final : public AsyncCatchUpRunner {
 public:
  explicit FakeCatchUpRunner(std::shared_ptr<FakeCatchUpControl> control)
      : control_(std::move(control)) {}

  Status Start(const AsyncCatchUpRequest& request) override {
    std::lock_guard<std::mutex> lock(control_->mu);
    control_->request = request;
    control_->started = true;
    control_->cv.notify_all();
    return Status::Ok();
  }

  StatusOr<KafkaProgress> CaptureSafeProgress() override {
    std::lock_guard<std::mutex> lock(control_->mu);
    ++control_->capture_calls;
    if (!control_->capture_status.ok()) {
      return control_->capture_status;
    }
    if (!control_->progress_observations.empty()) {
      const std::uint32_t index = std::min<std::uint32_t>(
          control_->capture_calls - 1,
          control_->progress_observations.size() - 1);
      return control_->progress_observations[index];
    }
    return control_->final_progress;
  }

  Status PollApplyCommitOnce() override {
    std::unique_lock<std::mutex> lock(control_->mu);
    ++control_->poll_calls;
    control_->poll_entered = true;
    control_->cv.notify_all();
    control_->cv.wait_for(lock, std::chrono::seconds(5), [&] {
      return control_->release_poll ||
             (control_->request.cancellation_requested != nullptr &&
              control_->request.cancellation_requested->load());
    });
    if (control_->request.cancellation_requested != nullptr &&
        control_->request.cancellation_requested->load()) {
      return Status::Cancelled("catch-up stopped");
    }
    control_->release_poll = false;
    return control_->poll_status;
  }

 private:
  std::shared_ptr<FakeCatchUpControl> control_;
};

void WaitFor(std::shared_ptr<FakeCatchUpControl> control,
             bool FakeCatchUpControl::*field) {
  std::unique_lock<std::mutex> lock(control->mu);
  KV_INDEX_CHECK(control->cv.wait_for(lock, std::chrono::seconds(5), [&] {
    return control.get()->*field;
  }));
}

void WaitForPollCalls(std::shared_ptr<FakeCatchUpControl> control,
                      std::uint32_t expected) {
  std::unique_lock<std::mutex> lock(control->mu);
  KV_INDEX_CHECK(control->cv.wait_for(lock, std::chrono::seconds(5), [&] {
    return control->poll_calls >= expected;
  }));
}

void ReleaseCatchUp(std::shared_ptr<FakeCatchUpControl> control) {
  std::lock_guard<std::mutex> lock(control->mu);
  control->release_poll = true;
  control->cv.notify_all();
}

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

std::uint64_t FindKeyForShard(const ForwardIndex& index,
                              std::uint32_t shard_id) {
  for (std::uint64_t key = 1; key < 100000; ++key) {
    if (index.ShardFor(key) == shard_id) {
      return key;
    }
  }
  std::abort();
}

TestArtifactSpec TwoShardSpec(const ForwardIndexOptions& options,
                              std::uint64_t key, std::int32_t score,
                              std::int64_t checkpoint,
                              std::int64_t high_watermark) {
  auto layout = MakeLayout();
  TestArtifactSpec spec;
  spec.artifact_id = "async-artifact";
  spec.shard_count = options.shard_count;
  spec.hash_seed = options.hash_seed;
  spec.hash_version = options.hash_version;
  spec.layout = layout;
  spec.source_progress.push_back(TestSourceProgress{
      .topic = "updates",
      .partition = 0,
      .checkpoint_next_offset = checkpoint,
      .high_watermark = high_watermark,
  });
  for (std::uint32_t shard = 0; shard < options.shard_count; ++shard) {
    ArtifactShardSpec shard_spec{.shard_id = shard};
    if (shard == 1) {
      shard_spec.rows.push_back(
          {.primary_key = key, .encoded = MakeRow(*layout, score)});
    }
    spec.shards.push_back(std::move(shard_spec));
  }
  return spec;
}

void SuccessfulLoadPublishesMmapBackedArtifactAndProgress() {
  ForwardIndexOptions options;
  options.shard_count = 2;
  options.hash_seed = 17;
  ForwardIndex index(options);
  const std::uint64_t key = FindKeyForShard(index, 1);
  const std::string path = TempPath("async_load_success.kvi");
  auto spec = TwoShardSpec(options, key, 321, 7, 7);
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  auto control = std::make_shared<FakeCatchUpControl>();
  control->final_progress = SinglePartitionProgress(7, 7);
  ScopedCatchUpRunnerFactory factory(
      [control](const ForwardIndexOptions&)
          -> StatusOr<std::unique_ptr<AsyncCatchUpRunner>> {
        return std::make_unique<FakeCatchUpRunner>(control);
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
  KV_INDEX_CHECK_EQ(state.total_shard_count, 2U);
  KV_INDEX_CHECK_EQ(state.loaded_shard_count, 2U);
  KV_INDEX_CHECK_EQ(state.prewarmed_shard_count, 2U);
  KV_INDEX_CHECK_EQ(state.cutover_shard_count, 2U);
  KV_INDEX_CHECK_EQ(state.source_progress.partitions.size(), 1U);
  KV_INDEX_CHECK_EQ(state.source_progress.partitions[0].committed_next_offset,
                    7);
  KV_INDEX_CHECK_EQ(state.shards.size(), 2U);
  KV_INDEX_CHECK(state.shards[0].cutover);
  KV_INDEX_CHECK(state.shards[1].cutover);

  const auto row = index.Get(key);
  KV_INDEX_CHECK(row.has_value());
  KV_INDEX_CHECK_EQ(row->Get<std::int32_t>(1).value(), 321);
}

void InvalidPathRemainsTerminalFailedWithRicherState() {
  ForwardIndex index(ForwardIndexOptions{});
  const std::string uri = "file:///tmp/kv_index/nonexistent-artifact";

  const kv_index::LoadId load_id = index.LoadAsync(LoadRequest{
      .artifact_uri = uri,
      .artifact_id = "missing",
  });
  WaitForTerminalState(index, load_id);
  const kv_index::LoadState state = index.GetLoadState(load_id);

  KV_INDEX_CHECK_EQ(state.code, LoadStateCode::kFailed);
  KV_INDEX_CHECK(state.terminal);
  KV_INDEX_CHECK_EQ(state.artifact_id, "missing");
  KV_INDEX_CHECK_EQ(state.artifact_uri, uri);
  KV_INDEX_CHECK(!state.message.empty());
  KV_INDEX_CHECK_EQ(state.loaded_shard_count, 0U);
  KV_INDEX_CHECK_EQ(state.cutover_shard_count, 0U);
}

void HashOrShardMismatchFailsClosedWithoutPublishing() {
  ForwardIndexOptions options;
  options.shard_count = 2;
  options.hash_seed = 17;
  ForwardIndex index(options);
  const std::uint64_t key = FindKeyForShard(index, 1);
  const std::string path = TempPath("async_load_mismatch.kvi");

  ForwardIndexOptions artifact_options = options;
  artifact_options.hash_seed = 99;
  auto spec = TwoShardSpec(artifact_options, key, 111, 3, 3);
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  const kv_index::LoadId load_id = index.LoadAsync(LoadRequest{
      .artifact_uri = path,
      .artifact_id = spec.artifact_id,
  });
  WaitForTerminalState(index, load_id);
  const kv_index::LoadState state = index.GetLoadState(load_id);

  KV_INDEX_CHECK_EQ(state.code, LoadStateCode::kFailed);
  KV_INDEX_CHECK(state.terminal);
  KV_INDEX_CHECK_EQ(state.cutover_shard_count, 0U);
  KV_INDEX_CHECK(!index.Get(key).has_value());
}

void UnsafeSourceProgressGuardFailsClosedBeforeCutover() {
  ForwardIndexOptions options;
  options.shard_count = 2;
  options.hash_seed = 17;
  ForwardIndex index(options);
  const std::uint64_t key = FindKeyForShard(index, 1);
  const std::string path = TempPath("async_load_unsafe_progress.kvi");
  auto spec = TwoShardSpec(options, key, 222, 5, 8);
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  const kv_index::LoadId load_id = index.LoadAsync(LoadRequest{
      .artifact_uri = path,
      .artifact_id = spec.artifact_id,
  });
  WaitForTerminalState(index, load_id);
  const kv_index::LoadState state = index.GetLoadState(load_id);

  KV_INDEX_CHECK_EQ(state.code, LoadStateCode::kFailed);
  KV_INDEX_CHECK(state.terminal);
  KV_INDEX_CHECK_EQ(state.loaded_shard_count, 2U);
  KV_INDEX_CHECK_EQ(state.prewarmed_shard_count, 2U);
  KV_INDEX_CHECK_EQ(state.cutover_shard_count, 0U);
  KV_INDEX_CHECK_EQ(state.source_progress.partitions.size(), 1U);
  KV_INDEX_CHECK(!index.Get(key).has_value());
}

void LoadAsyncReturnsWhileCatchUpIsStillRunningAndReadsContinue() {
  ForwardIndexOptions options;
  options.shard_count = 2;
  options.hash_seed = 17;
  ForwardIndex index(options);
  const std::uint64_t key = FindKeyForShard(index, 1);
  const std::string path = TempPath("async_load_running_catchup.kvi");
  auto spec = TwoShardSpec(options, key, 444, 5, 8);
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  auto control = std::make_shared<FakeCatchUpControl>();
  control->progress_observations.push_back(SinglePartitionProgress(5, 8));
  control->progress_observations.push_back(SinglePartitionProgress(8, 8));
  ScopedCatchUpRunnerFactory factory(
      [control](const ForwardIndexOptions&)
          -> StatusOr<std::unique_ptr<AsyncCatchUpRunner>> {
        return std::make_unique<FakeCatchUpRunner>(control);
      });

  const kv_index::LoadId load_id = index.LoadAsync(LoadRequest{
      .artifact_uri = path,
      .artifact_id = spec.artifact_id,
  });
  WaitFor(control, &FakeCatchUpControl::poll_entered);

  const kv_index::LoadState running = index.GetLoadState(load_id);
  KV_INDEX_CHECK_NE(load_id, kv_index::kInvalidLoadId);
  KV_INDEX_CHECK_EQ(running.code, LoadStateCode::kRunning);
  KV_INDEX_CHECK(!running.terminal);
  KV_INDEX_CHECK(!index.Get(key).has_value());

  ReleaseCatchUp(control);
  WaitForTerminalState(index, load_id);
  const kv_index::LoadState terminal = index.GetLoadState(load_id);
  KV_INDEX_CHECK_EQ(terminal.code, LoadStateCode::kSucceeded);
  KV_INDEX_CHECK(index.Get(key).has_value());
}

void CancellationBeforeCutoverPreventsPublish() {
  ForwardIndexOptions options;
  options.shard_count = 2;
  options.hash_seed = 17;
  ForwardIndex index(options);
  const std::uint64_t key = FindKeyForShard(index, 1);
  const std::string path = TempPath("async_load_cancel_before_cutover.kvi");
  auto spec = TwoShardSpec(options, key, 555, 5, 8);
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());
  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(
                     index, FullState(1, 41, spec.layout, key, 111))
                     .ok());
  const auto before = ForwardIndexTestPeer::LoadShard(index, 1);
  KV_INDEX_CHECK(before.ok());
  KV_INDEX_CHECK((*before) != nullptr);
  KV_INDEX_CHECK_EQ((*before)->Generation(), 41U);
  {
    auto old_row = index.Get(key);
    KV_INDEX_CHECK(old_row.has_value());
    KV_INDEX_CHECK_EQ(old_row->Get<std::int32_t>(1).value(), 111);
  }

  auto control = std::make_shared<FakeCatchUpControl>();
  control->progress_observations.push_back(SinglePartitionProgress(5, 8));
  control->progress_observations.push_back(SinglePartitionProgress(8, 8));
  ScopedCatchUpRunnerFactory factory(
      [control](const ForwardIndexOptions&)
          -> StatusOr<std::unique_ptr<AsyncCatchUpRunner>> {
        return std::make_unique<FakeCatchUpRunner>(control);
      });

  const kv_index::LoadId load_id = index.LoadAsync(LoadRequest{
      .artifact_uri = path,
      .artifact_id = spec.artifact_id,
  });
  WaitFor(control, &FakeCatchUpControl::poll_entered);
  KV_INDEX_CHECK(index.CancelLoad(load_id));

  ReleaseCatchUp(control);
  WaitForTerminalState(index, load_id);
  const kv_index::LoadState state = index.GetLoadState(load_id);
  KV_INDEX_CHECK(state.code == LoadStateCode::kCancelled ||
                 state.code == LoadStateCode::kFailed);
  KV_INDEX_CHECK(state.terminal);
  KV_INDEX_CHECK_EQ(state.cutover_shard_count, 0U);
  KV_INDEX_CHECK(state.last_error.find("cancel") != std::string::npos ||
                 state.message.find("cancel") != std::string::npos);
  auto row = index.Get(key);
  KV_INDEX_CHECK(row.has_value());
  KV_INDEX_CHECK_EQ(row->Get<std::int32_t>(1).value(), 111);
  const auto status = index.GetRuntimeStatus();
  KV_INDEX_CHECK_EQ(status.shards[1].generation, 41U);
  const auto after = ForwardIndexTestPeer::LoadShard(index, 1);
  KV_INDEX_CHECK(after.ok());
  KV_INDEX_CHECK((*after) != nullptr);
  KV_INDEX_CHECK_EQ((*after)->Generation(), 41U);
}

void CatchUpStartsAtArtifactCheckpointAndAdvancesToSafeWatermark() {
  ForwardIndexOptions options;
  options.shard_count = 2;
  options.hash_seed = 17;
  ForwardIndex index(options);
  const std::uint64_t key = FindKeyForShard(index, 1);
  const std::string path = TempPath("async_load_catchup_progress.kvi");
  auto spec = TwoShardSpec(options, key, 666, 5, 8);
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  auto control = std::make_shared<FakeCatchUpControl>();
  control->progress_observations.push_back(SinglePartitionProgress(5, 8));
  control->progress_observations.push_back(SinglePartitionProgress(8, 8));
  ScopedCatchUpRunnerFactory factory(
      [control](const ForwardIndexOptions&)
          -> StatusOr<std::unique_ptr<AsyncCatchUpRunner>> {
        return std::make_unique<FakeCatchUpRunner>(control);
      });

  const kv_index::LoadId load_id = index.LoadAsync(LoadRequest{
      .artifact_uri = path,
      .artifact_id = spec.artifact_id,
  });
  WaitFor(control, &FakeCatchUpControl::started);

  {
    std::lock_guard<std::mutex> lock(control->mu);
    KV_INDEX_CHECK_EQ(control->request.checkpoint.next_offsets.size(), 1U);
    KV_INDEX_CHECK_EQ(control->request.checkpoint.next_offsets[0].offset, 5);
    KV_INDEX_CHECK_EQ(
        control->request.safe_progress.partitions[0].high_watermark, 8);
  }

  ReleaseCatchUp(control);
  WaitForTerminalState(index, load_id);
  const kv_index::LoadState state = index.GetLoadState(load_id);
  KV_INDEX_CHECK_EQ(state.code, LoadStateCode::kSucceeded);
  KV_INDEX_CHECK_EQ(state.source_progress.partitions[0].committed_next_offset,
                    8);
  KV_INDEX_CHECK_EQ(state.source_progress.partitions[0].lag, 0);
  KV_INDEX_CHECK_EQ(state.cutover_shard_count, 2U);
}

void CatchUpCapturesPostStartSafeWatermarkEvenWhenArtifactLooksCaughtUp() {
  ForwardIndexOptions options;
  options.shard_count = 2;
  options.hash_seed = 17;
  ForwardIndex index(options);
  const std::uint64_t key = FindKeyForShard(index, 1);
  const std::string path = TempPath("async_load_post_start_safe.kvi");
  auto spec = TwoShardSpec(options, key, 888, 5, 5);
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  auto control = std::make_shared<FakeCatchUpControl>();
  control->progress_observations.push_back(SinglePartitionProgress(5, 8));
  control->progress_observations.push_back(SinglePartitionProgress(8, 8));
  ScopedCatchUpRunnerFactory factory(
      [control](const ForwardIndexOptions&)
          -> StatusOr<std::unique_ptr<AsyncCatchUpRunner>> {
        return std::make_unique<FakeCatchUpRunner>(control);
      });

  const kv_index::LoadId load_id = index.LoadAsync(LoadRequest{
      .artifact_uri = path,
      .artifact_id = spec.artifact_id,
  });
  WaitForPollCalls(control, 1);

  KV_INDEX_CHECK_EQ(index.GetLoadState(load_id).code, LoadStateCode::kRunning);
  KV_INDEX_CHECK(!index.Get(key).has_value());

  ReleaseCatchUp(control);
  WaitForTerminalState(index, load_id);
  const kv_index::LoadState state = index.GetLoadState(load_id);
  KV_INDEX_CHECK_EQ(state.code, LoadStateCode::kSucceeded);
  KV_INDEX_CHECK_EQ(state.source_progress.partitions[0].committed_next_offset,
                    8);
  KV_INDEX_CHECK_EQ(state.source_progress.partitions[0].high_watermark, 8);
  KV_INDEX_CHECK(index.Get(key).has_value());
}

void CatchUpContinuesUntilObservedSafeWatermarkIsCommitted() {
  ForwardIndexOptions options;
  options.shard_count = 2;
  options.hash_seed = 17;
  ForwardIndex index(options);
  const std::uint64_t key = FindKeyForShard(index, 1);
  const std::string path = TempPath("async_load_stale_target_retry.kvi");
  auto spec = TwoShardSpec(options, key, 999, 5, 5);
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  auto control = std::make_shared<FakeCatchUpControl>();
  control->progress_observations.push_back(SinglePartitionProgress(5, 8));
  control->progress_observations.push_back(SinglePartitionProgress(5, 8));
  control->progress_observations.push_back(SinglePartitionProgress(8, 8));
  ScopedCatchUpRunnerFactory factory(
      [control](const ForwardIndexOptions&)
          -> StatusOr<std::unique_ptr<AsyncCatchUpRunner>> {
        return std::make_unique<FakeCatchUpRunner>(control);
      });

  const kv_index::LoadId load_id = index.LoadAsync(LoadRequest{
      .artifact_uri = path,
      .artifact_id = spec.artifact_id,
  });
  WaitForPollCalls(control, 1);
  KV_INDEX_CHECK_EQ(index.GetLoadState(load_id).code, LoadStateCode::kRunning);
  KV_INDEX_CHECK(!index.Get(key).has_value());

  ReleaseCatchUp(control);
  WaitForPollCalls(control, 2);
  KV_INDEX_CHECK_EQ(index.GetLoadState(load_id).code, LoadStateCode::kRunning);
  KV_INDEX_CHECK(!index.Get(key).has_value());

  ReleaseCatchUp(control);
  WaitForTerminalState(index, load_id);
  const kv_index::LoadState state = index.GetLoadState(load_id);
  KV_INDEX_CHECK_EQ(state.code, LoadStateCode::kSucceeded);
  KV_INDEX_CHECK_EQ(state.source_progress.partitions[0].committed_next_offset,
                    8);
  KV_INDEX_CHECK_EQ(state.source_progress.partitions[0].lag, 0);
  KV_INDEX_CHECK_EQ(state.cutover_shard_count, 2U);
}

void CatchUpFailureFailsClosedWithoutPublishing() {
  ForwardIndexOptions options;
  options.shard_count = 2;
  options.hash_seed = 17;
  ForwardIndex index(options);
  const std::uint64_t key = FindKeyForShard(index, 1);
  const std::string path = TempPath("async_load_catchup_failure.kvi");
  auto spec = TwoShardSpec(options, key, 777, 5, 8);
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  auto control = std::make_shared<FakeCatchUpControl>();
  control->progress_observations.push_back(SinglePartitionProgress(5, 8));
  control->poll_status = Status::Unavailable("poll/apply/commit failed");
  ScopedCatchUpRunnerFactory factory(
      [control](const ForwardIndexOptions&)
          -> StatusOr<std::unique_ptr<AsyncCatchUpRunner>> {
        return std::make_unique<FakeCatchUpRunner>(control);
      });

  const kv_index::LoadId load_id = index.LoadAsync(LoadRequest{
      .artifact_uri = path,
      .artifact_id = spec.artifact_id,
  });
  WaitFor(control, &FakeCatchUpControl::poll_entered);

  ReleaseCatchUp(control);
  WaitForTerminalState(index, load_id);
  const kv_index::LoadState state = index.GetLoadState(load_id);
  KV_INDEX_CHECK_EQ(state.code, LoadStateCode::kFailed);
  KV_INDEX_CHECK_EQ(state.cutover_shard_count, 0U);
  KV_INDEX_CHECK(!index.Get(key).has_value());
}

int Main() {
  SuccessfulLoadPublishesMmapBackedArtifactAndProgress();
  InvalidPathRemainsTerminalFailedWithRicherState();
  HashOrShardMismatchFailsClosedWithoutPublishing();
  UnsafeSourceProgressGuardFailsClosedBeforeCutover();
  LoadAsyncReturnsWhileCatchUpIsStillRunningAndReadsContinue();
  CancellationBeforeCutoverPreventsPublish();
  CatchUpStartsAtArtifactCheckpointAndAdvancesToSafeWatermark();
  CatchUpCapturesPostStartSafeWatermarkEvenWhenArtifactLooksCaughtUp();
  CatchUpContinuesUntilObservedSafeWatermarkIsCommitted();
  CatchUpFailureFailsClosedWithoutPublishing();
  return 0;
}

}  // namespace

int main() { return Main(); }
