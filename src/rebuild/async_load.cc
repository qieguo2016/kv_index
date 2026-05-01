#include "src/rebuild/async_load.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "src/artifact/artifact_format.h"
#include "src/ingest/kafka_update_consumer.h"
#include "src/artifact/mmap_snapshot_backing.h"
#include "src/store/realtime_delta.h"
#include "src/store/snapshot.h"
#include "src/ingest/update_applier.h"
#include "src/ingest/update_coordinator.h"

namespace kv_index::rebuild {
namespace {

std::mutex& CatchUpFactoryMutex() {
  static std::mutex* mu = new std::mutex();
  return *mu;
}

AsyncCatchUpRunnerFactory& CatchUpFactoryForTesting() {
  static AsyncCatchUpRunnerFactory* factory = new AsyncCatchUpRunnerFactory();
  return *factory;
}

bool SamePartition(const KafkaPartition& lhs, const KafkaPartition& rhs) {
  return lhs.topic == rhs.topic && lhs.partition == rhs.partition;
}

KafkaCheckpoint BuildCheckpoint(
    const std::vector<artifact::ArtifactSourceProgress>& progress) {
  KafkaCheckpoint checkpoint;
  checkpoint.next_offsets.reserve(progress.size());
  for (const artifact::ArtifactSourceProgress& entry : progress) {
    checkpoint.next_offsets.push_back(KafkaPosition{
        .partition =
            KafkaPartition{.topic = entry.topic, .partition = entry.partition},
        .offset = entry.checkpoint_next_offset,
    });
  }
  return checkpoint;
}

KafkaProgress BuildSafeProgress(
    const std::vector<artifact::ArtifactSourceProgress>& progress) {
  KafkaProgress safe_progress;
  safe_progress.partitions.reserve(progress.size());
  for (const artifact::ArtifactSourceProgress& entry : progress) {
    safe_progress.partitions.push_back(KafkaPartitionProgress{
        .partition =
            KafkaPartition{.topic = entry.topic, .partition = entry.partition},
        .committed_next_offset = entry.checkpoint_next_offset,
        .high_watermark = entry.high_watermark,
        .lag = std::max<std::int64_t>(
            0, entry.high_watermark - entry.checkpoint_next_offset),
    });
  }
  return safe_progress;
}

bool CatchUpRequired(const KafkaProgress& progress) {
  for (const KafkaPartitionProgress& entry : progress.partitions) {
    if (entry.committed_next_offset < entry.high_watermark) {
      return true;
    }
  }
  return false;
}

StatusOr<KafkaPartitionProgress> FindProgress(
    const KafkaProgress& progress, const KafkaPartition& partition) {
  const auto it = std::find_if(
      progress.partitions.begin(), progress.partitions.end(),
      [&](const KafkaPartitionProgress& entry) {
        return SamePartition(entry.partition, partition);
      });
  if (it == progress.partitions.end()) {
    return Status::FailedPrecondition(
        "catch-up progress is missing a source partition");
  }
  return *it;
}

StatusOr<KafkaProgress> UpdateObservedSafeProgress(
    const KafkaProgress& target, const KafkaProgress& observed) {
  if (target.partitions.empty()) {
    return Status::FailedPrecondition("artifact source progress is empty");
  }
  KafkaProgress updated;
  updated.partitions.reserve(target.partitions.size());
  for (const KafkaPartitionProgress& target_entry : target.partitions) {
    auto observed_entry = FindProgress(observed, target_entry.partition);
    if (!observed_entry.ok()) {
      return observed_entry.status();
    }
    const std::int64_t high_watermark =
        std::max(target_entry.high_watermark, observed_entry->high_watermark);
    updated.partitions.push_back(KafkaPartitionProgress{
        .partition = target_entry.partition,
        .committed_next_offset = observed_entry->committed_next_offset,
        .high_watermark = high_watermark,
        .lag = std::max<std::int64_t>(
            0, high_watermark - observed_entry->committed_next_offset),
    });
  }
  return updated;
}

void MarkFailed(LoadState* state, const Status& status) {
  state->code = LoadStateCode::kFailed;
  state->terminal = true;
  state->last_error = status.message();
  state->message = status.message().empty() ? "artifact load failed"
                                            : status.message();
}

void MarkCancelled(LoadState* state) {
  state->code = LoadStateCode::kCancelled;
  state->terminal = true;
  state->last_error = "load cancelled";
  state->message = "load cancelled";
}

void PopulateSourceProgress(
    LoadState* state, const std::vector<artifact::ArtifactSourceProgress>& progress) {
  state->source_progress.partitions.clear();
  state->source_progress.partitions.reserve(progress.size());
  for (const artifact::ArtifactSourceProgress& entry : progress) {
    state->source_progress.partitions.push_back(KafkaPartitionProgress{
        .partition =
            KafkaPartition{.topic = entry.topic, .partition = entry.partition},
        .committed_next_offset = entry.checkpoint_next_offset,
        .high_watermark = entry.high_watermark,
        .lag = std::max<std::int64_t>(
            0, entry.high_watermark - entry.checkpoint_next_offset),
    });
  }
}

void StoreState(const AsyncLoadCallbacks& callbacks, const LoadState& state) {
  if (callbacks.store_state) {
    callbacks.store_state(state);
  }
}

bool IsCancelled(const AsyncLoadCallbacks& callbacks) {
  return callbacks.is_cancelled && callbacks.is_cancelled();
}

Status CheckCancelled(const AsyncLoadCallbacks& callbacks, LoadState* state) {
  if (IsCancelled(callbacks)) {
    MarkCancelled(state);
    StoreState(callbacks, *state);
    return Status::Cancelled("load cancelled");
  }
  return Status::Ok();
}

std::vector<std::shared_ptr<store::RealtimeDeltaAtomicTable>> CreateRealtimeShards(
    const std::shared_ptr<const CompiledRowLayout>& layout,
    const ForwardIndexOptions& options) {
  std::vector<std::shared_ptr<store::RealtimeDeltaAtomicTable>> shards;
  shards.reserve(options.shard_count);
  for (std::uint32_t shard_id = 0; shard_id < options.shard_count; ++shard_id) {
    shards.push_back(std::make_shared<store::RealtimeDeltaAtomicTable>(
        store::RealtimeDeltaAtomicTable::Options{
            .layout = layout,
            .capacity = 1024,
            .hash_seed = options.hash_seed,
            .hash_version = options.hash_version,
        }));
  }
  return shards;
}

class ProductionCatchUpRunner final : public AsyncCatchUpRunner {
 public:
  explicit ProductionCatchUpRunner(ingest::KafkaUpdateConsumer consumer)
      : consumer_(std::move(consumer)) {}

  Status Start(const AsyncCatchUpRequest& request) override {
    request_ = request;
    if (request_.safe_progress.partitions.empty()) {
      return Status::FailedPrecondition("catch-up safe progress is empty");
    }
    const Status seek_status = consumer_.Seek(request_.checkpoint);
    if (!seek_status.ok()) {
      return seek_status;
    }
    const std::string& logical_topic =
        request_.safe_progress.partitions.front().partition.topic;
    applier_ = std::make_unique<ingest::UpdateApplier>(ingest::UpdateApplierOptions{
        .logical_topic = logical_topic,
        .targets =
            {ingest::UpdateTargetRoute{
                .role = ingest::UpdateGenerationRole::kRebuild,
                .generation_id = request_.load_id,
                .shard_count = request_.realtime_shards.size(),
                .hash_seed = request_.hash_seed,
                .hash_version = request_.hash_version,
                .layout = request_.layout,
                .realtime_shards = request_.realtime_shards,
            }},
    });
    coordinator_ = std::make_unique<ingest::UpdateCoordinator>(
        &consumer_, applier_.get(),
        ingest::UpdateCoordinatorOptions{
            .logical_topic = logical_topic,
            .poll_options = PollOptions{.timeout_ms = 100, .max_messages = 128},
        });
    return Status::Ok();
  }

  StatusOr<KafkaProgress> CaptureSafeProgress() override {
    if (coordinator_ == nullptr) {
      return Status::FailedPrecondition("catch-up runner has not started");
    }
    return consumer_.Progress();
  }

  Status PollApplyCommitOnce() override {
    if (coordinator_ == nullptr) {
      return Status::FailedPrecondition("catch-up runner has not started");
    }
    if (request_.cancellation_requested != nullptr &&
        request_.cancellation_requested->load()) {
      return Status::Cancelled("catch-up cancelled");
    }
    return coordinator_->PollApplyCommitOnce();
  }

 private:
  ingest::KafkaUpdateConsumer consumer_;
  AsyncCatchUpRequest request_;
  std::unique_ptr<ingest::UpdateApplier> applier_;
  std::unique_ptr<ingest::UpdateCoordinator> coordinator_;
};

StatusOr<std::unique_ptr<AsyncCatchUpRunner>> CreateCatchUpRunner(
    const ForwardIndexOptions& options) {
  {
    std::lock_guard<std::mutex> lock(CatchUpFactoryMutex());
    if (CatchUpFactoryForTesting()) {
      return CatchUpFactoryForTesting()(options);
    }
  }

  if (options.kafka_consumer.bootstrap_servers.empty() ||
      options.kafka_consumer.group_id.empty() ||
      options.kafka_consumer.topics.empty()) {
    return Status::FailedPrecondition(
        "catch-up required but no production Kafka consumer can be started");
  }

  auto consumer = ingest::KafkaUpdateConsumer::Create(options.kafka_consumer);
  if (!consumer.ok()) {
    return consumer.status();
  }
  return std::make_unique<ProductionCatchUpRunner>(std::move(consumer).value());
}

}  // namespace

AsyncCatchUpRunnerFactory SetAsyncCatchUpRunnerFactoryForTesting(
    AsyncCatchUpRunnerFactory factory) {
  std::lock_guard<std::mutex> lock(CatchUpFactoryMutex());
  AsyncCatchUpRunnerFactory previous = std::move(CatchUpFactoryForTesting());
  CatchUpFactoryForTesting() = std::move(factory);
  return previous;
}

Status RunExternalArtifactLoad(const LoadRequest& request, LoadId id,
                               const ForwardIndexOptions& options,
                               AsyncLoadCallbacks callbacks,
                               LoadState* state) {
  if (state == nullptr) {
    return Status::InvalidArgument("load state output is null");
  }
  *state = LoadState{
      .id = id,
      .code = LoadStateCode::kRunning,
      .terminal = false,
      .artifact_uri = request.artifact_uri,
      .artifact_id = request.artifact_id,
  };
  StoreState(callbacks, *state);
  if (request.artifact_uri.empty()) {
    const Status status = Status::InvalidArgument("artifact uri is empty");
    MarkFailed(state, status);
    StoreState(callbacks, *state);
    return status;
  }
  if (!callbacks.publish_shard) {
    const Status status =
        Status::FailedPrecondition("artifact load has no publish callback");
    MarkFailed(state, status);
    StoreState(callbacks, *state);
    return status;
  }
  if (const Status status = CheckCancelled(callbacks, state); !status.ok()) {
    return status;
  }

  const artifact::MmapSnapshotLoadOptions load_options{
      .expected_shard_count = options.shard_count,
      .expected_hash_seed = options.hash_seed,
      .expected_hash_version = options.hash_version,
  };
  std::vector<std::shared_ptr<artifact::MmapSnapshotBacking>> backings;
  backings.reserve(options.shard_count);
  state->total_shard_count = options.shard_count;
  state->shards.reserve(options.shard_count);
  for (std::uint32_t shard_id = 0; shard_id < options.shard_count; ++shard_id) {
    state->shards.push_back(LoadState::ShardProgress{.shard_id = shard_id});
  }

  for (std::uint32_t shard_id = 0; shard_id < options.shard_count; ++shard_id) {
    if (const Status status = CheckCancelled(callbacks, state); !status.ok()) {
      return status;
    }
    auto backing =
        artifact::MmapSnapshotBacking::LoadShard(request.artifact_uri, shard_id,
                                       load_options);
    if (!backing.ok()) {
      MarkFailed(state, backing.status());
      StoreState(callbacks, *state);
      return backing.status();
    }
    if (state->artifact_id.empty()) {
      state->artifact_id = (*backing)->artifact().artifact_id;
    }
    if (shard_id == 0) {
      PopulateSourceProgress(state, (*backing)->artifact().source_progress);
    }
    state->shards[shard_id].loaded = true;
    ++state->loaded_shard_count;
    backings.push_back(std::move(backing).value());
    StoreState(callbacks, *state);
  }

  for (std::uint32_t shard_id = 0; shard_id < options.shard_count; ++shard_id) {
    if (const Status status = CheckCancelled(callbacks, state); !status.ok()) {
      return status;
    }
    const Status status = backings[shard_id]->Prewarm();
    if (!status.ok()) {
      MarkFailed(state, status);
      StoreState(callbacks, *state);
      return status;
    }
    state->shards[shard_id].prewarmed = true;
    ++state->prewarmed_shard_count;
    StoreState(callbacks, *state);
  }

  const KafkaProgress artifact_progress =
      BuildSafeProgress(backings.front()->artifact().source_progress);
  std::vector<std::shared_ptr<store::RealtimeDeltaAtomicTable>> realtime_shards;
  if (artifact_progress.partitions.empty()) {
    const Status status =
        Status::FailedPrecondition("artifact source progress is empty");
    MarkFailed(state, status);
    StoreState(callbacks, *state);
    return status;
  }

  realtime_shards = CreateRealtimeShards(backings.front()->layout(), options);
  auto runner = CreateCatchUpRunner(options);
  if (!runner.ok()) {
    MarkFailed(state, runner.status());
    StoreState(callbacks, *state);
    return runner.status();
  }

  state->message = "artifact catch-up running";
  StoreState(callbacks, *state);

  AsyncCatchUpRequest catch_up_request{
      .load_id = id,
      .checkpoint =
          BuildCheckpoint(backings.front()->artifact().source_progress),
      .safe_progress = artifact_progress,
      .hash_seed = options.hash_seed,
      .hash_version = options.hash_version,
      .layout = backings.front()->layout(),
      .realtime_shards = realtime_shards,
      .cancellation_requested = callbacks.cancellation_requested,
  };
  const Status start_status = (*runner)->Start(catch_up_request);
  if (!start_status.ok()) {
    MarkFailed(state, start_status);
    StoreState(callbacks, *state);
    return start_status;
  }
  if (const Status status = CheckCancelled(callbacks, state); !status.ok()) {
    return status;
  }

  auto observed = (*runner)->CaptureSafeProgress();
  if (!observed.ok()) {
    MarkFailed(state, observed.status());
    StoreState(callbacks, *state);
    return observed.status();
  }
  auto safe_progress = UpdateObservedSafeProgress(artifact_progress, *observed);
  if (!safe_progress.ok()) {
    MarkFailed(state, safe_progress.status());
    StoreState(callbacks, *state);
    return safe_progress.status();
  }
  state->source_progress = safe_progress.value();
  StoreState(callbacks, *state);

  while (CatchUpRequired(state->source_progress)) {
    if (const Status status = CheckCancelled(callbacks, state); !status.ok()) {
      return status;
    }
    const Status poll_status = (*runner)->PollApplyCommitOnce();
    if (!poll_status.ok()) {
      if (poll_status.code() == StatusCode::kCancelled ||
          IsCancelled(callbacks)) {
        MarkCancelled(state);
      } else {
        MarkFailed(state, poll_status);
      }
      StoreState(callbacks, *state);
      return poll_status;
    }

    observed = (*runner)->CaptureSafeProgress();
    if (!observed.ok()) {
      MarkFailed(state, observed.status());
      StoreState(callbacks, *state);
      return observed.status();
    }
    safe_progress =
        UpdateObservedSafeProgress(state->source_progress, *observed);
    if (!safe_progress.ok()) {
      MarkFailed(state, safe_progress.status());
      StoreState(callbacks, *state);
      return safe_progress.status();
    }
    state->source_progress = safe_progress.value();
    StoreState(callbacks, *state);
  }

  if (const Status status = CheckCancelled(callbacks, state); !status.ok()) {
    return status;
  }
  for (std::uint32_t shard_id = 0; shard_id < options.shard_count; ++shard_id) {
    if (const Status status = CheckCancelled(callbacks, state); !status.ok()) {
      return status;
    }
    auto full_backing =
        std::static_pointer_cast<const store::SnapshotBacking>(backings[shard_id]);
    std::shared_ptr<const store::RealtimeDeltaAtomicTable> realtime_delta;
    if (!realtime_shards.empty()) {
      realtime_delta = realtime_shards[shard_id];
    }
    auto shard_state = std::make_shared<const runtime::ShardState>(
        shard_id, id,
        runtime::ShardState::Layers{
            .realtime_delta = std::move(realtime_delta),
            .full_snapshot = store::FullSnapshotView(std::move(full_backing)),
        });
    const Status status =
        callbacks.publish_shard(shard_id, std::move(shard_state));
    if (!status.ok()) {
      MarkFailed(state, status);
      StoreState(callbacks, *state);
      return status;
    }
    state->shards[shard_id].cutover = true;
    ++state->cutover_shard_count;
    StoreState(callbacks, *state);
  }

  state->code = LoadStateCode::kSucceeded;
  state->terminal = true;
  state->message = "artifact load succeeded";
  StoreState(callbacks, *state);
  return Status::Ok();
}

}  // namespace kv_index::rebuild
