#ifndef KV_INDEX_SRC_REBUILD_ASYNC_LOAD_H_
#define KV_INDEX_SRC_REBUILD_ASYNC_LOAD_H_

#include <atomic>
#include <functional>
#include <memory>

#include "kv_index/forward_index.h"
#include "kv_index/status.h"
#include "kv_index/types.h"
#include "src/store/realtime_delta.h"
#include "src/runtime/shard_state.h"

namespace kv_index::core {

using PublishShardFn = std::function<Status(
    std::uint32_t, std::shared_ptr<const ShardState>)>;
using StoreLoadStateFn = std::function<void(const LoadState&)>;
using IsLoadCancelledFn = std::function<bool()>;

struct AsyncCatchUpRequest {
  LoadId load_id = kInvalidLoadId;
  KafkaCheckpoint checkpoint;
  KafkaProgress safe_progress;
  std::uint64_t hash_seed = 0;
  std::uint32_t hash_version = 1;
  std::shared_ptr<const CompiledRowLayout> layout;
  std::vector<std::shared_ptr<RealtimeDeltaAtomicTable>> realtime_shards;
  std::shared_ptr<std::atomic_bool> cancellation_requested;
};

class AsyncCatchUpRunner {
 public:
  virtual ~AsyncCatchUpRunner() = default;

  virtual Status Start(const AsyncCatchUpRequest& request) = 0;
  virtual StatusOr<KafkaProgress> CaptureSafeProgress() = 0;
  virtual Status PollApplyCommitOnce() = 0;
};

using AsyncCatchUpRunnerFactory =
    std::function<StatusOr<std::unique_ptr<AsyncCatchUpRunner>>(
        const ForwardIndexOptions&)>;

AsyncCatchUpRunnerFactory SetAsyncCatchUpRunnerFactoryForTesting(
    AsyncCatchUpRunnerFactory factory);

struct AsyncLoadCallbacks {
  PublishShardFn publish_shard;
  StoreLoadStateFn store_state;
  IsLoadCancelledFn is_cancelled;
  std::shared_ptr<std::atomic_bool> cancellation_requested;
};

Status RunExternalArtifactLoad(const LoadRequest& request, LoadId id,
                               const ForwardIndexOptions& options,
                               AsyncLoadCallbacks callbacks,
                               LoadState* state);

}  // namespace kv_index::core

#endif  // KV_INDEX_SRC_REBUILD_ASYNC_LOAD_H_
