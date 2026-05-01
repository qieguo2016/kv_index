#ifndef KV_INDEX_SRC_REBUILD_FULL_REBASE_H_
#define KV_INDEX_SRC_REBUILD_FULL_REBASE_H_

#include <cstdint>
#include <memory>

#include "kv_index/status.h"
#include "src/store/realtime_delta.h"
#include "src/runtime/shard_state.h"
#include "src/store/snapshot.h"
#include "src/store/snapshot_builder.h"

namespace kv_index::internal::rebuild {

struct FullRebaseBuildRequest {
  const runtime::ShardState& state;
  bool external_async_load_active = false;
  store::SnapshotBuildOptions build_options = {};
};

struct FinishFullRebaseRequest {
  const runtime::ShardState& previous;
  std::uint64_t successor_generation = 0;
  std::shared_ptr<const store::RealtimeDeltaAtomicTable> rebase_realtime;
  std::shared_ptr<const store::OwnedSnapshotBacking> full_backing;
};

StatusOr<std::shared_ptr<const store::OwnedSnapshotBacking>> BuildRebasedFullSnapshot(
    const FullRebaseBuildRequest& request);

StatusOr<std::shared_ptr<const runtime::ShardState>> FinishFullRebase(
    FinishFullRebaseRequest request);

}  // namespace kv_index::internal::rebuild

#endif  // KV_INDEX_SRC_REBUILD_FULL_REBASE_H_
