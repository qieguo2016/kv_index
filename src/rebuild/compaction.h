#ifndef KV_INDEX_SRC_REBUILD_COMPACTION_H_
#define KV_INDEX_SRC_REBUILD_COMPACTION_H_

#include <cstdint>
#include <memory>

#include "kv_index/status.h"
#include "src/store/realtime_delta.h"
#include "src/runtime/shard_state.h"
#include "src/store/snapshot.h"
#include "src/store/snapshot_builder.h"

namespace kv_index::internal::rebuild {

struct CompactionBuildRequest {
  const runtime::ShardState& state;
  store::RealtimeDeltaBoundary boundary;
  store::SnapshotBuildOptions build_options = {};
};

struct FinishDeltaCompactionRequest {
  const runtime::ShardState& previous;
  std::uint64_t successor_generation = 0;
  std::shared_ptr<const store::RealtimeDeltaAtomicTable> successor_realtime;
  std::shared_ptr<const store::OwnedSnapshotBacking> compact_backing;
};

StatusOr<std::shared_ptr<const store::OwnedSnapshotBacking>>
BuildCompactedDeltaSnapshot(const CompactionBuildRequest& request);

StatusOr<std::shared_ptr<const runtime::ShardState>> FinishDeltaCompaction(
    FinishDeltaCompactionRequest request);

}  // namespace kv_index::internal::rebuild

#endif  // KV_INDEX_SRC_REBUILD_COMPACTION_H_
