#ifndef KV_INDEX_SRC_REBUILD_COMPACTION_H_
#define KV_INDEX_SRC_REBUILD_COMPACTION_H_

#include <cstdint>
#include <memory>

#include "kv_index/status.h"
#include "src/store/realtime_delta.h"
#include "src/runtime/shard_state.h"
#include "src/store/snapshot.h"
#include "src/store/snapshot_builder.h"

namespace kv_index::core {

struct CompactionBuildRequest {
  const ShardState& state;
  RealtimeDeltaBoundary boundary;
  SnapshotBuildOptions build_options = {};
};

struct FinishDeltaCompactionRequest {
  const ShardState& previous;
  std::uint64_t successor_generation = 0;
  std::shared_ptr<const RealtimeDeltaAtomicTable> successor_realtime;
  std::shared_ptr<const OwnedSnapshotBacking> compact_backing;
};

StatusOr<std::shared_ptr<const OwnedSnapshotBacking>>
BuildCompactedDeltaSnapshot(const CompactionBuildRequest& request);

StatusOr<std::shared_ptr<const ShardState>> FinishDeltaCompaction(
    FinishDeltaCompactionRequest request);

}  // namespace kv_index::core

#endif  // KV_INDEX_SRC_REBUILD_COMPACTION_H_
