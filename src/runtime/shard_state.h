#ifndef KV_INDEX_SRC_RUNTIME_SHARD_STATE_H_
#define KV_INDEX_SRC_RUNTIME_SHARD_STATE_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "kv_index/row.h"
#include "kv_index/status.h"
#include "src/store/realtime_delta.h"
#include "src/store/snapshot.h"

namespace kv_index::core {

class ShardState {
 public:
  struct Layers {
    std::shared_ptr<const RealtimeDeltaAtomicTable> realtime_delta;
    std::optional<CompactDeltaSnapshot> compact_delta;
    std::optional<FullSnapshotView> full_snapshot;
  };

  explicit ShardState(std::uint32_t shard_id, std::uint64_t generation,
                      Layers layers = {});

  std::uint32_t ShardId() const noexcept { return shard_id_; }
  std::uint64_t Generation() const noexcept { return generation_; }
  const std::shared_ptr<const RealtimeDeltaAtomicTable>& realtime_delta()
      const noexcept {
    return realtime_delta_;
  }
  const std::optional<CompactDeltaSnapshot>& compact_delta() const noexcept {
    return compact_delta_;
  }
  const std::optional<FullSnapshotView>& full_snapshot() const noexcept {
    return full_snapshot_;
  }

  StatusOr<std::optional<Row>> Get(std::uint64_t primary_key) const;
  StatusOr<std::vector<std::optional<Row>>> MGet(
      const std::vector<std::uint64_t>& primary_keys) const;
  ShardRuntimeStatus GetRuntimeStatus() const;

 private:
  std::uint32_t shard_id_ = 0;
  std::uint64_t generation_ = 0;
  std::shared_ptr<const RealtimeDeltaAtomicTable> realtime_delta_;
  std::optional<CompactDeltaSnapshot> compact_delta_;
  std::optional<FullSnapshotView> full_snapshot_;
};

}  // namespace kv_index::core

#endif  // KV_INDEX_SRC_RUNTIME_SHARD_STATE_H_
