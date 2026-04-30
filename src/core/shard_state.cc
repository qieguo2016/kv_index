#include "src/core/shard_state.h"

#include <optional>
#include <utility>
#include <vector>

#include "src/core/mmap_snapshot_backing.h"

namespace kv_index::core {
namespace {

void CopyLayoutMetadata(const std::shared_ptr<const CompiledRowLayout>& layout,
                        ShardRuntimeStatus* status) {
  if (layout == nullptr || status == nullptr || status->schema_version != 0) {
    return;
  }
  status->schema_version = layout->schema_version();
  status->layout_fingerprint = layout->layout_fingerprint();
}

}  // namespace

ShardState::ShardState(std::uint32_t shard_id, std::uint64_t generation,
                       Layers layers)
    : shard_id_(shard_id),
      generation_(generation),
      realtime_delta_(std::move(layers.realtime_delta)),
      compact_delta_(std::move(layers.compact_delta)),
      full_snapshot_(std::move(layers.full_snapshot)) {}

StatusOr<std::optional<Row>> ShardState::Get(
    std::uint64_t primary_key) const {
  if (realtime_delta_ != nullptr) {
    auto realtime_row = realtime_delta_->Get(primary_key);
    if (!realtime_row.ok()) {
      return realtime_row.status();
    }
    if (realtime_row->has_value()) {
      return std::move(realtime_row).value();
    }
  }

  if (compact_delta_.has_value()) {
    auto compact_row = compact_delta_->Get(primary_key);
    if (!compact_row.ok()) {
      return compact_row.status();
    }
    if (compact_row->has_value()) {
      return std::move(compact_row).value();
    }
  }

  if (full_snapshot_.has_value()) {
    auto full_row = full_snapshot_->Get(primary_key);
    if (!full_row.ok()) {
      return full_row.status();
    }
    if (full_row->has_value()) {
      return std::move(full_row).value();
    }
  }

  return std::optional<Row>();
}

StatusOr<std::vector<std::optional<Row>>> ShardState::MGet(
    const std::vector<std::uint64_t>& primary_keys) const {
  std::vector<std::optional<Row>> rows;
  rows.reserve(primary_keys.size());
  for (const std::uint64_t primary_key : primary_keys) {
    auto row = Get(primary_key);
    if (!row.ok()) {
      return row.status();
    }
    rows.push_back(std::move(row).value());
  }
  return rows;
}

ShardRuntimeStatus ShardState::GetRuntimeStatus() const {
  ShardRuntimeStatus status{
      .shard_id = shard_id_,
      .generation = generation_,
  };

  if (realtime_delta_ != nullptr) {
    status.has_realtime_delta = true;
    status.realtime_delta = realtime_delta_->stats();
    CopyLayoutMetadata(realtime_delta_->layout(), &status);
  }

  if (compact_delta_.has_value()) {
    status.has_compact_delta = true;
    if (compact_delta_->backing() != nullptr) {
      status.compact_row_count = compact_delta_->backing()->row_count();
      CopyLayoutMetadata(compact_delta_->backing()->layout(), &status);
    }
  }

  if (full_snapshot_.has_value()) {
    status.has_full_snapshot = true;
    const auto& backing = full_snapshot_->backing();
    if (backing != nullptr) {
      status.full_row_count = backing->row_count();
      CopyLayoutMetadata(backing->layout(), &status);
      if (const auto* mmap = dynamic_cast<const MmapSnapshotBacking*>(
              backing.get());
          mmap != nullptr) {
        status.artifact_id = mmap->artifact().artifact_id;
      }
    }
  }

  return status;
}

}  // namespace kv_index::core
