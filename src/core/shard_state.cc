#include "src/core/shard_state.h"

#include <optional>
#include <utility>
#include <vector>

namespace kv_index::core {

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

}  // namespace kv_index::core
