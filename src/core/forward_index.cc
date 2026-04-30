#include "kv_index/forward_index.h"

#include "src/core/hash.h"

#include <stdexcept>
#include <utility>

namespace kv_index {

bool IsPowerOfTwo(std::uint32_t value) noexcept {
  return value != 0 && (value & (value - 1)) == 0;
}

ForwardIndex::ForwardIndex(const ForwardIndexOptions& options)
    : options_(options) {
  if (!IsPowerOfTwo(options_.shard_count)) {
    throw std::invalid_argument(
        "ForwardIndexOptions::shard_count must be a non-zero power of two");
  }
}

std::uint32_t ForwardIndex::ShardFor(std::uint64_t primary_key) const noexcept {
  return static_cast<std::uint32_t>(
      core::StableHash64(primary_key, options_.hash_seed,
                         options_.hash_version) &
      (static_cast<std::uint64_t>(options_.shard_count) - 1ULL));
}

std::optional<Row> ForwardIndex::Get(std::uint64_t primary_key) const {
  (void)primary_key;
  return std::nullopt;
}

std::vector<std::optional<Row>> ForwardIndex::MGet(
    const std::vector<std::uint64_t>& primary_keys) const {
  std::vector<std::optional<Row>> rows;
  rows.resize(primary_keys.size());
  return rows;
}

LoadId ForwardIndex::LoadAsync(const LoadRequest& request) {
  std::lock_guard<std::mutex> lock(load_mu_);
  const LoadId id = next_load_id_++;

  LoadState state;
  state.id = id;
  state.code = LoadStateCode::kFailed;
  state.terminal = true;
  state.message = "Async artifact loading is not implemented in the bootstrap";
  if (!request.artifact_id.empty()) {
    state.message += ": ";
    state.message += request.artifact_id;
  }

  loads_.emplace(id, std::move(state));
  return id;
}

LoadState ForwardIndex::GetLoadState(LoadId id) const {
  std::lock_guard<std::mutex> lock(load_mu_);
  const auto it = loads_.find(id);
  if (it == loads_.end()) {
    return LoadState{
        .id = id,
        .code = LoadStateCode::kUnknown,
        .terminal = true,
        .message = "unknown load id",
    };
  }
  return it->second;
}

bool ForwardIndex::CancelLoad(LoadId id) {
  std::lock_guard<std::mutex> lock(load_mu_);
  const auto it = loads_.find(id);
  if (it == loads_.end() || it->second.terminal) {
    return false;
  }

  it->second.code = LoadStateCode::kCancelled;
  it->second.terminal = true;
  it->second.message = "load cancelled";
  return true;
}

}  // namespace kv_index
