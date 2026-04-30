#include "kv_index/forward_index.h"

#include "src/core/hash.h"
#include "src/core/shard_directory.h"

#include <stdexcept>
#include <utility>
#include <vector>

namespace kv_index {
namespace {

[[noreturn]] void ThrowStatus(const Status& status) {
  std::string message = status.message().empty() ? "forward index read failed"
                                                 : status.message();
  throw std::runtime_error(message);
}

}  // namespace

bool IsPowerOfTwo(std::uint32_t value) noexcept {
  return value != 0 && (value & (value - 1)) == 0;
}

ForwardIndex::ForwardIndex(const ForwardIndexOptions& options)
    : options_(options) {
  if (!IsPowerOfTwo(options_.shard_count)) {
    throw std::invalid_argument(
        "ForwardIndexOptions::shard_count must be a non-zero power of two");
  }
  shard_directory_ =
      std::make_unique<core::ShardDirectory>(options_.shard_count);
}

ForwardIndex::~ForwardIndex() = default;

std::uint32_t ForwardIndex::ShardFor(std::uint64_t primary_key) const noexcept {
  return static_cast<std::uint32_t>(
      core::StableHash64(primary_key, options_.hash_seed,
                         options_.hash_version) &
      (static_cast<std::uint64_t>(options_.shard_count) - 1ULL));
}

std::optional<Row> ForwardIndex::Get(std::uint64_t primary_key) const {
  auto shard = shard_directory_->Load(ShardFor(primary_key));
  if (!shard.ok()) {
    ThrowStatus(shard.status());
  }
  if (*shard == nullptr) {
    return std::nullopt;
  }

  auto row = (*shard)->Get(primary_key);
  if (!row.ok()) {
    ThrowStatus(row.status());
  }
  return std::move(row).value();
}

std::vector<std::optional<Row>> ForwardIndex::MGet(
    const std::vector<std::uint64_t>& primary_keys) const {
  std::vector<std::optional<Row>> rows(primary_keys.size());
  std::vector<std::vector<std::size_t>> positions_by_shard(
      options_.shard_count);
  for (std::size_t i = 0; i < primary_keys.size(); ++i) {
    positions_by_shard[ShardFor(primary_keys[i])].push_back(i);
  }

  for (std::uint32_t shard_id = 0; shard_id < options_.shard_count;
       ++shard_id) {
    const auto& positions = positions_by_shard[shard_id];
    if (positions.empty()) {
      continue;
    }

    auto shard = shard_directory_->Load(shard_id);
    if (!shard.ok()) {
      ThrowStatus(shard.status());
    }
    if (*shard == nullptr) {
      continue;
    }

    std::vector<std::uint64_t> shard_keys;
    shard_keys.reserve(positions.size());
    for (const std::size_t position : positions) {
      shard_keys.push_back(primary_keys[position]);
    }

    auto shard_rows = (*shard)->MGet(shard_keys);
    if (!shard_rows.ok()) {
      ThrowStatus(shard_rows.status());
    }
    if (shard_rows->size() != positions.size()) {
      throw std::runtime_error(
          "shard returned a row count that does not match request count");
    }
    for (std::size_t i = 0; i < positions.size(); ++i) {
      rows[positions[i]] = std::move((*shard_rows)[i]);
    }
  }

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
