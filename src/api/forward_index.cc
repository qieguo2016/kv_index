#include "kv_index/forward_index.h"

#include "src/rebuild/async_load.h"
#include "src/base/hash.h"
#include "src/runtime/shard_directory.h"

#include <sys/stat.h>

#include <atomic>
#include <memory>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace kv_index {
namespace {

[[noreturn]] void ThrowStatus(const Status& status) {
  std::string message = status.message().empty() ? "forward index read failed"
                                                 : status.message();
  throw std::runtime_error(message);
}

Status PreflightLocalArtifactUri(const LoadRequest& request) {
  if (request.artifact_uri.empty()) {
    return Status::InvalidArgument("artifact uri is empty");
  }

  std::string path = request.artifact_uri;
  const std::string file_scheme = "file://";
  const auto scheme_pos = path.find("://");
  if (scheme_pos != std::string::npos) {
    if (path.rfind(file_scheme, 0) != 0) {
      return Status::InvalidArgument("artifact uri scheme is unsupported");
    }
    path = path.substr(file_scheme.size());
  }
  if (path.empty()) {
    return Status::InvalidArgument("artifact path is empty");
  }
  struct stat file_stat {};
  if (stat(path.c_str(), &file_stat) != 0) {
    return Status::NotFound("artifact file does not exist");
  }
  if (!S_ISREG(file_stat.st_mode)) {
    return Status::InvalidArgument("artifact path is not a regular file");
  }
  return Status::Ok();
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
      std::make_unique<runtime::ShardDirectory>(
          options_.shard_count);
}

ForwardIndex::~ForwardIndex() {
  {
    std::lock_guard<std::mutex> lock(load_mu_);
    for (auto& [_, cancellation] : load_cancellations_) {
      if (cancellation != nullptr) {
        cancellation->store(true);
      }
    }
  }
  for (std::thread& worker : load_workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
}

std::uint32_t ForwardIndex::ShardFor(std::uint64_t primary_key) const noexcept {
  return static_cast<std::uint32_t>(
      base::StableHash64(primary_key, options_.hash_seed,
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
  const Status preflight = PreflightLocalArtifactUri(request);
  if (!preflight.ok()) {
    loads_.emplace(id, LoadState{
                           .id = id,
                           .code = LoadStateCode::kFailed,
                           .terminal = true,
                           .artifact_uri = request.artifact_uri,
                           .artifact_id = request.artifact_id,
                           .last_error = preflight.message(),
                           .message = preflight.message(),
                       });
    return id;
  }

  auto cancellation = std::make_shared<std::atomic_bool>(false);
  loads_.emplace(id, LoadState{
                         .id = id,
                         .code = LoadStateCode::kRunning,
                         .terminal = false,
                         .artifact_uri = request.artifact_uri,
                         .artifact_id = request.artifact_id,
                     });
  load_cancellations_.emplace(id, cancellation);
  load_workers_.emplace_back([this, request, id, cancellation] {
    LoadState state;
    (void)rebuild::RunExternalArtifactLoad(
        request, id, options_,
        rebuild::AsyncLoadCallbacks{
            .publish_shard =
                [this](std::uint32_t shard_id,
                       std::shared_ptr<const runtime::ShardState>
                           state) {
                  return shard_directory_->Publish(shard_id, std::move(state));
                },
            .store_state =
                [this, id](const LoadState& state) {
                  std::lock_guard<std::mutex> lock(load_mu_);
                  auto it = loads_.find(id);
                  if (it == loads_.end()) {
                    return;
                  }
                  if (it->second.code == LoadStateCode::kCancelled &&
                      it->second.terminal &&
                      state.code != LoadStateCode::kCancelled) {
                    LoadState cancelled = state;
                    cancelled.code = LoadStateCode::kCancelled;
                    cancelled.terminal = true;
                    cancelled.message = "load cancelled";
                    cancelled.last_error = "load cancelled";
                    it->second = std::move(cancelled);
                    return;
                  }
                  it->second = state;
                },
            .is_cancelled =
                [cancellation] { return cancellation->load(); },
            .cancellation_requested = cancellation,
        },
        &state);
    std::lock_guard<std::mutex> lock(load_mu_);
    load_cancellations_.erase(id);
  });
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
  it->second.last_error = "load cancelled";
  it->second.message = "load cancelled";
  const auto cancellation = load_cancellations_.find(id);
  if (cancellation != load_cancellations_.end() &&
      cancellation->second != nullptr) {
    cancellation->second->store(true);
  }
  return true;
}

RuntimeStatus ForwardIndex::GetRuntimeStatus() const {
  RuntimeStatus status{
      .shard_count = options_.shard_count,
  };
  status.shards.reserve(options_.shard_count);
  for (std::uint32_t shard_id = 0; shard_id < options_.shard_count;
       ++shard_id) {
    auto shard = shard_directory_->Load(shard_id);
    if (!shard.ok()) {
      status.shards.push_back(ShardRuntimeStatus{
          .shard_id = shard_id,
          .last_error = shard.status().message(),
      });
      if (status.last_error.empty()) {
        status.last_error = shard.status().message();
      }
      continue;
    }
    if (*shard == nullptr) {
      status.shards.push_back(ShardRuntimeStatus{.shard_id = shard_id});
      continue;
    }
    status.shards.push_back((*shard)->GetRuntimeStatus());
  }

  {
    std::lock_guard<std::mutex> lock(load_mu_);
    status.loads.reserve(loads_.size());
    for (const auto& [_, state] : loads_) {
      status.loads.push_back(state);
      if (!state.last_error.empty()) {
        status.last_error = state.last_error;
      }
    }
  }

  return status;
}

}  // namespace kv_index
