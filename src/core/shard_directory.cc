#include "src/core/shard_directory.h"

#include <atomic>
#include <memory>
#include <utility>

namespace kv_index::core {

ShardDirectory::ShardDirectory(std::uint32_t shard_count)
    : shard_count_(shard_count), shards_(shard_count) {}

Status ShardDirectory::Publish(std::uint32_t shard_id,
                               std::shared_ptr<const ShardState> state) {
  if (shard_id >= shard_count_) {
    return Status::InvalidArgument("shard id is out of range");
  }
  if (state == nullptr) {
    return Status::InvalidArgument("published shard state must not be null");
  }

  std::atomic_store_explicit(&shards_[shard_id], std::move(state),
                             std::memory_order_release);
  return Status::Ok();
}

StatusOr<std::shared_ptr<const ShardState>> ShardDirectory::Load(
    std::uint32_t shard_id) const {
  if (shard_id >= shard_count_) {
    return Status::InvalidArgument("shard id is out of range");
  }

  return std::atomic_load_explicit(&shards_[shard_id],
                                   std::memory_order_acquire);
}

}  // namespace kv_index::core
