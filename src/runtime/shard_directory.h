#ifndef KV_INDEX_SRC_RUNTIME_SHARD_DIRECTORY_H_
#define KV_INDEX_SRC_RUNTIME_SHARD_DIRECTORY_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "kv_index/status.h"
#include "src/runtime/shard_state.h"

namespace kv_index::internal::runtime {

class ShardDirectory {
 public:
  explicit ShardDirectory(std::uint32_t shard_count);

  std::uint32_t ShardCount() const noexcept { return shard_count_; }

  Status Publish(std::uint32_t shard_id,
                 std::shared_ptr<const ShardState> state);
  StatusOr<std::shared_ptr<const ShardState>> Load(
      std::uint32_t shard_id) const;

 private:
  std::uint32_t shard_count_ = 0;
  std::vector<std::shared_ptr<const ShardState>> shards_;
};

}  // namespace kv_index::internal::runtime

#endif  // KV_INDEX_SRC_RUNTIME_SHARD_DIRECTORY_H_
