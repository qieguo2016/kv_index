#ifndef KV_INDEX_SRC_TESTING_TEST_PEER_H_
#define KV_INDEX_SRC_TESTING_TEST_PEER_H_

#include <cstdint>
#include <memory>
#include <utility>

#include "kv_index/forward_index.h"
#include "kv_index/status.h"
#include "src/runtime/shard_directory.h"
#include "src/runtime/shard_state.h"

namespace kv_index::internal::testing {

class ForwardIndexTestPeer {
 public:
  static Status PublishShard(ForwardIndex& index,
                             std::shared_ptr<const runtime::ShardState> state) {
    if (state == nullptr) {
      return Status::InvalidArgument("published shard state must not be null");
    }
    return PublishShard(index, state->ShardId(), std::move(state));
  }

  static Status PublishShard(ForwardIndex& index, std::uint32_t shard_id,
                             std::shared_ptr<const runtime::ShardState> state) {
    if (index.shard_directory_ == nullptr) {
      return Status::FailedPrecondition(
          "forward index has no shard directory");
    }
    return index.shard_directory_->Publish(shard_id, std::move(state));
  }

  static StatusOr<std::shared_ptr<const runtime::ShardState>> LoadShard(
      const ForwardIndex& index, std::uint32_t shard_id) {
    if (index.shard_directory_ == nullptr) {
      return Status::FailedPrecondition(
          "forward index has no shard directory");
    }
    return index.shard_directory_->Load(shard_id);
  }
};

}  // namespace kv_index::internal::testing

#endif  // KV_INDEX_SRC_TESTING_TEST_PEER_H_
