#ifndef KV_INDEX_FORWARD_INDEX_H_
#define KV_INDEX_FORWARD_INDEX_H_

#include <cstdint>
#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "kv_index/row.h"
#include "kv_index/types.h"

namespace kv_index {

namespace runtime {
class ShardDirectory;
}  // namespace runtime

namespace testing {
class ForwardIndexTestPeer;
}  // namespace testing

struct ForwardIndexOptions {
  std::uint32_t shard_count = 128;
  std::uint64_t hash_seed = 0;
  std::uint32_t hash_version = 1;
  ThresholdConfig thresholds;
  KafkaConsumerConfig kafka_consumer;
};

bool IsPowerOfTwo(std::uint32_t value) noexcept;
std::uint64_t StableHash64(std::uint64_t primary_key, std::uint64_t seed,
                           std::uint32_t version) noexcept;

class ForwardIndex {
 public:
  explicit ForwardIndex(const ForwardIndexOptions& options);
  ~ForwardIndex();

  const ForwardIndexOptions& options() const noexcept { return options_; }
  std::uint32_t ShardCount() const noexcept { return options_.shard_count; }
  std::uint32_t ShardFor(std::uint64_t primary_key) const noexcept;

  std::optional<Row> Get(std::uint64_t primary_key) const;
  std::vector<std::optional<Row>> MGet(
      const std::vector<std::uint64_t>& primary_keys) const;

  LoadId LoadAsync(const LoadRequest& request);
  LoadState GetLoadState(LoadId id) const;
  bool CancelLoad(LoadId id);
  RuntimeStatus GetRuntimeStatus() const;

 private:
  friend class testing::ForwardIndexTestPeer;

  ForwardIndexOptions options_;
  std::unique_ptr<runtime::ShardDirectory> shard_directory_;

  mutable std::mutex load_mu_;
  LoadId next_load_id_ = 1;
  std::unordered_map<LoadId, LoadState> loads_;
  std::unordered_map<LoadId, std::shared_ptr<std::atomic_bool>>
      load_cancellations_;
  std::vector<std::thread> load_workers_;
};

}  // namespace kv_index

#endif  // KV_INDEX_FORWARD_INDEX_H_
