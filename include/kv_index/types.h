#ifndef KV_INDEX_TYPES_H_
#define KV_INDEX_TYPES_H_

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace kv_index {

using FieldId = std::uint32_t;
using LoadId = std::uint64_t;

inline constexpr LoadId kInvalidLoadId = 0;

struct ThresholdConfig {
  std::uint64_t realtime_delta_row_arena_bytes = 64ULL * 1024ULL * 1024ULL;
  std::uint64_t realtime_delta_payload_pool_bytes = 64ULL * 1024ULL * 1024ULL;
  double realtime_delta_unique_key_ratio = 0.05;
  std::uint64_t compact_delta_bytes = 128ULL * 1024ULL * 1024ULL;
  double compact_delta_full_snapshot_ratio = 0.10;
  std::uint32_t cutover_batch_size = 1;
};

struct KafkaConsumerConfig {
  std::string bootstrap_servers;
  std::string group_id;
  std::vector<std::string> topics;
};

struct LoadRequest {
  std::string artifact_uri;
  std::string artifact_id;
};

enum class LoadStateCode {
  kUnknown,
  kPending,
  kRunning,
  kSucceeded,
  kFailed,
  kCancelled,
};

struct LoadState {
  LoadId id = kInvalidLoadId;
  LoadStateCode code = LoadStateCode::kUnknown;
  bool terminal = true;
  std::string message;
};

}  // namespace kv_index

#endif  // KV_INDEX_TYPES_H_
