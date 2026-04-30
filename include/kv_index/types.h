#ifndef KV_INDEX_TYPES_H_
#define KV_INDEX_TYPES_H_

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "kv_index/status.h"

namespace kv_index {

using FieldId = std::uint32_t;
using LoadId = std::uint64_t;

inline constexpr LoadId kInvalidLoadId = 0;

struct SourcePosition {
  std::int32_t partition = -1;
  std::int64_t offset = -1;
};

enum class SourcePositionUpdateDecision {
  kStale,
  kIdempotent,
  kNewer,
};

inline bool IsValidSourcePosition(SourcePosition position) noexcept {
  return position.partition >= 0 && position.offset >= 0;
}

// Same-key updates are deterministically comparable only within one
// partition. Higher offsets win, equal offsets are idempotent no-ops, lower
// offsets are stale, and cross-partition ordering fails closed.
inline StatusOr<SourcePositionUpdateDecision> ClassifySourcePositionUpdate(
    SourcePosition current, SourcePosition candidate) {
  if (!IsValidSourcePosition(current) ||
      !IsValidSourcePosition(candidate)) {
    return Status::InvalidArgument("source position is invalid");
  }
  if (current.partition != candidate.partition) {
    return Status::FailedPrecondition(
        "same-key source positions are from different partitions");
  }
  if (candidate.offset < current.offset) {
    return SourcePositionUpdateDecision::kStale;
  }
  if (candidate.offset == current.offset) {
    return SourcePositionUpdateDecision::kIdempotent;
  }
  return SourcePositionUpdateDecision::kNewer;
}

struct ThresholdConfig {
  std::uint64_t realtime_delta_row_arena_bytes = 64ULL * 1024ULL * 1024ULL;
  std::uint64_t realtime_delta_payload_pool_bytes = 64ULL * 1024ULL * 1024ULL;
  double realtime_delta_unique_key_ratio = 0.05;
  double realtime_delta_load_factor = 0.60;
  std::uint64_t compact_delta_bytes = 128ULL * 1024ULL * 1024ULL;
  double compact_delta_full_snapshot_ratio = 0.10;
  std::uint32_t cutover_batch_size = 1;
};

struct RealtimeDeltaStats {
  std::size_t hash_capacity = 0;
  std::size_t unique_visible_keys = 0;
  std::size_t published_row_count = 0;
  std::uint64_t row_slot_bytes = 0;
  std::uint64_t payload_pool_bytes = 0;
  double load_factor = 0.0;

  double UniqueKeyRatio(std::uint64_t full_snapshot_row_count) const noexcept {
    if (full_snapshot_row_count == 0) {
      return 0.0;
    }
    return static_cast<double>(unique_visible_keys) /
           static_cast<double>(full_snapshot_row_count);
  }
};

struct KafkaConsumerConfig {
  std::string bootstrap_servers;
  std::string group_id;
  std::vector<std::string> topics;
};

struct KafkaPartition {
  std::string topic;
  std::int32_t partition = -1;
};

struct KafkaPosition {
  KafkaPartition partition;
  std::int64_t offset = -1;
};

// Kafka message metadata records the consumed message offset. W04 source
// ordering uses that exact message offset.
struct KafkaMessageMetadata {
  KafkaPartition partition;
  std::int64_t offset = -1;
  std::string key;
  std::int64_t timestamp_millis = 0;
};

struct KafkaUpsertMessage {
  KafkaMessageMetadata metadata;
  std::uint64_t primary_key = 0;
  std::vector<std::byte> payload;
};

// Checkpoints store Kafka's commit offset: the next offset to consume for each
// topic+partition. After processing message offset N, commit N+1.
struct KafkaCheckpoint {
  std::vector<KafkaPosition> next_offsets;
};

struct KafkaPartitionProgress {
  KafkaPartition partition;
  std::int64_t committed_next_offset = -1;
  std::int64_t high_watermark = -1;
  std::int64_t lag = -1;
};

struct KafkaProgress {
  std::vector<KafkaPartitionProgress> partitions;
};

struct PollOptions {
  std::int32_t timeout_ms = 100;
  std::size_t max_messages = 1;
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
  std::string artifact_uri;
  std::string artifact_id;
  std::uint32_t total_shard_count = 0;
  std::uint32_t loaded_shard_count = 0;
  std::uint32_t prewarmed_shard_count = 0;
  std::uint32_t cutover_shard_count = 0;
  KafkaProgress source_progress;
  struct ShardProgress {
    std::uint32_t shard_id = 0;
    bool loaded = false;
    bool prewarmed = false;
    bool cutover = false;
    std::string message;
  };
  std::vector<ShardProgress> shards;
  std::string last_error;
  std::string message;
};

struct ShardRuntimeStatus {
  std::uint32_t shard_id = 0;
  std::uint64_t generation = 0;
  std::uint64_t schema_version = 0;
  std::uint64_t layout_fingerprint = 0;
  std::string artifact_id;
  bool has_realtime_delta = false;
  bool has_compact_delta = false;
  bool has_full_snapshot = false;
  RealtimeDeltaStats realtime_delta;
  std::uint64_t compact_row_count = 0;
  std::uint64_t full_row_count = 0;
  std::uint64_t accessor_mismatch_count = 0;
  std::string last_error;
};

struct RuntimeStatus {
  std::uint32_t shard_count = 0;
  std::vector<ShardRuntimeStatus> shards;
  std::vector<LoadState> loads;
  std::string last_error;
};

}  // namespace kv_index

#endif  // KV_INDEX_TYPES_H_
