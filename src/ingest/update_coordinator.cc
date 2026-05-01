#include "src/ingest/update_coordinator.h"

#include <cstdint>
#include <limits>
#include <map>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace kv_index::ingest {
namespace {

using PartitionKey = std::pair<std::string, std::int32_t>;

Status ValidateOptions(const UpdateCoordinatorOptions& options) {
  if (options.logical_topic.empty()) {
    return Status::InvalidArgument("update coordinator logical topic is empty");
  }
  if (options.poll_options.timeout_ms < 0) {
    return Status::InvalidArgument("poll timeout must not be negative");
  }
  if (options.poll_options.max_messages == 0) {
    return Status::InvalidArgument("poll max_messages must be positive");
  }
  return Status::Ok();
}

Status ValidateBatchTopicAndPositions(
    const std::vector<KafkaUpsertMessage>& messages,
    const std::string& logical_topic) {
  for (const KafkaUpsertMessage& message : messages) {
    if (message.metadata.partition.topic.empty()) {
      return Status::InvalidArgument("kafka message topic is empty");
    }
    if (message.metadata.partition.topic != logical_topic) {
      return Status::FailedPrecondition(
          "coordinator stream contains multiple logical topics");
    }
    if (message.metadata.partition.partition < 0) {
      return Status::InvalidArgument("kafka message partition is invalid");
    }
    if (message.metadata.offset < 0) {
      return Status::InvalidArgument("kafka message offset is invalid");
    }
  }
  return Status::Ok();
}

StatusOr<KafkaCheckpoint> BuildCommitCheckpoint(
    const std::vector<KafkaUpsertMessage>& messages) {
  std::map<PartitionKey, std::int64_t> max_offsets;
  for (const KafkaUpsertMessage& message : messages) {
    if (message.metadata.offset == std::numeric_limits<std::int64_t>::max()) {
      return Status::InvalidArgument("kafka message offset cannot advance");
    }
    const PartitionKey key{message.metadata.partition.topic,
                           message.metadata.partition.partition};
    const auto it = max_offsets.find(key);
    if (it == max_offsets.end()) {
      max_offsets.emplace(key, message.metadata.offset);
      continue;
    }
    if (message.metadata.offset > it->second) {
      it->second = message.metadata.offset;
    }
  }

  KafkaCheckpoint checkpoint;
  checkpoint.next_offsets.reserve(max_offsets.size());
  for (const auto& [key, offset] : max_offsets) {
    checkpoint.next_offsets.push_back(KafkaPosition{
        .partition =
            KafkaPartition{
                .topic = key.first,
                .partition = key.second,
            },
        .offset = offset + 1,
    });
  }
  return checkpoint;
}

}  // namespace

UpdateCoordinator::UpdateCoordinator(KafkaUpdateConsumer* consumer,
                                     UpdateApplier* applier,
                                     UpdateCoordinatorOptions options)
    : consumer_(consumer), applier_(applier), options_(std::move(options)) {}

Status UpdateCoordinator::PollApplyCommitOnce() {
  if (consumer_ == nullptr) {
    return Status::FailedPrecondition("update coordinator has no consumer");
  }
  if (applier_ == nullptr) {
    return Status::FailedPrecondition("update coordinator has no applier");
  }
  if (const Status status = ValidateOptions(options_); !status.ok()) {
    return status;
  }

  auto batch = consumer_->Poll(options_.poll_options);
  if (!batch.ok()) {
    return batch.status();
  }
  if (batch->empty()) {
    return Status::Ok();
  }

  if (const Status status =
          ValidateBatchTopicAndPositions(batch.value(), options_.logical_topic);
      !status.ok()) {
    return status;
  }

  auto checkpoint = BuildCommitCheckpoint(batch.value());
  if (!checkpoint.ok()) {
    return checkpoint.status();
  }

  if (const Status status =
          applier_->ApplyBatch(std::span<const KafkaUpsertMessage>(
              batch->data(), batch->size()));
      !status.ok()) {
    return status;
  }

  return consumer_->Commit(checkpoint.value());
}

}  // namespace kv_index::ingest
