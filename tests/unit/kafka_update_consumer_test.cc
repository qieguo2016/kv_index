#include "src/ingest/kafka_update_consumer.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kv_index/status.h"
#include "kv_index/types.h"
#include "tests/test_support/test_macros.h"

namespace {

using kv_index::KafkaCheckpoint;
using kv_index::KafkaConsumerConfig;
using kv_index::KafkaPartition;
using kv_index::KafkaPartitionProgress;
using kv_index::KafkaPosition;
using kv_index::KafkaProgress;
using kv_index::KafkaUpsertMessage;
using kv_index::PollOptions;
using kv_index::Status;
using kv_index::StatusCode;
using kv_index::StatusOr;
using kv_index::internal::ingest::KafkaUpdateConsumer;
using kv_index::internal::ingest::KafkaUpdateConsumerClient;

KafkaConsumerConfig ValidConfig() {
  return KafkaConsumerConfig{
      .bootstrap_servers = "127.0.0.1:9092",
      .group_id = "kv-index-test",
      .topics = {"updates"},
  };
}

KafkaUpsertMessage Message(std::string topic, std::int32_t partition,
                           std::int64_t offset, std::uint64_t primary_key) {
  return KafkaUpsertMessage{
      .metadata =
          {
              .partition =
                  KafkaPartition{
                      .topic = std::move(topic),
                      .partition = partition,
                  },
              .offset = offset,
              .key = std::to_string(primary_key),
          },
      .primary_key = primary_key,
      .payload = {std::byte{0x01}, std::byte{0x02}},
  };
}

class FakeKafkaUpdateConsumerClient final : public KafkaUpdateConsumerClient {
 public:
  StatusOr<std::vector<KafkaUpsertMessage>> Poll(PollOptions options) override {
    last_poll_options = options;
    if (!poll_status.ok()) {
      return poll_status;
    }
    return poll_batch;
  }

  Status Seek(const KafkaCheckpoint& checkpoint) override {
    if (!seek_status.ok()) {
      return seek_status;
    }
    sought_checkpoint = checkpoint;
    return Status::Ok();
  }

  StatusOr<KafkaProgress> Progress() override {
    if (!progress_status.ok()) {
      return progress_status;
    }
    return progress;
  }

  Status Commit(const KafkaCheckpoint& checkpoint) override {
    if (!commit_status.ok()) {
      return commit_status;
    }
    committed_checkpoint = checkpoint;
    return Status::Ok();
  }

  std::vector<KafkaUpsertMessage> poll_batch;
  PollOptions last_poll_options;
  Status poll_status = Status::Ok();
  KafkaCheckpoint sought_checkpoint;
  Status seek_status = Status::Ok();
  KafkaProgress progress;
  Status progress_status = Status::Ok();
  KafkaCheckpoint committed_checkpoint;
  Status commit_status = Status::Ok();
};

KafkaUpdateConsumer ConsumerWith(FakeKafkaUpdateConsumerClient** fake_out) {
  auto fake = std::make_unique<FakeKafkaUpdateConsumerClient>();
  *fake_out = fake.get();
  auto consumer =
      KafkaUpdateConsumer::CreateForTesting(ValidConfig(), std::move(fake));
  KV_INDEX_CHECK(consumer.ok());
  return std::move(consumer).value();
}

void FakePollPreservesBatchOrderingAndOptions() {
  FakeKafkaUpdateConsumerClient* fake = nullptr;
  KafkaUpdateConsumer consumer = ConsumerWith(&fake);
  fake->poll_batch = {
      Message("updates", 0, 10, 100),
      Message("updates", 0, 11, 101),
      Message("updates", 1, 7, 200),
  };

  auto batch = consumer.Poll(PollOptions{.timeout_ms = 25, .max_messages = 3});

  KV_INDEX_CHECK(batch.ok());
  KV_INDEX_CHECK_EQ(batch->size(), 3U);
  KV_INDEX_CHECK_EQ((*batch)[0].primary_key, 100U);
  KV_INDEX_CHECK_EQ((*batch)[1].metadata.offset, 11);
  KV_INDEX_CHECK_EQ((*batch)[2].metadata.partition.partition, 1);
  KV_INDEX_CHECK_EQ(fake->last_poll_options.timeout_ms, 25);
  KV_INDEX_CHECK_EQ(fake->last_poll_options.max_messages, 3U);
}

void SeekAndCommitUsePerTopicPartitionCheckpoints() {
  FakeKafkaUpdateConsumerClient* fake = nullptr;
  KafkaUpdateConsumer consumer = ConsumerWith(&fake);
  KafkaCheckpoint checkpoint{
      .next_offsets =
          {
              KafkaPosition{.partition = KafkaPartition{.topic = "updates",
                                                        .partition = 0},
                            .offset = 13},
              KafkaPosition{.partition = KafkaPartition{.topic = "updates",
                                                        .partition = 1},
                            .offset = 8},
          },
  };

  KV_INDEX_CHECK(consumer.Seek(checkpoint).ok());
  KV_INDEX_CHECK_EQ(fake->sought_checkpoint.next_offsets.size(), 2U);
  KV_INDEX_CHECK_EQ(fake->sought_checkpoint.next_offsets[0].offset, 13);
  KV_INDEX_CHECK_EQ(fake->sought_checkpoint.next_offsets[1].partition.partition,
                    1);

  KV_INDEX_CHECK(consumer.Commit(checkpoint).ok());
  KV_INDEX_CHECK_EQ(fake->committed_checkpoint.next_offsets.size(), 2U);
  KV_INDEX_CHECK_EQ(fake->committed_checkpoint.next_offsets[0].partition.topic,
                    "updates");
  KV_INDEX_CHECK_EQ(fake->committed_checkpoint.next_offsets[1].offset, 8);
}

void ProgressReportsPerPartitionCommittedOffsetAndLag() {
  FakeKafkaUpdateConsumerClient* fake = nullptr;
  KafkaUpdateConsumer consumer = ConsumerWith(&fake);
  fake->progress = KafkaProgress{
      .partitions =
          {
              KafkaPartitionProgress{
                  .partition = KafkaPartition{.topic = "updates",
                                              .partition = 0},
                  .committed_next_offset = 13,
                  .high_watermark = 20,
                  .lag = 7,
              },
              KafkaPartitionProgress{
                  .partition = KafkaPartition{.topic = "updates",
                                              .partition = 1},
                  .committed_next_offset = 8,
                  .high_watermark = 10,
                  .lag = 2,
              },
          },
  };

  auto progress = consumer.Progress();

  KV_INDEX_CHECK(progress.ok());
  KV_INDEX_CHECK_EQ(progress->partitions.size(), 2U);
  KV_INDEX_CHECK_EQ(progress->partitions[0].committed_next_offset, 13);
  KV_INDEX_CHECK_EQ(progress->partitions[0].lag, 7);
  KV_INDEX_CHECK_EQ(progress->partitions[1].partition.partition, 1);
  KV_INDEX_CHECK_EQ(progress->partitions[1].high_watermark, 10);
}

void ConfigAndCheckpointValidationFailClosed() {
  auto valid_fake = std::make_unique<FakeKafkaUpdateConsumerClient>();
  auto missing_bootstrap =
      KafkaUpdateConsumer::CreateForTesting(
          KafkaConsumerConfig{.group_id = "group", .topics = {"updates"}},
          std::move(valid_fake));
  KV_INDEX_CHECK(!missing_bootstrap.ok());
  KV_INDEX_CHECK_EQ(missing_bootstrap.status().code(),
                    StatusCode::kInvalidArgument);

  auto empty_topic_fake = std::make_unique<FakeKafkaUpdateConsumerClient>();
  auto empty_topic =
      KafkaUpdateConsumer::CreateForTesting(
          KafkaConsumerConfig{.bootstrap_servers = "broker",
                              .group_id = "group",
                              .topics = {""}},
          std::move(empty_topic_fake));
  KV_INDEX_CHECK(!empty_topic.ok());
  KV_INDEX_CHECK_EQ(empty_topic.status().code(), StatusCode::kInvalidArgument);

  FakeKafkaUpdateConsumerClient* fake = nullptr;
  KafkaUpdateConsumer consumer = ConsumerWith(&fake);
  auto bad_checkpoint = consumer.Commit(KafkaCheckpoint{
      .next_offsets =
          {KafkaPosition{.partition = KafkaPartition{.topic = "updates",
                                                     .partition = 0},
                         .offset = 0},
           KafkaPosition{.partition = KafkaPartition{.topic = "updates",
                                                     .partition = 0},
                         .offset = 1}},
  });
  KV_INDEX_CHECK(!bad_checkpoint.ok());
  KV_INDEX_CHECK_EQ(bad_checkpoint.code(), StatusCode::kInvalidArgument);

  auto bad_poll = consumer.Poll(PollOptions{.timeout_ms = 1,
                                            .max_messages = 0});
  KV_INDEX_CHECK(!bad_poll.ok());
  KV_INDEX_CHECK_EQ(bad_poll.status().code(), StatusCode::kInvalidArgument);
}

void ClientErrorsPropagateWithoutRewritingStatus() {
  FakeKafkaUpdateConsumerClient* fake = nullptr;
  KafkaUpdateConsumer consumer = ConsumerWith(&fake);
  fake->poll_status = Status::Unavailable("poll failed");
  fake->seek_status = Status::FailedPrecondition("seek failed");
  fake->progress_status = Status::Internal("progress failed");
  fake->commit_status = Status::Unavailable("commit failed");

  auto poll = consumer.Poll(PollOptions{.timeout_ms = 1, .max_messages = 1});
  KV_INDEX_CHECK(!poll.ok());
  KV_INDEX_CHECK_EQ(poll.status().code(), StatusCode::kUnavailable);

  auto seek = consumer.Seek(KafkaCheckpoint{
      .next_offsets = {KafkaPosition{
          .partition = KafkaPartition{.topic = "updates", .partition = 0},
          .offset = 2}},
  });
  KV_INDEX_CHECK(!seek.ok());
  KV_INDEX_CHECK_EQ(seek.code(), StatusCode::kFailedPrecondition);

  auto progress = consumer.Progress();
  KV_INDEX_CHECK(!progress.ok());
  KV_INDEX_CHECK_EQ(progress.status().code(), StatusCode::kInternal);

  auto commit = consumer.Commit(KafkaCheckpoint{
      .next_offsets = {KafkaPosition{
          .partition = KafkaPartition{.topic = "updates", .partition = 0},
          .offset = 2}},
  });
  KV_INDEX_CHECK(!commit.ok());
  KV_INDEX_CHECK_EQ(commit.code(), StatusCode::kUnavailable);
}

}  // namespace

int main() {
  FakePollPreservesBatchOrderingAndOptions();
  SeekAndCommitUsePerTopicPartitionCheckpoints();
  ProgressReportsPerPartitionCommittedOffsetAndLag();
  ConfigAndCheckpointValidationFailClosed();
  ClientErrorsPropagateWithoutRewritingStatus();
  return 0;
}
