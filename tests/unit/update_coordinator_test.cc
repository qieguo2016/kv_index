#include "src/ingest/update_coordinator.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kv_index/row.h"
#include "kv_index/schema.h"
#include "kv_index/status.h"
#include "kv_index/types.h"
#include "src/ingest/kafka_update_consumer.h"
#include "src/store/realtime_delta.h"
#include "src/ingest/update_applier.h"
#include "tests/test_support/test_macros.h"

namespace {

using kv_index::CompiledRowLayout;
using kv_index::FieldEncoding;
using kv_index::FieldSpec;
using kv_index::FieldType;
using kv_index::KafkaCheckpoint;
using kv_index::KafkaConsumerConfig;
using kv_index::KafkaPartition;
using kv_index::KafkaProgress;
using kv_index::KafkaUpsertMessage;
using kv_index::PollOptions;
using kv_index::Row;
using kv_index::RuntimeSchema;
using kv_index::Status;
using kv_index::StatusCode;
using kv_index::StatusOr;
using kv_index::internal::ingest::KafkaUpdateConsumer;
using kv_index::internal::ingest::KafkaUpdateConsumerClient;
using kv_index::internal::store::RealtimeDeltaAtomicTable;
using kv_index::internal::ingest::UpdateApplier;
using kv_index::internal::ingest::UpdateApplierOptions;
using kv_index::internal::ingest::UpdateCoordinator;
using kv_index::internal::ingest::UpdateCoordinatorOptions;
using kv_index::internal::ingest::UpdateGenerationRole;
using kv_index::internal::ingest::UpdateTargetRoute;

enum class WireKind : std::uint8_t {
  kInt32 = 2,
};

template <typename T>
void AppendLittleEndian(T value, std::vector<std::byte>* bytes) {
  for (std::size_t i = 0; i < sizeof(T); ++i) {
    bytes->push_back(static_cast<std::byte>(
        (static_cast<std::uint64_t>(value) >> (i * 8U)) & 0xffU));
  }
}

std::vector<std::byte> ScorePayload(std::int32_t score) {
  std::vector<std::byte> bytes;
  bytes.push_back(std::byte{'K'});
  bytes.push_back(std::byte{'V'});
  bytes.push_back(std::byte{'I'});
  bytes.push_back(std::byte{'U'});
  AppendLittleEndian<std::uint16_t>(1, &bytes);
  AppendLittleEndian<std::uint16_t>(0, &bytes);
  AppendLittleEndian<std::uint32_t>(1, &bytes);
  AppendLittleEndian<std::uint32_t>(1, &bytes);
  bytes.push_back(static_cast<std::byte>(WireKind::kInt32));
  AppendLittleEndian<std::uint32_t>(sizeof(score), &bytes);
  AppendLittleEndian<std::uint32_t>(static_cast<std::uint32_t>(score), &bytes);
  return bytes;
}

KafkaUpsertMessage Message(std::uint64_t primary_key, std::string topic,
                           std::int32_t partition, std::int64_t offset,
                           std::vector<std::byte> payload) {
  return KafkaUpsertMessage{
      .metadata =
          {
              .partition =
                  KafkaPartition{.topic = std::move(topic),
                                 .partition = partition},
              .offset = offset,
              .key = std::to_string(primary_key),
          },
      .primary_key = primary_key,
      .payload = std::move(payload),
  };
}

class FakeKafkaUpdateConsumerClient final : public KafkaUpdateConsumerClient {
 public:
  StatusOr<std::vector<KafkaUpsertMessage>> Poll(PollOptions options) override {
    ++poll_count;
    last_poll_options = options;
    if (!poll_status.ok()) {
      return poll_status;
    }
    return poll_batch;
  }

  Status Seek(const KafkaCheckpoint&) override { return Status::Ok(); }

  StatusOr<KafkaProgress> Progress() override { return KafkaProgress{}; }

  Status Commit(const KafkaCheckpoint& checkpoint) override {
    ++commit_count;
    committed_checkpoints.push_back(checkpoint);
    if (!commit_status.ok()) {
      return commit_status;
    }
    return Status::Ok();
  }

  std::vector<KafkaUpsertMessage> poll_batch;
  Status poll_status = Status::Ok();
  PollOptions last_poll_options;
  int poll_count = 0;
  int commit_count = 0;
  std::vector<KafkaCheckpoint> committed_checkpoints;
  Status commit_status = Status::Ok();
};

KafkaConsumerConfig ValidConfig() {
  return KafkaConsumerConfig{
      .bootstrap_servers = "127.0.0.1:9092",
      .group_id = "kv-index-test",
      .topics = {"updates"},
  };
}

KafkaUpdateConsumer ConsumerWith(FakeKafkaUpdateConsumerClient** fake_out) {
  auto fake = std::make_unique<FakeKafkaUpdateConsumerClient>();
  *fake_out = fake.get();
  auto consumer =
      KafkaUpdateConsumer::CreateForTesting(ValidConfig(), std::move(fake));
  KV_INDEX_CHECK(consumer.ok());
  return std::move(consumer).value();
}

FieldSpec Scalar(kv_index::FieldId field_id, std::string name, FieldType type,
                 bool nullable = true) {
  return FieldSpec{
      .field_id = field_id,
      .name = std::move(name),
      .type = type,
      .is_list = false,
      .nullable = nullable,
      .encoding = FieldEncoding::kFixed,
  };
}

std::shared_ptr<const CompiledRowLayout> Layout() {
  RuntimeSchema schema(500);
  KV_INDEX_CHECK(
      schema.AddField(Scalar(1, "score", FieldType::kInt32, false)).ok());
  auto layout = CompiledRowLayout::Compile(schema);
  KV_INDEX_CHECK(layout.ok());
  return std::make_shared<const CompiledRowLayout>(std::move(layout).value());
}

std::shared_ptr<RealtimeDeltaAtomicTable> MakeTable(
    std::shared_ptr<const CompiledRowLayout> layout) {
  return std::make_shared<RealtimeDeltaAtomicTable>(
      RealtimeDeltaAtomicTable::Options{.layout = std::move(layout),
                                        .capacity = 16});
}

UpdateApplier ApplierFor(std::shared_ptr<const CompiledRowLayout> layout,
                         std::shared_ptr<RealtimeDeltaAtomicTable> table) {
  return UpdateApplier(UpdateApplierOptions{
      .logical_topic = "updates",
      .targets =
          {
              UpdateTargetRoute{
                  .role = UpdateGenerationRole::kActive,
                  .generation_id = 1,
                  .shard_count = 1,
                  .hash_seed = 0,
                  .hash_version = 1,
                  .layout = std::move(layout),
                  .realtime_shards = {std::move(table)},
              },
          },
  });
}

UpdateCoordinator CoordinatorFor(KafkaUpdateConsumer* consumer,
                                 UpdateApplier* applier) {
  return UpdateCoordinator(
      consumer, applier,
      UpdateCoordinatorOptions{
          .logical_topic = "updates",
          .poll_options = PollOptions{.timeout_ms = 5, .max_messages = 16},
      });
}

void CheckNoRow(const std::shared_ptr<RealtimeDeltaAtomicTable>& table,
                std::uint64_t primary_key) {
  auto row = table->Get(primary_key);
  KV_INDEX_CHECK(row.ok());
  KV_INDEX_CHECK(!row->has_value());
}

std::int32_t VisibleScore(
    const std::shared_ptr<RealtimeDeltaAtomicTable>& table,
    std::uint64_t primary_key) {
  auto row = table->Get(primary_key);
  KV_INDEX_CHECK(row.ok());
  KV_INDEX_CHECK(row->has_value());
  return row->value().Get<std::int32_t>(1).value();
}

void EmptyPollDoesNotCommit() {
  FakeKafkaUpdateConsumerClient* fake = nullptr;
  KafkaUpdateConsumer consumer = ConsumerWith(&fake);
  auto layout = Layout();
  auto table = MakeTable(layout);
  UpdateApplier applier = ApplierFor(layout, table);
  UpdateCoordinator coordinator = CoordinatorFor(&consumer, &applier);

  Status status = coordinator.PollApplyCommitOnce();

  KV_INDEX_CHECK(status.ok());
  KV_INDEX_CHECK_EQ(fake->poll_count, 1);
  KV_INDEX_CHECK_EQ(fake->commit_count, 0);
  KV_INDEX_CHECK_EQ(fake->last_poll_options.max_messages, 16U);
}

void CommitsMaxOffsetPlusOnePerPartitionAfterSuccess() {
  FakeKafkaUpdateConsumerClient* fake = nullptr;
  KafkaUpdateConsumer consumer = ConsumerWith(&fake);
  fake->poll_batch = {
      Message(100, "updates", 0, 10, ScorePayload(10)),
      Message(101, "updates", 1, 7, ScorePayload(7)),
      Message(102, "updates", 0, 12, ScorePayload(12)),
  };
  auto layout = Layout();
  auto table = MakeTable(layout);
  UpdateApplier applier = ApplierFor(layout, table);
  UpdateCoordinator coordinator = CoordinatorFor(&consumer, &applier);

  Status status = coordinator.PollApplyCommitOnce();

  KV_INDEX_CHECK(status.ok());
  KV_INDEX_CHECK_EQ(fake->commit_count, 1);
  KV_INDEX_CHECK_EQ(fake->committed_checkpoints[0].next_offsets.size(), 2U);
  KV_INDEX_CHECK_EQ(
      fake->committed_checkpoints[0].next_offsets[0].partition.partition, 0);
  KV_INDEX_CHECK_EQ(fake->committed_checkpoints[0].next_offsets[0].offset, 13);
  KV_INDEX_CHECK_EQ(
      fake->committed_checkpoints[0].next_offsets[1].partition.partition, 1);
  KV_INDEX_CHECK_EQ(fake->committed_checkpoints[0].next_offsets[1].offset, 8);
  KV_INDEX_CHECK_EQ(VisibleScore(table, 100), 10);
  KV_INDEX_CHECK_EQ(VisibleScore(table, 101), 7);
  KV_INDEX_CHECK_EQ(VisibleScore(table, 102), 12);
}

void PollFailureDoesNotCommit() {
  FakeKafkaUpdateConsumerClient* fake = nullptr;
  KafkaUpdateConsumer consumer = ConsumerWith(&fake);
  fake->poll_status = Status::Unavailable("poll failed");
  auto layout = Layout();
  auto table = MakeTable(layout);
  UpdateApplier applier = ApplierFor(layout, table);
  UpdateCoordinator coordinator = CoordinatorFor(&consumer, &applier);

  Status status = coordinator.PollApplyCommitOnce();

  KV_INDEX_CHECK(!status.ok());
  KV_INDEX_CHECK_EQ(status.code(), StatusCode::kUnavailable);
  KV_INDEX_CHECK_EQ(fake->commit_count, 0);
}

void ApplyFailureDoesNotCommitOrPublish() {
  FakeKafkaUpdateConsumerClient* fake = nullptr;
  KafkaUpdateConsumer consumer = ConsumerWith(&fake);
  fake->poll_batch = {Message(200, "updates", 0, 1,
                              {std::byte{'b'}, std::byte{'a'},
                               std::byte{'d'}})};
  auto layout = Layout();
  auto table = MakeTable(layout);
  UpdateApplier applier = ApplierFor(layout, table);
  UpdateCoordinator coordinator = CoordinatorFor(&consumer, &applier);

  Status status = coordinator.PollApplyCommitOnce();

  KV_INDEX_CHECK(!status.ok());
  KV_INDEX_CHECK_EQ(status.code(), StatusCode::kInvalidArgument);
  KV_INDEX_CHECK_EQ(fake->commit_count, 0);
  CheckNoRow(table, 200);
}

void CommitFailureSurfacesAfterLocalPublish() {
  FakeKafkaUpdateConsumerClient* fake = nullptr;
  KafkaUpdateConsumer consumer = ConsumerWith(&fake);
  fake->poll_batch = {Message(300, "updates", 0, 2, ScorePayload(30))};
  fake->commit_status = Status::Unavailable("commit failed");
  auto layout = Layout();
  auto table = MakeTable(layout);
  UpdateApplier applier = ApplierFor(layout, table);
  UpdateCoordinator coordinator = CoordinatorFor(&consumer, &applier);

  Status status = coordinator.PollApplyCommitOnce();

  KV_INDEX_CHECK(!status.ok());
  KV_INDEX_CHECK_EQ(status.code(), StatusCode::kUnavailable);
  KV_INDEX_CHECK_EQ(fake->commit_count, 1);
  KV_INDEX_CHECK_EQ(VisibleScore(table, 300), 30);
}

void MultiTopicBatchFailsBeforeApplyOrCommit() {
  FakeKafkaUpdateConsumerClient* fake = nullptr;
  KafkaUpdateConsumer consumer = ConsumerWith(&fake);
  fake->poll_batch = {
      Message(400, "updates", 0, 3, ScorePayload(40)),
      Message(401, "other", 0, 4, ScorePayload(41)),
  };
  auto layout = Layout();
  auto table = MakeTable(layout);
  UpdateApplier applier = ApplierFor(layout, table);
  UpdateCoordinator coordinator = CoordinatorFor(&consumer, &applier);

  Status status = coordinator.PollApplyCommitOnce();

  KV_INDEX_CHECK(!status.ok());
  KV_INDEX_CHECK_EQ(status.code(), StatusCode::kFailedPrecondition);
  KV_INDEX_CHECK_EQ(fake->commit_count, 0);
  CheckNoRow(table, 400);
  CheckNoRow(table, 401);
}

}  // namespace

int main() {
  EmptyPollDoesNotCommit();
  CommitsMaxOffsetPlusOnePerPartitionAfterSuccess();
  PollFailureDoesNotCommit();
  ApplyFailureDoesNotCommitOrPublish();
  CommitFailureSurfacesAfterLocalPublish();
  MultiTopicBatchFailsBeforeApplyOrCommit();
  return 0;
}
