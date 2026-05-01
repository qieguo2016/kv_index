#include "src/ingest/kafka_update_consumer.h"

#include <charconv>
#include <chrono>
#include <cstdint>
#include <memory>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <librdkafka/rdkafka.h>

namespace kv_index::ingest {
namespace {

using PartitionKey = std::pair<std::string, std::int32_t>;

Status ValidateConfig(const KafkaConsumerConfig& config) {
  if (config.bootstrap_servers.empty()) {
    return Status::InvalidArgument("kafka bootstrap servers must not be empty");
  }
  if (config.group_id.empty()) {
    return Status::InvalidArgument("kafka group id must not be empty");
  }
  if (config.topics.empty()) {
    return Status::InvalidArgument("kafka topics must not be empty");
  }
  std::set<std::string> topics;
  for (const std::string& topic : config.topics) {
    if (topic.empty()) {
      return Status::InvalidArgument("kafka topic must not be empty");
    }
    if (!topics.insert(topic).second) {
      return Status::InvalidArgument("kafka topics must not contain duplicates");
    }
  }
  return Status::Ok();
}

Status ValidatePollOptions(PollOptions options) {
  if (options.timeout_ms < 0) {
    return Status::InvalidArgument("poll timeout must not be negative");
  }
  if (options.max_messages == 0) {
    return Status::InvalidArgument("poll max_messages must be positive");
  }
  return Status::Ok();
}

Status ValidatePartition(const KafkaPartition& partition) {
  if (partition.topic.empty()) {
    return Status::InvalidArgument("kafka partition topic must not be empty");
  }
  if (partition.partition < 0) {
    return Status::InvalidArgument("kafka partition id must not be negative");
  }
  return Status::Ok();
}

Status ValidateMessage(const KafkaUpsertMessage& message) {
  if (const Status status = ValidatePartition(message.metadata.partition);
      !status.ok()) {
    return status;
  }
  if (message.metadata.offset < 0) {
    return Status::InvalidArgument("kafka message offset must not be negative");
  }
  return Status::Ok();
}

Status ValidateCheckpoint(const KafkaCheckpoint& checkpoint) {
  if (checkpoint.next_offsets.empty()) {
    return Status::InvalidArgument("kafka checkpoint must not be empty");
  }
  std::set<PartitionKey> seen;
  for (const KafkaPosition& position : checkpoint.next_offsets) {
    if (const Status status = ValidatePartition(position.partition);
        !status.ok()) {
      return status;
    }
    if (position.offset < 0) {
      return Status::InvalidArgument(
          "kafka checkpoint offset must not be negative");
    }
    PartitionKey key{position.partition.topic, position.partition.partition};
    if (!seen.insert(std::move(key)).second) {
      return Status::InvalidArgument(
          "kafka checkpoint contains duplicate topic partition");
    }
  }
  return Status::Ok();
}

Status ValidateProgress(const KafkaProgress& progress) {
  std::set<PartitionKey> seen;
  for (const KafkaPartitionProgress& partition_progress :
       progress.partitions) {
    if (const Status status = ValidatePartition(partition_progress.partition);
        !status.ok()) {
      return status;
    }
    if (partition_progress.committed_next_offset < 0 ||
        partition_progress.high_watermark < 0 || partition_progress.lag < 0) {
      return Status::InvalidArgument(
          "kafka progress offsets and lag must not be negative");
    }
    PartitionKey key{partition_progress.partition.topic,
                     partition_progress.partition.partition};
    if (!seen.insert(std::move(key)).second) {
      return Status::InvalidArgument(
          "kafka progress contains duplicate topic partition");
    }
  }
  return Status::Ok();
}

struct TopicPartitionListDeleter {
  void operator()(rd_kafka_topic_partition_list_t* list) const {
    if (list != nullptr) {
      rd_kafka_topic_partition_list_destroy(list);
    }
  }
};

using TopicPartitionListPtr =
    std::unique_ptr<rd_kafka_topic_partition_list_t,
                    TopicPartitionListDeleter>;

Status SetKafkaConfig(rd_kafka_conf_t* conf, const char* name,
                      const std::string& value) {
  char error[512];
  const rd_kafka_conf_res_t result =
      rd_kafka_conf_set(conf, name, value.c_str(), error, sizeof(error));
  if (result != RD_KAFKA_CONF_OK) {
    return Status::InvalidArgument(std::string("invalid kafka config ") + name +
                                   ": " + error);
  }
  return Status::Ok();
}

TopicPartitionListPtr MakeTopicPartitionList(std::size_t size) {
  return TopicPartitionListPtr(
      rd_kafka_topic_partition_list_new(static_cast<int>(size)));
}

StatusOr<TopicPartitionListPtr> BuildTopicPartitionList(
    const KafkaCheckpoint& checkpoint) {
  auto list = MakeTopicPartitionList(checkpoint.next_offsets.size());
  if (list == nullptr) {
    return Status::Internal("failed to allocate kafka topic partition list");
  }
  for (const KafkaPosition& position : checkpoint.next_offsets) {
    rd_kafka_topic_partition_t* partition =
        rd_kafka_topic_partition_list_add(
            list.get(), position.partition.topic.c_str(),
            position.partition.partition);
    if (partition == nullptr) {
      return Status::Internal("failed to add kafka topic partition");
    }
    partition->offset = position.offset;
  }
  return list;
}

StatusOr<std::uint64_t> ParsePrimaryKey(std::string_view key) {
  if (key.empty()) {
    return Status::InvalidArgument("kafka upsert key is empty");
  }
  std::uint64_t primary_key = 0;
  const auto [ptr, ec] =
      std::from_chars(key.data(), key.data() + key.size(), primary_key);
  if (ec != std::errc() || ptr != key.data() + key.size()) {
    return Status::InvalidArgument(
        "kafka upsert key must be an unsigned decimal primary key");
  }
  return primary_key;
}

StatusOr<KafkaUpsertMessage> ConvertMessage(
    const rd_kafka_message_t& message) {
  if (message.rkt == nullptr) {
    return Status::InvalidArgument("kafka message has no topic");
  }
  if (message.partition < 0 || message.offset < 0) {
    return Status::InvalidArgument("kafka message position is invalid");
  }

  const std::string topic = rd_kafka_topic_name(message.rkt);
  const std::string_view key(
      static_cast<const char*>(message.key), message.key_len);
  auto primary_key = ParsePrimaryKey(key);
  if (!primary_key.ok()) {
    return primary_key.status();
  }

  std::vector<std::byte> payload;
  if (message.len > 0) {
    const auto* begin = static_cast<const std::byte*>(message.payload);
    payload.assign(begin, begin + message.len);
  }

  return KafkaUpsertMessage{
      .metadata =
          {
              .partition =
                  KafkaPartition{
                      .topic = topic,
                      .partition = message.partition,
                  },
              .offset = message.offset,
              .key = std::string(key),
          },
      .primary_key = primary_key.value(),
      .payload = std::move(payload),
  };
}

class LibrdkafkaUpdateConsumerClient final : public KafkaUpdateConsumerClient {
 public:
  explicit LibrdkafkaUpdateConsumerClient(rd_kafka_t* consumer)
      : consumer_(consumer) {}

  ~LibrdkafkaUpdateConsumerClient() override {
    if (consumer_ != nullptr) {
      rd_kafka_consumer_close(consumer_);
      rd_kafka_destroy(consumer_);
    }
  }

  static StatusOr<std::unique_ptr<KafkaUpdateConsumerClient>> Create(
      const KafkaConsumerConfig& config) {
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    if (conf == nullptr) {
      return Status::Internal("failed to allocate kafka config");
    }

    auto destroy_conf = [&conf]() {
      if (conf != nullptr) {
        rd_kafka_conf_destroy(conf);
        conf = nullptr;
      }
    };

    for (const auto& [name, value] :
         std::vector<std::pair<const char*, std::string>>{
             {"bootstrap.servers", config.bootstrap_servers},
             {"group.id", config.group_id},
             {"enable.auto.commit", "false"},
             {"enable.partition.eof", "true"},
             {"auto.offset.reset", "earliest"},
         }) {
      if (const Status status = SetKafkaConfig(conf, name, value);
          !status.ok()) {
        destroy_conf();
        return status;
      }
    }

    char error[512];
    rd_kafka_t* consumer =
        rd_kafka_new(RD_KAFKA_CONSUMER, conf, error, sizeof(error));
    if (consumer == nullptr) {
      destroy_conf();
      return Status::Unavailable(std::string("failed to create kafka consumer: ") +
                                 error);
    }
    conf = nullptr;

    const rd_kafka_resp_err_t poll_set_error =
        rd_kafka_poll_set_consumer(consumer);
    if (poll_set_error != RD_KAFKA_RESP_ERR_NO_ERROR) {
      rd_kafka_destroy(consumer);
      return Status::Unavailable(std::string("failed to configure kafka poll: ") +
                                 rd_kafka_err2str(poll_set_error));
    }

    auto topics = MakeTopicPartitionList(config.topics.size());
    if (topics == nullptr) {
      rd_kafka_destroy(consumer);
      return Status::Internal("failed to allocate kafka subscription list");
    }
    for (const std::string& topic : config.topics) {
      rd_kafka_topic_partition_list_add(topics.get(), topic.c_str(),
                                        RD_KAFKA_PARTITION_UA);
    }
    const rd_kafka_resp_err_t subscribe_error =
        rd_kafka_subscribe(consumer, topics.get());
    if (subscribe_error != RD_KAFKA_RESP_ERR_NO_ERROR) {
      rd_kafka_destroy(consumer);
      return Status::Unavailable(std::string("failed to subscribe kafka topics: ") +
                                 rd_kafka_err2str(subscribe_error));
    }

    return std::unique_ptr<KafkaUpdateConsumerClient>(
        new LibrdkafkaUpdateConsumerClient(consumer));
  }

  StatusOr<std::vector<KafkaUpsertMessage>> Poll(
      PollOptions options) override {
    std::vector<KafkaUpsertMessage> messages;
    messages.reserve(options.max_messages);
    for (std::size_t i = 0; i < options.max_messages; ++i) {
      const int timeout_ms = messages.empty() ? options.timeout_ms : 0;
      rd_kafka_message_t* raw_message =
          rd_kafka_consumer_poll(consumer_, timeout_ms);
      if (raw_message == nullptr) {
        break;
      }
      std::unique_ptr<rd_kafka_message_t, decltype(&rd_kafka_message_destroy)>
          message(raw_message, rd_kafka_message_destroy);
      if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
          continue;
        }
        return Status::Unavailable(std::string("kafka poll failed: ") +
                                   rd_kafka_message_errstr(message.get()));
      }
      auto converted = ConvertMessage(*message);
      if (!converted.ok()) {
        return converted.status();
      }
      messages.push_back(std::move(converted).value());
    }
    return messages;
  }

  Status Seek(const KafkaCheckpoint& checkpoint) override {
    auto partitions = BuildTopicPartitionList(checkpoint);
    if (!partitions.ok()) {
      return partitions.status();
    }
    rd_kafka_error_t* error =
        rd_kafka_seek_partitions(consumer_, partitions->get(), 5000);
    if (error != nullptr) {
      std::string message = rd_kafka_error_string(error);
      rd_kafka_error_destroy(error);
      return Status::Unavailable("kafka seek failed: " + message);
    }
    return Status::Ok();
  }

  StatusOr<KafkaProgress> Progress() override {
    rd_kafka_topic_partition_list_t* raw_assignment = nullptr;
    const rd_kafka_resp_err_t assignment_error =
        rd_kafka_assignment(consumer_, &raw_assignment);
    if (assignment_error != RD_KAFKA_RESP_ERR_NO_ERROR) {
      return Status::Unavailable(std::string("kafka assignment failed: ") +
                                 rd_kafka_err2str(assignment_error));
    }
    TopicPartitionListPtr assignment(raw_assignment);
    if (assignment == nullptr || assignment->cnt == 0) {
      return KafkaProgress{};
    }

    const rd_kafka_resp_err_t committed_error =
        rd_kafka_committed(consumer_, assignment.get(), 5000);
    if (committed_error != RD_KAFKA_RESP_ERR_NO_ERROR) {
      return Status::Unavailable(std::string("kafka committed offsets failed: ") +
                                 rd_kafka_err2str(committed_error));
    }

    KafkaProgress progress;
    progress.partitions.reserve(static_cast<std::size_t>(assignment->cnt));
    for (int i = 0; i < assignment->cnt; ++i) {
      const rd_kafka_topic_partition_t& partition = assignment->elems[i];
      if (partition.err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return Status::Unavailable(std::string("kafka partition progress failed: ") +
                                   rd_kafka_err2str(partition.err));
      }

      std::int64_t low = 0;
      std::int64_t high = 0;
      const rd_kafka_resp_err_t watermark_error =
          rd_kafka_query_watermark_offsets(consumer_, partition.topic,
                                           partition.partition, &low, &high,
                                           5000);
      if (watermark_error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return Status::Unavailable(std::string("kafka watermark query failed: ") +
                                   rd_kafka_err2str(watermark_error));
      }

      const std::int64_t committed =
          partition.offset >= 0 ? partition.offset : 0;
      progress.partitions.push_back(KafkaPartitionProgress{
          .partition =
              KafkaPartition{
                  .topic = partition.topic,
                  .partition = partition.partition,
              },
          .committed_next_offset = committed,
          .high_watermark = high,
          .lag = high >= committed ? high - committed : 0,
      });
    }
    return progress;
  }

  Status Commit(const KafkaCheckpoint& checkpoint) override {
    auto partitions = BuildTopicPartitionList(checkpoint);
    if (!partitions.ok()) {
      return partitions.status();
    }
    const rd_kafka_resp_err_t error =
        rd_kafka_commit(consumer_, partitions->get(), 0);
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
      return Status::Unavailable(std::string("kafka commit failed: ") +
                                 rd_kafka_err2str(error));
    }
    return Status::Ok();
  }

 private:
  rd_kafka_t* consumer_ = nullptr;
};

}  // namespace

KafkaUpdateConsumer::KafkaUpdateConsumer(
    KafkaConsumerConfig config, std::unique_ptr<KafkaUpdateConsumerClient> client)
    : config_(std::move(config)), client_(std::move(client)) {}

StatusOr<KafkaUpdateConsumer> KafkaUpdateConsumer::Create(
    KafkaConsumerConfig config) {
  if (const Status status = ValidateConfig(config); !status.ok()) {
    return status;
  }
  auto client = LibrdkafkaUpdateConsumerClient::Create(config);
  if (!client.ok()) {
    return client.status();
  }
  return KafkaUpdateConsumer(std::move(config), std::move(client).value());
}

StatusOr<KafkaUpdateConsumer> KafkaUpdateConsumer::CreateForTesting(
    KafkaConsumerConfig config,
    std::unique_ptr<KafkaUpdateConsumerClient> client) {
  if (const Status status = ValidateConfig(config); !status.ok()) {
    return status;
  }
  if (client == nullptr) {
    return Status::InvalidArgument("kafka update consumer client is null");
  }
  return KafkaUpdateConsumer(std::move(config), std::move(client));
}

StatusOr<std::vector<KafkaUpsertMessage>> KafkaUpdateConsumer::Poll(
    PollOptions options) {
  if (const Status status = ValidatePollOptions(options); !status.ok()) {
    return status;
  }
  if (client_ == nullptr) {
    return Status::FailedPrecondition("kafka update consumer has no client");
  }
  auto batch = client_->Poll(options);
  if (!batch.ok()) {
    return batch.status();
  }
  for (const KafkaUpsertMessage& message : batch.value()) {
    if (const Status status = ValidateMessage(message); !status.ok()) {
      return status;
    }
  }
  return std::move(batch).value();
}

Status KafkaUpdateConsumer::Seek(const KafkaCheckpoint& checkpoint) {
  if (const Status status = ValidateCheckpoint(checkpoint); !status.ok()) {
    return status;
  }
  if (client_ == nullptr) {
    return Status::FailedPrecondition("kafka update consumer has no client");
  }
  return client_->Seek(checkpoint);
}

StatusOr<KafkaProgress> KafkaUpdateConsumer::Progress() {
  if (client_ == nullptr) {
    return Status::FailedPrecondition("kafka update consumer has no client");
  }
  auto progress = client_->Progress();
  if (!progress.ok()) {
    return progress.status();
  }
  if (const Status status = ValidateProgress(progress.value()); !status.ok()) {
    return status;
  }
  return std::move(progress).value();
}

Status KafkaUpdateConsumer::Commit(const KafkaCheckpoint& checkpoint) {
  if (const Status status = ValidateCheckpoint(checkpoint); !status.ok()) {
    return status;
  }
  if (client_ == nullptr) {
    return Status::FailedPrecondition("kafka update consumer has no client");
  }
  return client_->Commit(checkpoint);
}

}  // namespace kv_index::ingest
