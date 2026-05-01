#ifndef KV_INDEX_SRC_INGEST_KAFKA_UPDATE_CONSUMER_H_
#define KV_INDEX_SRC_INGEST_KAFKA_UPDATE_CONSUMER_H_

#include <memory>
#include <vector>

#include "kv_index/status.h"
#include "kv_index/types.h"

namespace kv_index::internal::ingest {

class KafkaUpdateConsumerClient {
 public:
  virtual ~KafkaUpdateConsumerClient() = default;

  virtual StatusOr<std::vector<KafkaUpsertMessage>> Poll(
      PollOptions options) = 0;
  virtual Status Seek(const KafkaCheckpoint& checkpoint) = 0;
  virtual StatusOr<KafkaProgress> Progress() = 0;
  virtual Status Commit(const KafkaCheckpoint& checkpoint) = 0;
};

class KafkaUpdateConsumer {
 public:
  KafkaUpdateConsumer(KafkaUpdateConsumer&&) noexcept = default;
  KafkaUpdateConsumer& operator=(KafkaUpdateConsumer&&) noexcept = default;
  KafkaUpdateConsumer(const KafkaUpdateConsumer&) = delete;
  KafkaUpdateConsumer& operator=(const KafkaUpdateConsumer&) = delete;
  ~KafkaUpdateConsumer() = default;

  static StatusOr<KafkaUpdateConsumer> Create(KafkaConsumerConfig config);
  static StatusOr<KafkaUpdateConsumer> CreateForTesting(
      KafkaConsumerConfig config,
      std::unique_ptr<KafkaUpdateConsumerClient> client);

  StatusOr<std::vector<KafkaUpsertMessage>> Poll(PollOptions options);
  Status Seek(const KafkaCheckpoint& checkpoint);
  StatusOr<KafkaProgress> Progress();
  Status Commit(const KafkaCheckpoint& checkpoint);

  const KafkaConsumerConfig& config() const noexcept { return config_; }

 private:
  KafkaUpdateConsumer(KafkaConsumerConfig config,
                      std::unique_ptr<KafkaUpdateConsumerClient> client);

  KafkaConsumerConfig config_;
  std::unique_ptr<KafkaUpdateConsumerClient> client_;
};

}  // namespace kv_index::internal::ingest

#endif  // KV_INDEX_SRC_INGEST_KAFKA_UPDATE_CONSUMER_H_
