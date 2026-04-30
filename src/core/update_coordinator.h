#ifndef KV_INDEX_SRC_CORE_UPDATE_COORDINATOR_H_
#define KV_INDEX_SRC_CORE_UPDATE_COORDINATOR_H_

#include <string>

#include "kv_index/status.h"
#include "kv_index/types.h"
#include "src/core/kafka_update_consumer.h"
#include "src/core/update_applier.h"

namespace kv_index::core {

struct UpdateCoordinatorOptions {
  std::string logical_topic;
  PollOptions poll_options;
};

class UpdateCoordinator {
 public:
  UpdateCoordinator(KafkaUpdateConsumer* consumer, UpdateApplier* applier,
                    UpdateCoordinatorOptions options);

  Status PollApplyCommitOnce();

 private:
  KafkaUpdateConsumer* consumer_ = nullptr;
  UpdateApplier* applier_ = nullptr;
  UpdateCoordinatorOptions options_;
};

}  // namespace kv_index::core

#endif  // KV_INDEX_SRC_CORE_UPDATE_COORDINATOR_H_
