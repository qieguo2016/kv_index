#ifndef KV_INDEX_SRC_INGEST_UPDATE_APPLIER_H_
#define KV_INDEX_SRC_INGEST_UPDATE_APPLIER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "kv_index/schema.h"
#include "kv_index/status.h"
#include "kv_index/types.h"
#include "src/store/realtime_delta.h"

namespace kv_index::internal::ingest {

enum class UpdateGenerationRole : std::uint8_t {
  kActive = 1,
  kRebuild = 2,
  kRebase = 3,
  kCompaction = 4,
};

struct UpdateTargetRoute {
  UpdateGenerationRole role = UpdateGenerationRole::kActive;
  std::uint64_t generation_id = 0;
  std::size_t shard_count = 0;
  std::uint64_t hash_seed = 0;
  std::uint32_t hash_version = 1;
  std::shared_ptr<const CompiledRowLayout> layout;
  std::vector<std::shared_ptr<store::RealtimeDeltaAtomicTable>> realtime_shards;
};

struct UpdateApplierOptions {
  std::string logical_topic;
  std::vector<UpdateTargetRoute> targets;
};

class UpdateApplier {
 public:
  explicit UpdateApplier(UpdateApplierOptions options);

  Status Apply(const KafkaUpsertMessage& message);
  Status ApplyBatch(std::span<const KafkaUpsertMessage> messages);

 private:
  UpdateApplierOptions options_;
};

}  // namespace kv_index::internal::ingest

#endif  // KV_INDEX_SRC_INGEST_UPDATE_APPLIER_H_
