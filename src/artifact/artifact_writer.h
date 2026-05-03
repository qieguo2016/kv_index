#ifndef KV_INDEX_SRC_ARTIFACT_ARTIFACT_WRITER_H_
#define KV_INDEX_SRC_ARTIFACT_ARTIFACT_WRITER_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "kv_index/schema.h"
#include "kv_index/status.h"
#include "src/model/row_storage.h"

namespace kv_index::artifact {

struct ArtifactRow {
  std::uint64_t primary_key = 0;
  model::EncodedRow encoded;
};

struct ArtifactShardBuildSpec {
  std::uint32_t shard_id = 0;
  std::vector<ArtifactRow> rows;
};

struct ArtifactSourceProgressBuildSpec {
  std::string topic;
  std::int32_t partition = -1;
  std::int64_t checkpoint_next_offset = -1;
  std::int64_t high_watermark = -1;
};

struct ArtifactBuildSpec {
  std::string artifact_id;
  std::uint32_t shard_count = 1;
  std::uint64_t hash_seed = 0;
  std::uint32_t hash_version = 1;
  std::shared_ptr<const CompiledRowLayout> layout;
  bool include_source_progress_section = true;
  std::vector<ArtifactSourceProgressBuildSpec> source_progress;
  std::vector<ArtifactShardBuildSpec> shards;
};

Status WriteArtifact(const std::string& path, const ArtifactBuildSpec& spec);
Status WriteArtifactShard(const std::string& path,
                          const ArtifactBuildSpec& spec,
                          std::uint32_t shard_id);

}  // namespace kv_index::artifact

#endif  // KV_INDEX_SRC_ARTIFACT_ARTIFACT_WRITER_H_
