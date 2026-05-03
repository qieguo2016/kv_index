#ifndef KV_INDEX_TOOLS_OFFLINE_SRC_OFFLINE_ARTIFACT_BUILDER_H_
#define KV_INDEX_TOOLS_OFFLINE_SRC_OFFLINE_ARTIFACT_BUILDER_H_

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "kv_index/status.h"

namespace kv_index::offline {

struct BuildArtifactOptions {
  std::string schema_path;
  std::string input_path;
  std::string output_path;
  std::string artifact_id;
  std::optional<std::uint32_t> shard_count;
  std::optional<std::uint64_t> hash_seed;
  std::optional<std::uint32_t> hash_version;
  std::optional<std::uint64_t> schema_version;
  bool omit_source_progress = false;
};

struct BuildArtifactResult {
  std::uint64_t row_count = 0;
  std::uint32_t shard_count = 0;
  std::uint64_t hash_seed = 0;
  std::uint32_t hash_version = 1;
  std::uint64_t schema_version = 0;
  std::vector<std::uint64_t> rows_per_shard;
  std::vector<std::string> parquet_files;
};

StatusOr<BuildArtifactResult> BuildArtifactDirectory(
    const BuildArtifactOptions& options);

}  // namespace kv_index::offline

#endif  // KV_INDEX_TOOLS_OFFLINE_SRC_OFFLINE_ARTIFACT_BUILDER_H_
