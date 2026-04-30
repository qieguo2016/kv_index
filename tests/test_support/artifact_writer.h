#ifndef KV_INDEX_TESTS_TEST_SUPPORT_ARTIFACT_WRITER_H_
#define KV_INDEX_TESTS_TEST_SUPPORT_ARTIFACT_WRITER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "kv_index/schema.h"
#include "kv_index/status.h"
#include "src/core/row_storage.h"

namespace kv_index::test_support {

struct TestArtifactRow {
  std::uint64_t primary_key = 0;
  internal::EncodedRow encoded;
};

struct ArtifactShardSpec {
  std::uint32_t shard_id = 0;
  std::vector<TestArtifactRow> rows;
};

struct TestSourceProgress {
  std::string topic;
  std::int32_t partition = -1;
  std::int64_t checkpoint_next_offset = -1;
  std::int64_t high_watermark = -1;
};

struct TestArtifactSpec {
  std::string artifact_id;
  std::uint32_t shard_count = 1;
  std::uint64_t hash_seed = 0;
  std::uint32_t hash_version = 1;
  std::shared_ptr<const CompiledRowLayout> layout;
  std::vector<TestSourceProgress> source_progress;
  std::vector<ArtifactShardSpec> shards;
};

Status WriteTestArtifact(const std::string& path, const TestArtifactSpec& spec);

Status CorruptFirstSectionByte(std::vector<std::byte>* bytes);
Status RewriteFirstSectionOffset(std::vector<std::byte>* bytes,
                                 std::uint64_t offset);
Status RewriteFirstSectionLength(std::vector<std::byte>* bytes,
                                 std::uint64_t length);
StatusOr<std::uint64_t> SecondSectionOffset(std::span<const std::byte> bytes);

}  // namespace kv_index::test_support

#endif  // KV_INDEX_TESTS_TEST_SUPPORT_ARTIFACT_WRITER_H_
