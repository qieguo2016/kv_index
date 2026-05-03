#include "tests/test_support/artifact_writer.h"

#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>

#include "src/artifact/artifact_writer.h"
#include "src/artifact/artifact_format.h"
#include "src/base/byte_io.h"

namespace kv_index::test_support {

using kv_index::artifact::kArtifactHeaderSize;
using kv_index::artifact::kArtifactSectionEntrySize;
using kv_index::base::ReadLittleEndian;
using kv_index::base::WriteLittleEndian;

constexpr std::size_t kEntryFileOffsetOffset = 8;
constexpr std::size_t kEntryLengthOffset = 16;

Status WriteTestArtifact(const std::string& path, const TestArtifactSpec& spec) {
  artifact::ArtifactBuildSpec build_spec;
  build_spec.artifact_id = spec.artifact_id;
  build_spec.shard_count = spec.shard_count;
  build_spec.hash_seed = spec.hash_seed;
  build_spec.hash_version = spec.hash_version;
  build_spec.layout = spec.layout;
  build_spec.include_source_progress_section =
      spec.include_source_progress_section;
  build_spec.source_progress.reserve(spec.source_progress.size());
  for (const TestSourceProgress& progress : spec.source_progress) {
    build_spec.source_progress.push_back(artifact::ArtifactSourceProgressBuildSpec{
        .topic = progress.topic,
        .partition = progress.partition,
        .checkpoint_next_offset = progress.checkpoint_next_offset,
        .high_watermark = progress.high_watermark,
    });
  }
  build_spec.shards.reserve(spec.shards.size());
  for (const ArtifactShardSpec& shard : spec.shards) {
    artifact::ArtifactShardBuildSpec build_shard;
    build_shard.shard_id = shard.shard_id;
    build_shard.rows.reserve(shard.rows.size());
    for (const TestArtifactRow& row : shard.rows) {
      build_shard.rows.push_back(artifact::ArtifactRow{
          .primary_key = row.primary_key,
          .encoded = row.encoded,
      });
    }
    build_spec.shards.push_back(std::move(build_shard));
  }
  return artifact::WriteArtifact(path, build_spec);
}

Status CorruptFirstSectionByte(std::vector<std::byte>* bytes) {
  auto offset = ReadLittleEndian<std::uint64_t>(*bytes,
                                                kArtifactHeaderSize +
                                                    kEntryFileOffsetOffset);
  if (!offset.ok()) {
    return offset.status();
  }
  if (*offset >= bytes->size()) {
    return Status::InvalidArgument("first section offset is out of bounds");
  }
  (*bytes)[*offset] ^= std::byte{0x01};
  return Status::Ok();
}

Status RewriteFirstSectionOffset(std::vector<std::byte>* bytes,
                                 std::uint64_t offset) {
  return WriteLittleEndian<std::uint64_t>(
      offset, std::span<std::byte>(*bytes),
      kArtifactHeaderSize + kEntryFileOffsetOffset);
}

Status RewriteFirstSectionLength(std::vector<std::byte>* bytes,
                                 std::uint64_t length) {
  return WriteLittleEndian<std::uint64_t>(
      length, std::span<std::byte>(*bytes),
      kArtifactHeaderSize + kEntryLengthOffset);
}

StatusOr<std::uint64_t> SecondSectionOffset(std::span<const std::byte> bytes) {
  return ReadLittleEndian<std::uint64_t>(
      bytes, kArtifactHeaderSize + kArtifactSectionEntrySize +
                 kEntryFileOffsetOffset);
}

}  // namespace kv_index::test_support
