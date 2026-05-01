#ifndef KV_INDEX_SRC_ARTIFACT_ARTIFACT_FORMAT_H_
#define KV_INDEX_SRC_ARTIFACT_ARTIFACT_FORMAT_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "kv_index/schema.h"
#include "kv_index/status.h"

namespace kv_index::core {

inline constexpr std::uint64_t kArtifactMagic = 0x3149564b54524146ULL;
inline constexpr std::uint32_t kArtifactFormatVersion = 1;
inline constexpr std::uint32_t kArtifactHeaderSize = 128;
inline constexpr std::uint32_t kArtifactSectionEntrySize = 64;
inline constexpr std::uint32_t kArtifactGlobalShardId = 0xffffffffU;

enum class ArtifactChecksumAlgorithm : std::uint32_t {
  kFnv1a64 = 1,
};

enum class ArtifactSectionType : std::uint32_t {
  kSchemaMetadata = 1,
  kSourceProgress = 2,
  kFrozenPrimaryKeyIndex = 100,
  kRowSlots = 101,
  kRowArena = 102,
  kStringPools = 103,
  kScalarListPools = 104,
  kStringListPools = 105,
  kStringElementPools = 106,
};

struct ArtifactValidationOptions {
  std::uint32_t expected_shard_count = 0;
  std::uint64_t expected_hash_seed = 0;
  std::uint32_t expected_hash_version = 0;
};

struct ArtifactSection {
  ArtifactSectionType type = ArtifactSectionType::kSchemaMetadata;
  std::uint32_t shard_id = kArtifactGlobalShardId;
  std::uint64_t offset = 0;
  std::uint64_t length = 0;
  std::uint64_t checksum = 0;
};

struct ArtifactSourceProgress {
  std::string topic;
  std::int32_t partition = -1;
  std::int64_t checkpoint_next_offset = -1;
  std::int64_t high_watermark = -1;
};

struct ParsedArtifact {
  std::string artifact_id;
  std::uint32_t shard_count = 0;
  std::uint64_t hash_seed = 0;
  std::uint32_t hash_version = 1;
  std::uint64_t schema_version = 0;
  std::uint64_t layout_fingerprint = 0;
  std::shared_ptr<const CompiledRowLayout> layout;
  std::vector<ArtifactSourceProgress> source_progress;
  std::vector<ArtifactSection> sections;
  std::span<const std::byte> bytes;

  std::optional<ArtifactSection> FindSection(ArtifactSectionType type,
                                             std::uint32_t shard_id) const;
};

std::uint64_t Fnv1a64(std::span<const std::byte> bytes) noexcept;

StatusOr<ParsedArtifact> ParseArtifact(
    std::span<const std::byte> bytes,
    ArtifactValidationOptions options = {});

}  // namespace kv_index::core

#endif  // KV_INDEX_SRC_ARTIFACT_ARTIFACT_FORMAT_H_
