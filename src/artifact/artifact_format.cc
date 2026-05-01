#include "src/artifact/artifact_format.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <set>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "src/base/byte_io.h"

namespace kv_index::internal::artifact {
namespace {

using kv_index::internal::base::ReadLittleEndian;

constexpr std::size_t kMagicOffset = 0;
constexpr std::size_t kVersionOffset = 8;
constexpr std::size_t kHeaderSizeOffset = 12;
constexpr std::size_t kEntrySizeOffset = 16;
constexpr std::size_t kSectionCountOffset = 20;
constexpr std::size_t kChecksumAlgorithmOffset = 24;
constexpr std::size_t kShardCountOffset = 28;
constexpr std::size_t kHashSeedOffset = 32;
constexpr std::size_t kHashVersionOffset = 40;
constexpr std::size_t kSchemaVersionOffset = 48;
constexpr std::size_t kLayoutFingerprintOffset = 56;
constexpr std::size_t kArtifactIdLengthOffset = 64;
constexpr std::size_t kArtifactIdOffset = 68;
constexpr std::size_t kArtifactIdCapacity = 60;

constexpr std::size_t kEntryTypeOffset = 0;
constexpr std::size_t kEntryShardIdOffset = 4;
constexpr std::size_t kEntryFileOffsetOffset = 8;
constexpr std::size_t kEntryLengthOffset = 16;
constexpr std::size_t kEntryChecksumOffset = 24;

Status CheckBounds(std::span<const std::byte> bytes, std::uint64_t offset,
                   std::uint64_t length) {
  if (offset > bytes.size() || bytes.size() - offset < length) {
    return Status::InvalidArgument("artifact section is out of bounds");
  }
  return Status::Ok();
}

StatusOr<std::string> ReadString(std::span<const std::byte> bytes,
                                 std::size_t* offset) {
  auto size = ReadLittleEndian<std::uint32_t>(bytes, *offset);
  if (!size.ok()) {
    return size.status();
  }
  *offset += sizeof(std::uint32_t);
  if (*offset > bytes.size() || bytes.size() - *offset < *size) {
    return Status::InvalidArgument("artifact string is truncated");
  }
  std::string value(reinterpret_cast<const char*>(bytes.data() + *offset),
                    *size);
  *offset += *size;
  return value;
}

StatusOr<std::shared_ptr<const CompiledRowLayout>> ParseSchemaSection(
    std::span<const std::byte> bytes, std::uint64_t expected_schema_version,
    std::uint64_t expected_layout_fingerprint) {
  std::size_t offset = 0;
  auto schema_version = ReadLittleEndian<std::uint64_t>(bytes, offset);
  if (!schema_version.ok()) {
    return schema_version.status();
  }
  offset += sizeof(std::uint64_t);
  if (*schema_version != expected_schema_version) {
    return Status::FailedPrecondition("schema section version mismatch");
  }

  auto field_count = ReadLittleEndian<std::uint32_t>(bytes, offset);
  if (!field_count.ok()) {
    return field_count.status();
  }
  offset += sizeof(std::uint32_t);

  RuntimeSchema schema(*schema_version);
  for (std::uint32_t i = 0; i < *field_count; ++i) {
    auto field_id = ReadLittleEndian<std::uint32_t>(bytes, offset);
    if (!field_id.ok()) {
      return field_id.status();
    }
    offset += sizeof(std::uint32_t);
    if (offset + 4 > bytes.size()) {
      return Status::InvalidArgument("schema field entry is truncated");
    }
    const auto type = static_cast<FieldType>(
        std::to_integer<std::uint8_t>(bytes[offset++]));
    const bool is_list = std::to_integer<std::uint8_t>(bytes[offset++]) != 0;
    const bool nullable = std::to_integer<std::uint8_t>(bytes[offset++]) != 0;
    const auto encoding = static_cast<FieldEncoding>(
        std::to_integer<std::uint8_t>(bytes[offset++]));
    auto name = ReadString(bytes, &offset);
    if (!name.ok()) {
      return name.status();
    }
    Status status = schema.AddField(FieldSpec{
        .field_id = *field_id,
        .name = std::move(name).value(),
        .type = type,
        .is_list = is_list,
        .nullable = nullable,
        .encoding = encoding,
    });
    if (!status.ok()) {
      return status;
    }
  }
  if (offset != bytes.size()) {
    return Status::InvalidArgument("schema section has trailing bytes");
  }
  auto layout = CompiledRowLayout::Compile(schema);
  if (!layout.ok()) {
    return layout.status();
  }
  if (layout->layout_fingerprint() != expected_layout_fingerprint) {
    return Status::FailedPrecondition("schema layout fingerprint mismatch");
  }
  return std::make_shared<const CompiledRowLayout>(std::move(layout).value());
}

StatusOr<std::vector<ArtifactSourceProgress>> ParseSourceProgressSection(
    std::span<const std::byte> bytes) {
  std::size_t offset = 0;
  auto count = ReadLittleEndian<std::uint32_t>(bytes, offset);
  if (!count.ok()) {
    return count.status();
  }
  offset += sizeof(std::uint32_t);

  std::vector<ArtifactSourceProgress> progress;
  progress.reserve(*count);
  std::set<std::pair<std::string, std::int32_t>> seen;
  for (std::uint32_t i = 0; i < *count; ++i) {
    auto topic = ReadString(bytes, &offset);
    if (!topic.ok()) {
      return topic.status();
    }
    auto partition = ReadLittleEndian<std::int32_t>(bytes, offset);
    if (!partition.ok()) {
      return partition.status();
    }
    offset += sizeof(std::int32_t);
    auto checkpoint = ReadLittleEndian<std::int64_t>(bytes, offset);
    if (!checkpoint.ok()) {
      return checkpoint.status();
    }
    offset += sizeof(std::int64_t);
    auto high_watermark = ReadLittleEndian<std::int64_t>(bytes, offset);
    if (!high_watermark.ok()) {
      return high_watermark.status();
    }
    offset += sizeof(std::int64_t);
    if (topic->empty() || *partition < 0 || *checkpoint < 0 ||
        *high_watermark < *checkpoint) {
      return Status::InvalidArgument("invalid source progress entry");
    }
    if (!seen.insert({*topic, *partition}).second) {
      return Status::InvalidArgument("duplicate source progress entry");
    }
    progress.push_back(ArtifactSourceProgress{
        .topic = std::move(topic).value(),
        .partition = *partition,
        .checkpoint_next_offset = *checkpoint,
        .high_watermark = *high_watermark,
    });
  }
  if (offset != bytes.size()) {
    return Status::InvalidArgument("source progress section has trailing bytes");
  }
  return progress;
}

StatusOr<ArtifactSectionType> DecodeSectionType(std::uint32_t value) {
  switch (static_cast<ArtifactSectionType>(value)) {
    case ArtifactSectionType::kSchemaMetadata:
    case ArtifactSectionType::kSourceProgress:
    case ArtifactSectionType::kFrozenPrimaryKeyIndex:
    case ArtifactSectionType::kRowSlots:
    case ArtifactSectionType::kRowArena:
    case ArtifactSectionType::kStringPools:
    case ArtifactSectionType::kScalarListPools:
    case ArtifactSectionType::kStringListPools:
    case ArtifactSectionType::kStringElementPools:
      return static_cast<ArtifactSectionType>(value);
  }
  return Status::InvalidArgument("unsupported artifact section type");
}

}  // namespace

std::optional<ArtifactSection> ParsedArtifact::FindSection(
    ArtifactSectionType type, std::uint32_t shard_id) const {
  for (const ArtifactSection& section : sections) {
    if (section.type == type && section.shard_id == shard_id) {
      return section;
    }
  }
  return std::nullopt;
}

std::uint64_t Fnv1a64(std::span<const std::byte> bytes) noexcept {
  std::uint64_t hash = 14695981039346656037ULL;
  for (const std::byte byte : bytes) {
    hash ^= std::to_integer<std::uint8_t>(byte);
    hash *= 1099511628211ULL;
  }
  return hash;
}

StatusOr<ParsedArtifact> ParseArtifact(std::span<const std::byte> bytes,
                                       ArtifactValidationOptions options) {
  if (bytes.size() < kArtifactHeaderSize) {
    return Status::InvalidArgument("artifact header is truncated");
  }
  auto magic = ReadLittleEndian<std::uint64_t>(bytes, kMagicOffset);
  if (!magic.ok()) {
    return magic.status();
  }
  if (*magic != kArtifactMagic) {
    return Status::InvalidArgument("artifact magic mismatch");
  }
  auto version = ReadLittleEndian<std::uint32_t>(bytes, kVersionOffset);
  if (!version.ok()) {
    return version.status();
  }
  if (*version != kArtifactFormatVersion) {
    return Status::InvalidArgument("unsupported artifact format version");
  }
  auto header_size = ReadLittleEndian<std::uint32_t>(bytes, kHeaderSizeOffset);
  auto entry_size = ReadLittleEndian<std::uint32_t>(bytes, kEntrySizeOffset);
  if (!header_size.ok() || !entry_size.ok()) {
    return Status::InvalidArgument("artifact header metadata is truncated");
  }
  if (*header_size != kArtifactHeaderSize ||
      *entry_size != kArtifactSectionEntrySize) {
    return Status::InvalidArgument("unsupported artifact header layout");
  }
  auto section_count = ReadLittleEndian<std::uint32_t>(bytes,
                                                       kSectionCountOffset);
  auto checksum_algorithm = ReadLittleEndian<std::uint32_t>(
      bytes, kChecksumAlgorithmOffset);
  auto shard_count = ReadLittleEndian<std::uint32_t>(bytes, kShardCountOffset);
  auto hash_seed = ReadLittleEndian<std::uint64_t>(bytes, kHashSeedOffset);
  auto hash_version = ReadLittleEndian<std::uint32_t>(bytes,
                                                      kHashVersionOffset);
  auto schema_version = ReadLittleEndian<std::uint64_t>(bytes,
                                                        kSchemaVersionOffset);
  auto layout_fingerprint = ReadLittleEndian<std::uint64_t>(
      bytes, kLayoutFingerprintOffset);
  auto artifact_id_length = ReadLittleEndian<std::uint32_t>(
      bytes, kArtifactIdLengthOffset);
  if (!section_count.ok() || !checksum_algorithm.ok() || !shard_count.ok() ||
      !hash_seed.ok() || !hash_version.ok() || !schema_version.ok() ||
      !layout_fingerprint.ok() || !artifact_id_length.ok()) {
    return Status::InvalidArgument("artifact fixed header is truncated");
  }
  if (*checksum_algorithm !=
      static_cast<std::uint32_t>(ArtifactChecksumAlgorithm::kFnv1a64)) {
    return Status::InvalidArgument("unsupported artifact checksum algorithm");
  }
  if (*artifact_id_length > kArtifactIdCapacity ||
      kArtifactIdOffset + *artifact_id_length > kArtifactHeaderSize) {
    return Status::InvalidArgument("artifact id is out of bounds");
  }
  if (options.expected_shard_count != 0 &&
      options.expected_shard_count != *shard_count) {
    return Status::FailedPrecondition("artifact shard count mismatch");
  }
  if (options.expected_hash_version != 0 &&
      options.expected_hash_version != *hash_version) {
    return Status::FailedPrecondition("artifact hash version mismatch");
  }
  if (options.expected_hash_seed != *hash_seed) {
    return Status::FailedPrecondition("artifact hash seed mismatch");
  }

  const std::uint64_t directory_bytes =
      static_cast<std::uint64_t>(*section_count) * kArtifactSectionEntrySize;
  if (directory_bytes >
          std::numeric_limits<std::uint64_t>::max() - kArtifactHeaderSize ||
      kArtifactHeaderSize + directory_bytes > bytes.size()) {
    return Status::InvalidArgument("artifact section directory is truncated");
  }

  std::vector<ArtifactSection> sections;
  sections.reserve(*section_count);
  std::set<std::pair<std::uint32_t, std::uint32_t>> seen;
  for (std::uint32_t i = 0; i < *section_count; ++i) {
    const std::size_t entry_offset =
        kArtifactHeaderSize + i * kArtifactSectionEntrySize;
    auto type_value = ReadLittleEndian<std::uint32_t>(
        bytes, entry_offset + kEntryTypeOffset);
    auto shard_id = ReadLittleEndian<std::uint32_t>(
        bytes, entry_offset + kEntryShardIdOffset);
    auto file_offset = ReadLittleEndian<std::uint64_t>(
        bytes, entry_offset + kEntryFileOffsetOffset);
    auto length = ReadLittleEndian<std::uint64_t>(
        bytes, entry_offset + kEntryLengthOffset);
    auto checksum = ReadLittleEndian<std::uint64_t>(
        bytes, entry_offset + kEntryChecksumOffset);
    if (!type_value.ok() || !shard_id.ok() || !file_offset.ok() ||
        !length.ok() || !checksum.ok()) {
      return Status::InvalidArgument("artifact section entry is truncated");
    }
    auto type = DecodeSectionType(*type_value);
    if (!type.ok()) {
      return type.status();
    }
    if (*shard_id != kArtifactGlobalShardId && *shard_id >= *shard_count) {
      return Status::InvalidArgument("artifact section shard id is invalid");
    }
    if (!seen.insert({*type_value, *shard_id}).second) {
      return Status::InvalidArgument("duplicate artifact section");
    }
    if (const Status status = CheckBounds(bytes, *file_offset, *length);
        !status.ok()) {
      return status;
    }
    sections.push_back(ArtifactSection{
        .type = *type,
        .shard_id = *shard_id,
        .offset = *file_offset,
        .length = *length,
        .checksum = *checksum,
    });
  }

  std::vector<ArtifactSection> sorted = sections;
  std::sort(sorted.begin(), sorted.end(),
            [](const ArtifactSection& lhs, const ArtifactSection& rhs) {
              return lhs.offset < rhs.offset;
            });
  std::uint64_t previous_end = kArtifactHeaderSize + directory_bytes;
  for (const ArtifactSection& section : sorted) {
    if (section.offset < previous_end) {
      return Status::InvalidArgument("artifact sections overlap");
    }
    previous_end = section.offset + section.length;
  }

  for (const ArtifactSection& section : sections) {
    const std::span<const std::byte> section_bytes(
        bytes.data() + section.offset, section.length);
    if (Fnv1a64(section_bytes) != section.checksum) {
      return Status::FailedPrecondition("artifact section checksum mismatch");
    }
  }

  ParsedArtifact artifact;
  artifact.artifact_id = std::string(
      reinterpret_cast<const char*>(bytes.data() + kArtifactIdOffset),
      *artifact_id_length);
  artifact.shard_count = *shard_count;
  artifact.hash_seed = *hash_seed;
  artifact.hash_version = *hash_version;
  artifact.schema_version = *schema_version;
  artifact.layout_fingerprint = *layout_fingerprint;
  artifact.sections = std::move(sections);
  artifact.bytes = bytes;

  auto schema_section =
      artifact.FindSection(ArtifactSectionType::kSchemaMetadata,
                           kArtifactGlobalShardId);
  if (!schema_section.has_value()) {
    return Status::InvalidArgument("artifact schema section is missing");
  }
  auto layout = ParseSchemaSection(
      std::span<const std::byte>(bytes.data() + schema_section->offset,
                                 schema_section->length),
      artifact.schema_version, artifact.layout_fingerprint);
  if (!layout.ok()) {
    return layout.status();
  }
  artifact.layout = std::move(layout).value();

  auto progress_section =
      artifact.FindSection(ArtifactSectionType::kSourceProgress,
                           kArtifactGlobalShardId);
  if (!progress_section.has_value()) {
    return Status::InvalidArgument("artifact source progress section missing");
  }
  auto progress = ParseSourceProgressSection(
      std::span<const std::byte>(bytes.data() + progress_section->offset,
                                 progress_section->length));
  if (!progress.ok()) {
    return progress.status();
  }
  artifact.source_progress = std::move(progress).value();

  return artifact;
}

}  // namespace kv_index::internal::artifact
