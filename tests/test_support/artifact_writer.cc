#include "tests/test_support/artifact_writer.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "src/artifact/artifact_format.h"
#include "src/base/byte_io.h"
#include "src/store/snapshot_builder.h"

namespace kv_index::test_support {
namespace {

using kv_index::artifact::ArtifactSection;
using kv_index::artifact::ArtifactSectionType;
using kv_index::artifact::Fnv1a64;
using kv_index::artifact::kArtifactFormatVersion;
using kv_index::artifact::kArtifactGlobalShardId;
using kv_index::artifact::kArtifactHeaderSize;
using kv_index::artifact::kArtifactMagic;
using kv_index::artifact::kArtifactSectionEntrySize;
using kv_index::base::WriteLittleEndian;
using kv_index::base::ReadLittleEndian;

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

void AppendBytes(std::vector<std::byte>* out, std::span<const std::byte> bytes) {
  out->insert(out->end(), bytes.begin(), bytes.end());
}

template <typename T>
Status AppendInt(std::vector<std::byte>* out, T value) {
  const std::size_t offset = out->size();
  out->resize(offset + sizeof(T));
  return WriteLittleEndian<T>(value, std::span<std::byte>(*out), offset);
}

Status AppendString(std::vector<std::byte>* out, const std::string& value) {
  if (value.size() > UINT32_MAX) {
    return Status::InvalidArgument("test artifact string is too large");
  }
  if (const Status status =
          AppendInt<std::uint32_t>(out, static_cast<std::uint32_t>(value.size()));
      !status.ok()) {
    return status;
  }
  AppendBytes(out, std::as_bytes(std::span<const char>(value.data(),
                                                       value.size())));
  return Status::Ok();
}

Status WriteSectionDirectoryEntry(std::vector<std::byte>* out,
                                  std::size_t entry_offset,
                                  const ArtifactSection& section) {
  if (const Status status = WriteLittleEndian<std::uint32_t>(
          static_cast<std::uint32_t>(section.type), std::span<std::byte>(*out),
          entry_offset + kEntryTypeOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = WriteLittleEndian<std::uint32_t>(
          section.shard_id, std::span<std::byte>(*out),
          entry_offset + kEntryShardIdOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = WriteLittleEndian<std::uint64_t>(
          section.offset, std::span<std::byte>(*out),
          entry_offset + kEntryFileOffsetOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = WriteLittleEndian<std::uint64_t>(
          section.length, std::span<std::byte>(*out),
          entry_offset + kEntryLengthOffset);
      !status.ok()) {
    return status;
  }
  return WriteLittleEndian<std::uint64_t>(
      section.checksum, std::span<std::byte>(*out),
      entry_offset + kEntryChecksumOffset);
}

StatusOr<std::vector<std::byte>> SerializeSchema(
    const CompiledRowLayout& layout) {
  std::vector<std::byte> bytes;
  if (const Status status =
          AppendInt<std::uint64_t>(&bytes, layout.schema_version());
      !status.ok()) {
    return status;
  }
  if (layout.fields().size() > UINT32_MAX) {
    return Status::InvalidArgument("test artifact has too many fields");
  }
  if (const Status status = AppendInt<std::uint32_t>(
          &bytes, static_cast<std::uint32_t>(layout.fields().size()));
      !status.ok()) {
    return status;
  }
  for (const FieldLayout& field : layout.fields()) {
    if (const Status status = AppendInt<std::uint32_t>(&bytes, field.field_id);
        !status.ok()) {
      return status;
    }
    bytes.push_back(static_cast<std::byte>(field.type));
    bytes.push_back(static_cast<std::byte>(field.is_list ? 1 : 0));
    bytes.push_back(static_cast<std::byte>(field.nullable ? 1 : 0));
    bytes.push_back(static_cast<std::byte>(field.encoding));
    if (const Status status = AppendString(&bytes, field.name); !status.ok()) {
      return status;
    }
  }
  return bytes;
}

StatusOr<std::vector<std::byte>> SerializeSourceProgress(
    const std::vector<TestSourceProgress>& progress) {
  std::vector<std::byte> bytes;
  if (progress.size() > UINT32_MAX) {
    return Status::InvalidArgument("too many source progress entries");
  }
  if (const Status status = AppendInt<std::uint32_t>(
          &bytes, static_cast<std::uint32_t>(progress.size()));
      !status.ok()) {
    return status;
  }
  for (const TestSourceProgress& entry : progress) {
    if (const Status status = AppendString(&bytes, entry.topic); !status.ok()) {
      return status;
    }
    if (const Status status = AppendInt<std::int32_t>(&bytes, entry.partition);
        !status.ok()) {
      return status;
    }
    if (const Status status =
            AppendInt<std::int64_t>(&bytes, entry.checkpoint_next_offset);
        !status.ok()) {
      return status;
    }
    if (const Status status =
            AppendInt<std::int64_t>(&bytes, entry.high_watermark);
        !status.ok()) {
      return status;
    }
  }
  return bytes;
}

Status AppendBlob(std::vector<std::byte>* bytes,
                  std::span<const std::byte> blob) {
  if (blob.size() > UINT64_MAX) {
    return Status::InvalidArgument("blob is too large");
  }
  if (const Status status =
          AppendInt<std::uint64_t>(bytes, static_cast<std::uint64_t>(blob.size()));
      !status.ok()) {
    return status;
  }
  AppendBytes(bytes, blob);
  return Status::Ok();
}

Status AppendStringList(std::vector<std::byte>* bytes,
                        const std::vector<std::string>& values) {
  if (values.size() > UINT32_MAX) {
    return Status::InvalidArgument("string list is too large");
  }
  if (const Status status = AppendInt<std::uint32_t>(
          bytes, static_cast<std::uint32_t>(values.size()));
      !status.ok()) {
    return status;
  }
  for (const std::string& value : values) {
    if (const Status status = AppendString(bytes, value); !status.ok()) {
      return status;
    }
  }
  return Status::Ok();
}

StatusOr<std::vector<std::byte>> SerializeRowPayloads(
    const std::vector<store::OwnedSnapshotRowPayload>& payloads) {
  std::vector<std::byte> bytes;
  if (payloads.size() > UINT64_MAX) {
    return Status::InvalidArgument("too many payloads");
  }
  if (const Status status = AppendInt<std::uint64_t>(
          &bytes, static_cast<std::uint64_t>(payloads.size()));
      !status.ok()) {
    return status;
  }
  for (const store::OwnedSnapshotRowPayload& payload : payloads) {
    if (const Status status = AppendBlob(
            &bytes, std::span<const std::byte>(payload.arena.data(),
                                               payload.arena.size()));
        !status.ok()) {
      return status;
    }
    if (const Status status = AppendStringList(&bytes, payload.string_dictionary);
        !status.ok()) {
      return status;
    }
    if (payload.scalar_list_dictionary.size() > UINT32_MAX) {
      return Status::InvalidArgument("scalar list dictionary is too large");
    }
    if (const Status status = AppendInt<std::uint32_t>(
            &bytes,
            static_cast<std::uint32_t>(payload.scalar_list_dictionary.size()));
        !status.ok()) {
      return status;
    }
    for (const std::vector<std::byte>& list : payload.scalar_list_dictionary) {
      if (const Status status = AppendBlob(
              &bytes, std::span<const std::byte>(list.data(), list.size()));
          !status.ok()) {
        return status;
      }
    }
    if (payload.string_list_dictionary.size() > UINT32_MAX) {
      return Status::InvalidArgument("string list dictionary is too large");
    }
    if (const Status status = AppendInt<std::uint32_t>(
            &bytes,
            static_cast<std::uint32_t>(payload.string_list_dictionary.size()));
        !status.ok()) {
      return status;
    }
    for (const std::vector<std::string>& list : payload.string_list_dictionary) {
      if (const Status status = AppendStringList(&bytes, list); !status.ok()) {
        return status;
      }
    }
    if (const Status status =
            AppendStringList(&bytes, payload.string_element_dictionary);
        !status.ok()) {
      return status;
    }
  }
  return bytes;
}

Status AddSection(std::vector<std::byte>* file,
                  std::vector<ArtifactSection>* sections,
                  ArtifactSectionType type, std::uint32_t shard_id,
                  std::vector<std::byte> payload) {
  ArtifactSection section{
      .type = type,
      .shard_id = shard_id,
      .offset = static_cast<std::uint64_t>(file->size()),
      .length = static_cast<std::uint64_t>(payload.size()),
      .checksum = Fnv1a64(std::span<const std::byte>(payload.data(),
                                                     payload.size())),
  };
  AppendBytes(file, std::span<const std::byte>(payload.data(), payload.size()));
  sections->push_back(section);
  return Status::Ok();
}

StatusOr<std::shared_ptr<const store::OwnedSnapshotBacking>> BuildShardSnapshot(
    const TestArtifactSpec& spec, const ArtifactShardSpec& shard) {
  store::SnapshotBuilder builder(
      spec.layout, store::SnapshotBuildOptions{.hash_seed = spec.hash_seed,
                                              .hash_version = spec.hash_version});
  for (const TestArtifactRow& row : shard.rows) {
    if (const Status status = builder.AddRow(row.primary_key, row.encoded);
        !status.ok()) {
      return status;
    }
  }
  return builder.Seal();
}

}  // namespace

Status WriteTestArtifact(const std::string& path, const TestArtifactSpec& spec) {
  if (spec.layout == nullptr) {
    return Status::InvalidArgument("test artifact layout is required");
  }
  if (spec.artifact_id.size() > kArtifactIdCapacity) {
    return Status::InvalidArgument("test artifact id is too long");
  }
  if (spec.shards.size() != spec.shard_count) {
    return Status::InvalidArgument("test artifact shard specs are incomplete");
  }

  std::vector<ArtifactSection> sections;
  const std::uint32_t section_count = static_cast<std::uint32_t>(
      1 + (spec.include_source_progress_section ? 1 : 0) +
      spec.shard_count * 7);
  std::vector<std::byte> file(kArtifactHeaderSize +
                              section_count * kArtifactSectionEntrySize);

  auto schema = SerializeSchema(*spec.layout);
  if (!schema.ok()) {
    return schema.status();
  }
  if (const Status status =
          AddSection(&file, &sections, ArtifactSectionType::kSchemaMetadata,
                     kArtifactGlobalShardId, std::move(schema).value());
      !status.ok()) {
    return status;
  }

  if (spec.include_source_progress_section) {
    auto progress = SerializeSourceProgress(spec.source_progress);
    if (!progress.ok()) {
      return progress.status();
    }
    if (const Status status =
            AddSection(&file, &sections, ArtifactSectionType::kSourceProgress,
                       kArtifactGlobalShardId, std::move(progress).value());
        !status.ok()) {
      return status;
    }
  }

  for (const ArtifactShardSpec& shard : spec.shards) {
    auto backing = BuildShardSnapshot(spec, shard);
    if (!backing.ok()) {
      return backing.status();
    }
    if (const Status status = AddSection(
            &file, &sections, ArtifactSectionType::kFrozenPrimaryKeyIndex,
            shard.shard_id,
            std::vector<std::byte>((*backing)->frozen_index_bytes().begin(),
                                   (*backing)->frozen_index_bytes().end()));
        !status.ok()) {
      return status;
    }
    if (const Status status = AddSection(
            &file, &sections, ArtifactSectionType::kRowSlots, shard.shard_id,
            std::vector<std::byte>((*backing)->row_slot_bytes().begin(),
                                   (*backing)->row_slot_bytes().end()));
        !status.ok()) {
      return status;
    }
    auto row_arena = SerializeRowPayloads((*backing)->row_payloads());
    if (!row_arena.ok()) {
      return row_arena.status();
    }
    if (const Status status =
            AddSection(&file, &sections, ArtifactSectionType::kRowArena,
                       shard.shard_id, std::move(row_arena).value());
        !status.ok()) {
      return status;
    }
    for (const ArtifactSectionType pool_type :
         {ArtifactSectionType::kStringPools,
          ArtifactSectionType::kScalarListPools,
          ArtifactSectionType::kStringListPools,
          ArtifactSectionType::kStringElementPools}) {
      if (const Status status =
              AddSection(&file, &sections, pool_type, shard.shard_id,
                         {std::byte{0}});
          !status.ok()) {
        return status;
      }
    }
  }

  if (sections.size() != section_count) {
    return Status::Internal("test artifact section count mismatch");
  }
  if (const Status status = WriteLittleEndian<std::uint64_t>(
          kArtifactMagic, std::span<std::byte>(file), kMagicOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = WriteLittleEndian<std::uint32_t>(
          kArtifactFormatVersion, std::span<std::byte>(file), kVersionOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = WriteLittleEndian<std::uint32_t>(
          kArtifactHeaderSize, std::span<std::byte>(file), kHeaderSizeOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = WriteLittleEndian<std::uint32_t>(
          kArtifactSectionEntrySize, std::span<std::byte>(file),
          kEntrySizeOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = WriteLittleEndian<std::uint32_t>(
          section_count, std::span<std::byte>(file), kSectionCountOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = WriteLittleEndian<std::uint32_t>(
          static_cast<std::uint32_t>(artifact::ArtifactChecksumAlgorithm::kFnv1a64),
          std::span<std::byte>(file), kChecksumAlgorithmOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = WriteLittleEndian<std::uint32_t>(
          spec.shard_count, std::span<std::byte>(file), kShardCountOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = WriteLittleEndian<std::uint64_t>(
          spec.hash_seed, std::span<std::byte>(file), kHashSeedOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = WriteLittleEndian<std::uint32_t>(
          spec.hash_version, std::span<std::byte>(file), kHashVersionOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = WriteLittleEndian<std::uint64_t>(
          spec.layout->schema_version(), std::span<std::byte>(file),
          kSchemaVersionOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = WriteLittleEndian<std::uint64_t>(
          spec.layout->layout_fingerprint(), std::span<std::byte>(file),
          kLayoutFingerprintOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = WriteLittleEndian<std::uint32_t>(
          static_cast<std::uint32_t>(spec.artifact_id.size()),
          std::span<std::byte>(file), kArtifactIdLengthOffset);
      !status.ok()) {
    return status;
  }
  std::copy(spec.artifact_id.begin(), spec.artifact_id.end(),
            reinterpret_cast<char*>(file.data() + kArtifactIdOffset));

  for (std::size_t i = 0; i < sections.size(); ++i) {
    if (const Status status = WriteSectionDirectoryEntry(
            &file, kArtifactHeaderSize + i * kArtifactSectionEntrySize,
            sections[i]);
        !status.ok()) {
      return status;
    }
  }

  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output) {
    return Status::Unavailable("failed to open test artifact for writing");
  }
  output.write(reinterpret_cast<const char*>(file.data()),
               static_cast<std::streamsize>(file.size()));
  if (!output) {
    return Status::Unavailable("failed to write test artifact");
  }
  return Status::Ok();
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
