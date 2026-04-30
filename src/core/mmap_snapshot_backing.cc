#include "src/core/mmap_snapshot_backing.h"

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <span>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "src/core/byte_io.h"

namespace kv_index::core {
namespace {

using kv_index::internal::ReadLittleEndian;

StatusOr<std::string> ResolveLocalPath(const std::string& artifact_uri) {
  const std::string file_prefix = "file://";
  const std::size_t scheme = artifact_uri.find("://");
  if (scheme != std::string::npos &&
      artifact_uri.rfind(file_prefix, 0) != 0) {
    return Status::InvalidArgument("unsupported artifact uri scheme");
  }
  std::string path = artifact_uri;
  if (path.rfind(file_prefix, 0) == 0) {
    path = path.substr(file_prefix.size());
  }
  if (path.empty()) {
    return Status::InvalidArgument("artifact path is empty");
  }
  return path;
}

StatusOr<std::span<const std::byte>> SectionBytes(
    const ParsedArtifact& artifact, ArtifactSectionType type,
    std::uint32_t shard_id) {
  auto section = artifact.FindSection(type, shard_id);
  if (!section.has_value()) {
    return Status::InvalidArgument("required shard artifact section missing");
  }
  return std::span<const std::byte>(artifact.bytes.data() + section->offset,
                                    section->length);
}

StatusOr<std::string> ReadString(std::span<const std::byte> bytes,
                                 std::size_t* offset) {
  auto size = ReadLittleEndian<std::uint32_t>(bytes, *offset);
  if (!size.ok()) {
    return size.status();
  }
  *offset += sizeof(std::uint32_t);
  if (*offset > bytes.size() || bytes.size() - *offset < *size) {
    return Status::InvalidArgument("mmap row string is truncated");
  }
  std::string value(reinterpret_cast<const char*>(bytes.data() + *offset),
                    *size);
  *offset += *size;
  return value;
}

StatusOr<std::vector<std::byte>> ReadBlob(std::span<const std::byte> bytes,
                                          std::size_t* offset) {
  auto size = ReadLittleEndian<std::uint64_t>(bytes, *offset);
  if (!size.ok()) {
    return size.status();
  }
  *offset += sizeof(std::uint64_t);
  if (*offset > bytes.size() || bytes.size() - *offset < *size) {
    return Status::InvalidArgument("mmap row blob is truncated");
  }
  std::vector<std::byte> value(bytes.begin() + *offset,
                               bytes.begin() + *offset + *size);
  *offset += *size;
  return value;
}

StatusOr<std::vector<std::string>> ReadStringList(
    std::span<const std::byte> bytes, std::size_t* offset) {
  auto count = ReadLittleEndian<std::uint32_t>(bytes, *offset);
  if (!count.ok()) {
    return count.status();
  }
  *offset += sizeof(std::uint32_t);
  std::vector<std::string> values;
  values.reserve(*count);
  for (std::uint32_t i = 0; i < *count; ++i) {
    auto value = ReadString(bytes, offset);
    if (!value.ok()) {
      return value.status();
    }
    values.push_back(std::move(value).value());
  }
  return values;
}

StatusOr<OwnedSnapshotRowPayload> ReadPayloadAt(
    std::span<const std::byte> bytes, std::uint64_t wanted_index) {
  std::size_t offset = 0;
  auto row_count = ReadLittleEndian<std::uint64_t>(bytes, offset);
  if (!row_count.ok()) {
    return row_count.status();
  }
  offset += sizeof(std::uint64_t);
  if (wanted_index >= *row_count) {
    return Status::InvalidArgument("mmap row payload index is out of bounds");
  }
  for (std::uint64_t row_index = 0; row_index < *row_count; ++row_index) {
    OwnedSnapshotRowPayload payload;
    auto arena = ReadBlob(bytes, &offset);
    if (!arena.ok()) {
      return arena.status();
    }
    payload.arena = std::move(arena).value();

    auto strings = ReadStringList(bytes, &offset);
    if (!strings.ok()) {
      return strings.status();
    }
    payload.string_dictionary = std::move(strings).value();

    auto scalar_list_count = ReadLittleEndian<std::uint32_t>(bytes, offset);
    if (!scalar_list_count.ok()) {
      return scalar_list_count.status();
    }
    offset += sizeof(std::uint32_t);
    payload.scalar_list_dictionary.reserve(*scalar_list_count);
    for (std::uint32_t i = 0; i < *scalar_list_count; ++i) {
      auto list = ReadBlob(bytes, &offset);
      if (!list.ok()) {
        return list.status();
      }
      payload.scalar_list_dictionary.push_back(std::move(list).value());
    }

    auto string_list_count = ReadLittleEndian<std::uint32_t>(bytes, offset);
    if (!string_list_count.ok()) {
      return string_list_count.status();
    }
    offset += sizeof(std::uint32_t);
    payload.string_list_dictionary.reserve(*string_list_count);
    for (std::uint32_t i = 0; i < *string_list_count; ++i) {
      auto list = ReadStringList(bytes, &offset);
      if (!list.ok()) {
        return list.status();
      }
      payload.string_list_dictionary.push_back(std::move(list).value());
    }

    auto string_elements = ReadStringList(bytes, &offset);
    if (!string_elements.ok()) {
      return string_elements.status();
    }
    payload.string_element_dictionary = std::move(string_elements).value();

    if (row_index == wanted_index) {
      return payload;
    }
  }
  return Status::Internal("mmap row payload scan missed requested row");
}

Status ValidatePayloadRowCount(std::span<const std::byte> bytes,
                               std::uint64_t expected_row_count) {
  auto row_count = ReadLittleEndian<std::uint64_t>(bytes, 0);
  if (!row_count.ok()) {
    return row_count.status();
  }
  if (*row_count != expected_row_count) {
    return Status::FailedPrecondition("mmap row payload count mismatch");
  }
  if (expected_row_count == 0 && bytes.size() != sizeof(std::uint64_t)) {
    return Status::InvalidArgument("empty mmap payload section has trailing data");
  }
  if (expected_row_count > 0) {
    auto payload = ReadPayloadAt(bytes, expected_row_count - 1);
    if (!payload.ok()) {
      return payload.status();
    }
  }
  return Status::Ok();
}

StatusOr<void*> MapFile(const std::string& path, std::size_t* size_out) {
  const int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    if (errno == ENOENT) {
      return Status::NotFound("artifact file not found");
    }
    return Status::Unavailable(std::string("failed to open artifact file: ") +
                               std::strerror(errno));
  }
  struct stat statbuf {};
  if (fstat(fd, &statbuf) != 0) {
    const int saved_errno = errno;
    close(fd);
    return Status::Unavailable(std::string("failed to stat artifact file: ") +
                               std::strerror(saved_errno));
  }
  if (statbuf.st_size <= 0) {
    close(fd);
    return Status::InvalidArgument("artifact file is empty");
  }
  void* mapping = mmap(nullptr, static_cast<std::size_t>(statbuf.st_size),
                       PROT_READ, MAP_PRIVATE, fd, 0);
  const int saved_errno = errno;
  close(fd);
  if (mapping == MAP_FAILED) {
    return Status::Unavailable(std::string("failed to mmap artifact file: ") +
                               std::strerror(saved_errno));
  }
  *size_out = static_cast<std::size_t>(statbuf.st_size);
  return mapping;
}

void TouchSectionPages(std::span<const std::byte> bytes,
                       std::size_t* touched_pages, volatile std::uint8_t* sink) {
  if (bytes.empty()) {
    return;
  }
  const long page_size = sysconf(_SC_PAGESIZE);
  const std::size_t step = page_size > 0 ? static_cast<std::size_t>(page_size)
                                        : static_cast<std::size_t>(4096);
  for (std::size_t offset = 0; offset < bytes.size(); offset += step) {
    *sink ^= std::to_integer<std::uint8_t>(bytes[offset]);
    ++(*touched_pages);
  }
}

}  // namespace

MmapSnapshotBacking::MmapSnapshotBacking(
    void* mapping, std::size_t mapping_size, ParsedArtifact artifact,
    std::uint32_t shard_id, std::span<const std::byte> frozen_index_bytes,
    std::span<const std::byte> row_slot_bytes,
    std::span<const std::byte> row_payload_bytes,
    std::vector<std::span<const std::byte>> prewarm_sections)
    : mapping_(mapping),
      mapping_size_(mapping_size),
      artifact_(std::move(artifact)),
      shard_id_(shard_id),
      frozen_index_bytes_(frozen_index_bytes),
      row_slot_bytes_(row_slot_bytes),
      row_payload_bytes_(row_payload_bytes),
      prewarm_sections_(std::move(prewarm_sections)) {
  row_count_ =
      artifact_.layout->row_slot_size() == 0
          ? 0
          : row_slot_bytes_.size() / artifact_.layout->row_slot_size();
}

MmapSnapshotBacking::~MmapSnapshotBacking() {
  if (mapping_ != nullptr && mapping_size_ > 0) {
    munmap(mapping_, mapping_size_);
  }
}

StatusOr<std::shared_ptr<MmapSnapshotBacking>> MmapSnapshotBacking::LoadShard(
    const std::string& artifact_uri, std::uint32_t shard_id,
    MmapSnapshotLoadOptions options) {
  auto path = ResolveLocalPath(artifact_uri);
  if (!path.ok()) {
    return path.status();
  }

  std::size_t mapping_size = 0;
  auto mapping = MapFile(*path, &mapping_size);
  if (!mapping.ok()) {
    return mapping.status();
  }

  std::span<const std::byte> bytes(static_cast<const std::byte*>(*mapping),
                                   mapping_size);
  auto parsed = ParseArtifact(
      bytes, ArtifactValidationOptions{
                 .expected_shard_count = options.expected_shard_count,
                 .expected_hash_seed = options.expected_hash_seed,
                 .expected_hash_version = options.expected_hash_version,
             });
  if (!parsed.ok()) {
    munmap(*mapping, mapping_size);
    return parsed.status();
  }
  if (shard_id >= parsed->shard_count) {
    munmap(*mapping, mapping_size);
    return Status::InvalidArgument("requested shard id is out of range");
  }

  auto index = SectionBytes(*parsed, ArtifactSectionType::kFrozenPrimaryKeyIndex,
                            shard_id);
  auto row_slots =
      SectionBytes(*parsed, ArtifactSectionType::kRowSlots, shard_id);
  auto row_payloads =
      SectionBytes(*parsed, ArtifactSectionType::kRowArena, shard_id);
  if (!index.ok() || !row_slots.ok() || !row_payloads.ok()) {
    munmap(*mapping, mapping_size);
    if (!index.ok()) {
      return index.status();
    }
    if (!row_slots.ok()) {
      return row_slots.status();
    }
    return row_payloads.status();
  }
  const std::size_t row_slot_size = parsed->layout->row_slot_size();
  if (row_slot_size == 0 || row_slots->size() % row_slot_size != 0) {
    munmap(*mapping, mapping_size);
    return Status::InvalidArgument("mmap row slot section is misaligned");
  }
  const std::uint64_t row_count =
      static_cast<std::uint64_t>(row_slots->size() / row_slot_size);
  if (const Status status = ValidatePayloadRowCount(*row_payloads, row_count);
      !status.ok()) {
    munmap(*mapping, mapping_size);
    return status;
  }

  std::vector<std::span<const std::byte>> prewarm_sections;
  for (const ArtifactSectionType type :
       {ArtifactSectionType::kFrozenPrimaryKeyIndex,
        ArtifactSectionType::kRowSlots, ArtifactSectionType::kRowArena,
        ArtifactSectionType::kStringPools,
        ArtifactSectionType::kScalarListPools,
        ArtifactSectionType::kStringListPools,
        ArtifactSectionType::kStringElementPools}) {
    auto section = SectionBytes(*parsed, type, shard_id);
    if (!section.ok()) {
      munmap(*mapping, mapping_size);
      return section.status();
    }
    prewarm_sections.push_back(*section);
  }

  return std::shared_ptr<MmapSnapshotBacking>(new MmapSnapshotBacking(
      *mapping, mapping_size, std::move(parsed).value(), shard_id, *index,
      *row_slots, *row_payloads, std::move(prewarm_sections)));
}

StatusOr<internal::EncodedRow> MmapSnapshotBacking::EncodedRowAt(
    std::uint64_t row_offset) const {
  if (layout() == nullptr) {
    return Status::FailedPrecondition("mmap snapshot has no row layout");
  }
  const std::size_t row_slot_size = layout()->row_slot_size();
  if (row_slot_size == 0) {
    return Status::InvalidArgument("mmap row slot size is zero");
  }
  if (row_offset > row_slot_bytes_.size()) {
    return Status::InvalidArgument("mmap row offset is out of bounds");
  }
  const std::size_t offset = static_cast<std::size_t>(row_offset);
  if (row_slot_bytes_.size() - offset < row_slot_size) {
    return Status::InvalidArgument("mmap row slot is truncated");
  }
  if (offset % row_slot_size != 0) {
    return Status::InvalidArgument("mmap row offset is not slot aligned");
  }
  const std::uint64_t row_index = offset / row_slot_size;
  auto payload = ReadPayloadAt(row_payload_bytes_, row_index);
  if (!payload.ok()) {
    return payload.status();
  }
  return internal::EncodedRow{
      .schema_version = layout()->schema_version(),
      .layout_fingerprint = layout()->layout_fingerprint(),
      .row_slot = std::vector<std::byte>(row_slot_bytes_.begin() + offset,
                                         row_slot_bytes_.begin() + offset +
                                             row_slot_size),
      .arena = std::move(payload->arena),
      .string_dictionary = std::move(payload->string_dictionary),
      .scalar_list_dictionary = std::move(payload->scalar_list_dictionary),
      .string_list_dictionary = std::move(payload->string_list_dictionary),
      .string_element_dictionary =
          std::move(payload->string_element_dictionary),
  };
}

Status MmapSnapshotBacking::Prewarm() {
  volatile std::uint8_t sink = 0;
  prewarm_touched_pages_ = 0;
  for (std::span<const std::byte> section : prewarm_sections_) {
    TouchSectionPages(section, &prewarm_touched_pages_, &sink);
  }
  prewarmed_ = true;
  return Status::Ok();
}

}  // namespace kv_index::core
