#include "src/store/frozen_primary_key_index.h"

#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <unordered_set>
#include <utility>
#include <vector>

#include "src/base/byte_io.h"
#include "src/base/hash.h"

namespace kv_index::core {
namespace {

constexpr std::uint8_t kEmptyControl = 0x80;

std::uint8_t H2(std::uint64_t hash) noexcept {
  return static_cast<std::uint8_t>((hash >> 57U) & 0x7fU);
}

bool HasBytes(std::size_t size, std::uint64_t offset,
              std::uint64_t byte_count) {
  if (offset > static_cast<std::uint64_t>(size)) {
    return false;
  }
  return static_cast<std::uint64_t>(size) - offset >= byte_count;
}

StatusOr<std::uint64_t> CheckedAdd(std::uint64_t lhs, std::uint64_t rhs) {
  if (lhs > std::numeric_limits<std::uint64_t>::max() - rhs) {
    return Status::InvalidArgument("frozen primary-key index size overflows");
  }
  return lhs + rhs;
}

StatusOr<std::uint64_t> CheckedMul(std::uint64_t lhs, std::uint64_t rhs) {
  if (lhs != 0 && rhs > std::numeric_limits<std::uint64_t>::max() / lhs) {
    return Status::InvalidArgument("frozen primary-key index size overflows");
  }
  return lhs * rhs;
}

StatusOr<std::uint64_t> CalculateCapacity(std::uint64_t row_count,
                                          std::uint32_t group_width) {
  if (group_width == 0 ||
      group_width != FrozenPrimaryKeyIndexView::kDefaultGroupWidth) {
    return Status::InvalidArgument("unsupported frozen index group width");
  }
  if (row_count >
      (std::numeric_limits<std::uint64_t>::max() - 6U) / 8U) {
    return Status::InvalidArgument("frozen index row count is too large");
  }

  const std::uint64_t min_for_load = (row_count * 8U + 6U) / 7U;
  std::uint64_t capacity = group_width;
  while (capacity < min_for_load) {
    if (capacity > std::numeric_limits<std::uint64_t>::max() / 2U) {
      return Status::InvalidArgument("frozen index capacity is too large");
    }
    capacity *= 2U;
  }
  return capacity;
}

Status WriteHeader(std::span<std::byte> bytes,
                   const FrozenPrimaryKeyIndexMetadata& metadata) {
  if (const Status status = internal::WriteLittleEndian(
          FrozenPrimaryKeyIndexView::kMagic, bytes,
          FrozenPrimaryKeyIndexView::kMagicOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = internal::WriteLittleEndian(
          FrozenPrimaryKeyIndexView::kFormatVersion, bytes,
          FrozenPrimaryKeyIndexView::kFormatVersionOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = internal::WriteLittleEndian(
          static_cast<std::uint32_t>(
              FrozenPrimaryKeyIndexView::kSerializedHeaderSize),
          bytes, FrozenPrimaryKeyIndexView::kHeaderSizeOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = internal::WriteLittleEndian(
          metadata.row_count, bytes,
          FrozenPrimaryKeyIndexView::kRowCountOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = internal::WriteLittleEndian(
          metadata.capacity, bytes,
          FrozenPrimaryKeyIndexView::kCapacityOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = internal::WriteLittleEndian(
          metadata.hash_seed, bytes,
          FrozenPrimaryKeyIndexView::kHashSeedOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = internal::WriteLittleEndian(
          metadata.hash_version, bytes,
          FrozenPrimaryKeyIndexView::kHashVersionOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = internal::WriteLittleEndian(
          metadata.group_width, bytes,
          FrozenPrimaryKeyIndexView::kGroupWidthOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = internal::WriteLittleEndian(
          metadata.control_offset, bytes,
          FrozenPrimaryKeyIndexView::kControlOffsetOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = internal::WriteLittleEndian(
          metadata.key_offset, bytes,
          FrozenPrimaryKeyIndexView::kKeyOffsetOffset);
      !status.ok()) {
    return status;
  }
  if (const Status status = internal::WriteLittleEndian(
          metadata.row_offset_offset, bytes,
          FrozenPrimaryKeyIndexView::kRowOffsetOffsetOffset);
      !status.ok()) {
    return status;
  }
  return internal::WriteLittleEndian(metadata.total_size, bytes,
                                     FrozenPrimaryKeyIndexView::kTotalSizeOffset);
}

template <typename T>
StatusOr<T> ReadHeaderField(std::span<const std::byte> bytes,
                            std::size_t offset) {
  auto value = internal::ReadLittleEndian<T>(bytes, offset);
  if (!value.ok()) {
    return Status::InvalidArgument(value.status().message());
  }
  return value.value();
}

StatusOr<FrozenPrimaryKeyIndexMetadata> ReadMetadata(
    std::span<const std::byte> bytes) {
  if (bytes.size() < FrozenPrimaryKeyIndexView::kSerializedHeaderSize) {
    return Status::InvalidArgument("frozen primary-key index header is truncated");
  }

  auto magic =
      ReadHeaderField<std::uint64_t>(bytes, FrozenPrimaryKeyIndexView::kMagicOffset);
  if (!magic.ok()) {
    return magic.status();
  }
  if (magic.value() != FrozenPrimaryKeyIndexView::kMagic) {
    return Status::InvalidArgument("frozen primary-key index magic is invalid");
  }

  auto format_version = ReadHeaderField<std::uint32_t>(
      bytes, FrozenPrimaryKeyIndexView::kFormatVersionOffset);
  if (!format_version.ok()) {
    return format_version.status();
  }
  if (format_version.value() != FrozenPrimaryKeyIndexView::kFormatVersion) {
    return Status::InvalidArgument("frozen primary-key index version is unsupported");
  }

  auto header_size = ReadHeaderField<std::uint32_t>(
      bytes, FrozenPrimaryKeyIndexView::kHeaderSizeOffset);
  if (!header_size.ok()) {
    return header_size.status();
  }
  if (header_size.value() !=
      FrozenPrimaryKeyIndexView::kSerializedHeaderSize) {
    return Status::InvalidArgument("frozen primary-key index header size is invalid");
  }

  FrozenPrimaryKeyIndexMetadata metadata;
  auto row_count = ReadHeaderField<std::uint64_t>(
      bytes, FrozenPrimaryKeyIndexView::kRowCountOffset);
  if (!row_count.ok()) {
    return row_count.status();
  }
  metadata.row_count = row_count.value();
  auto capacity = ReadHeaderField<std::uint64_t>(
      bytes, FrozenPrimaryKeyIndexView::kCapacityOffset);
  if (!capacity.ok()) {
    return capacity.status();
  }
  metadata.capacity = capacity.value();
  auto hash_seed = ReadHeaderField<std::uint64_t>(
      bytes, FrozenPrimaryKeyIndexView::kHashSeedOffset);
  if (!hash_seed.ok()) {
    return hash_seed.status();
  }
  metadata.hash_seed = hash_seed.value();
  auto hash_version = ReadHeaderField<std::uint32_t>(
      bytes, FrozenPrimaryKeyIndexView::kHashVersionOffset);
  if (!hash_version.ok()) {
    return hash_version.status();
  }
  metadata.hash_version = hash_version.value();
  auto group_width = ReadHeaderField<std::uint32_t>(
      bytes, FrozenPrimaryKeyIndexView::kGroupWidthOffset);
  if (!group_width.ok()) {
    return group_width.status();
  }
  metadata.group_width = group_width.value();
  auto control_offset = ReadHeaderField<std::uint64_t>(
      bytes, FrozenPrimaryKeyIndexView::kControlOffsetOffset);
  if (!control_offset.ok()) {
    return control_offset.status();
  }
  metadata.control_offset = control_offset.value();
  auto key_offset = ReadHeaderField<std::uint64_t>(
      bytes, FrozenPrimaryKeyIndexView::kKeyOffsetOffset);
  if (!key_offset.ok()) {
    return key_offset.status();
  }
  metadata.key_offset = key_offset.value();
  auto row_offset_offset = ReadHeaderField<std::uint64_t>(
      bytes, FrozenPrimaryKeyIndexView::kRowOffsetOffsetOffset);
  if (!row_offset_offset.ok()) {
    return row_offset_offset.status();
  }
  metadata.row_offset_offset = row_offset_offset.value();
  auto total_size = ReadHeaderField<std::uint64_t>(
      bytes, FrozenPrimaryKeyIndexView::kTotalSizeOffset);
  if (!total_size.ok()) {
    return total_size.status();
  }
  metadata.total_size = total_size.value();
  return metadata;
}

Status ValidateMetadata(const FrozenPrimaryKeyIndexMetadata& metadata,
                        std::size_t byte_size) {
  if (metadata.group_width !=
      FrozenPrimaryKeyIndexView::kDefaultGroupWidth) {
    return Status::InvalidArgument("frozen primary-key index group width is invalid");
  }
  if (metadata.capacity == 0 || !std::has_single_bit(metadata.capacity)) {
    return Status::InvalidArgument("frozen primary-key index capacity is invalid");
  }
  if (metadata.capacity < metadata.group_width) {
    return Status::InvalidArgument(
        "frozen primary-key index capacity is smaller than group width");
  }
  const std::uint64_t max_loaded_rows = (metadata.capacity / 8U) * 7U;
  if (metadata.row_count > max_loaded_rows) {
    return Status::InvalidArgument(
        "frozen primary-key index capacity is too small for row count");
  }

  auto control_bytes = CheckedAdd(metadata.capacity, metadata.group_width);
  if (!control_bytes.ok()) {
    return control_bytes.status();
  }
  auto key_bytes = CheckedMul(metadata.capacity, sizeof(std::uint64_t));
  if (!key_bytes.ok()) {
    return key_bytes.status();
  }
  auto row_offset_bytes = CheckedMul(metadata.capacity, sizeof(std::uint64_t));
  if (!row_offset_bytes.ok()) {
    return row_offset_bytes.status();
  }

  auto expected_key_offset =
      CheckedAdd(metadata.control_offset, control_bytes.value());
  if (!expected_key_offset.ok()) {
    return expected_key_offset.status();
  }
  auto expected_row_offset =
      CheckedAdd(metadata.key_offset, key_bytes.value());
  if (!expected_row_offset.ok()) {
    return expected_row_offset.status();
  }
  auto expected_total =
      CheckedAdd(metadata.row_offset_offset, row_offset_bytes.value());
  if (!expected_total.ok()) {
    return expected_total.status();
  }

  if (metadata.control_offset !=
          FrozenPrimaryKeyIndexView::kSerializedHeaderSize ||
      metadata.key_offset != expected_key_offset.value() ||
      metadata.row_offset_offset != expected_row_offset.value() ||
      metadata.total_size != expected_total.value()) {
    return Status::InvalidArgument("frozen primary-key index section offsets are invalid");
  }
  if (metadata.total_size != static_cast<std::uint64_t>(byte_size)) {
    return Status::InvalidArgument("frozen primary-key index byte size is invalid");
  }
  if (!HasBytes(byte_size, metadata.control_offset, control_bytes.value()) ||
      !HasBytes(byte_size, metadata.key_offset, key_bytes.value()) ||
      !HasBytes(byte_size, metadata.row_offset_offset,
                row_offset_bytes.value())) {
    return Status::InvalidArgument("frozen primary-key index section is truncated");
  }
  return Status::Ok();
}

std::size_t SlotOffset(std::uint64_t section_offset, std::uint64_t slot) {
  return static_cast<std::size_t>(section_offset +
                                  slot * sizeof(std::uint64_t));
}

}  // namespace

FrozenPrimaryKeyIndexView::FrozenPrimaryKeyIndexView(
    std::span<const std::byte> bytes, FrozenPrimaryKeyIndexMetadata metadata)
    : bytes_(bytes), metadata_(metadata) {}

StatusOr<std::uint64_t> FrozenPrimaryKeyIndexView::ReadKey(
    std::uint64_t slot) const {
  if (slot >= metadata_.capacity) {
    return Status::InvalidArgument("frozen primary-key index key slot is invalid");
  }
  auto value = internal::ReadLittleEndian<std::uint64_t>(
      bytes_, SlotOffset(metadata_.key_offset, slot));
  if (!value.ok()) {
    return Status::InvalidArgument(value.status().message());
  }
  return value.value();
}

StatusOr<std::uint64_t> FrozenPrimaryKeyIndexView::ReadRowOffset(
    std::uint64_t slot) const {
  if (slot >= metadata_.capacity) {
    return Status::InvalidArgument(
        "frozen primary-key index row-offset slot is invalid");
  }
  auto value = internal::ReadLittleEndian<std::uint64_t>(
      bytes_, SlotOffset(metadata_.row_offset_offset, slot));
  if (!value.ok()) {
    return Status::InvalidArgument(value.status().message());
  }
  return value.value();
}

StatusOr<FrozenPrimaryKeyIndexView> FrozenPrimaryKeyIndexView::Validate(
    std::span<const std::byte> bytes) {
  auto metadata = ReadMetadata(bytes);
  if (!metadata.ok()) {
    return metadata.status();
  }
  if (const Status status = ValidateMetadata(metadata.value(), bytes.size());
      !status.ok()) {
    return status;
  }

  FrozenPrimaryKeyIndexView view(bytes, metadata.value());
  const std::size_t control_offset =
      static_cast<std::size_t>(metadata->control_offset);
  for (std::uint64_t i = 0; i < metadata->group_width; ++i) {
    if (bytes[control_offset + metadata->capacity + i] !=
        bytes[control_offset + i]) {
      return Status::InvalidArgument(
          "frozen primary-key index control bytes are not mirrored");
    }
  }

  std::unordered_set<std::uint64_t> seen_keys;
  seen_keys.reserve(static_cast<std::size_t>(metadata->row_count));
  std::uint64_t occupied_count = 0;
  for (std::uint64_t slot = 0; slot < metadata->capacity; ++slot) {
    const auto control =
        std::to_integer<std::uint8_t>(bytes[control_offset + slot]);
    if (control == kEmptyControl) {
      continue;
    }
    if ((control & kEmptyControl) != 0) {
      return Status::InvalidArgument(
          "frozen primary-key index control byte is invalid");
    }

    auto key = view.ReadKey(slot);
    if (!key.ok()) {
      return key.status();
    }
    if (!seen_keys.insert(key.value()).second) {
      return Status::InvalidArgument(
          "frozen primary-key index contains duplicate primary keys");
    }
    const std::uint64_t hash =
        StableHash64(key.value(), metadata->hash_seed, metadata->hash_version);
    if (control != H2(hash)) {
      return Status::InvalidArgument(
          "frozen primary-key index control byte does not match key hash");
    }

    const std::uint64_t mask = metadata->capacity - 1U;
    std::uint64_t probe = hash & mask;
    for (std::uint64_t scanned = 0; scanned < metadata->capacity; ++scanned) {
      const auto probe_control =
          std::to_integer<std::uint8_t>(bytes[control_offset + probe]);
      if (probe == slot) {
        break;
      }
      if (probe_control == kEmptyControl) {
        return Status::InvalidArgument(
            "frozen primary-key index key is unreachable from its hash bucket");
      }
      probe = (probe + 1U) & mask;
    }

    ++occupied_count;
  }

  if (occupied_count != metadata->row_count) {
    return Status::InvalidArgument(
        "frozen primary-key index row count does not match occupied slots");
  }

  return view;
}

StatusOr<std::optional<std::uint64_t>> FrozenPrimaryKeyIndexView::Lookup(
    std::uint64_t primary_key) const {
  if (metadata_.capacity == 0) {
    return Status::InvalidArgument("frozen primary-key index view is invalid");
  }
  const std::uint64_t hash =
      StableHash64(primary_key, metadata_.hash_seed, metadata_.hash_version);
  const std::uint8_t h2 = H2(hash);
  const std::uint64_t mask = metadata_.capacity - 1U;
  const std::size_t control_offset =
      static_cast<std::size_t>(metadata_.control_offset);
  std::uint64_t slot = hash & mask;

  for (std::uint64_t scanned = 0; scanned < metadata_.capacity; ++scanned) {
    const auto control =
        std::to_integer<std::uint8_t>(bytes_[control_offset + slot]);
    if (control == kEmptyControl) {
      return std::optional<std::uint64_t>();
    }
    if (control == h2) {
      auto key = ReadKey(slot);
      if (!key.ok()) {
        return key.status();
      }
      if (key.value() == primary_key) {
        auto row_offset = ReadRowOffset(slot);
        if (!row_offset.ok()) {
          return row_offset.status();
        }
        return std::optional<std::uint64_t>(row_offset.value());
      }
    }
    slot = (slot + 1U) & mask;
  }

  return Status::InvalidArgument(
      "frozen primary-key index lookup exhausted every slot");
}

StatusOr<std::vector<FrozenPrimaryKeyIndexEntry>>
FrozenPrimaryKeyIndexView::Entries() const {
  if (metadata_.capacity == 0) {
    return Status::InvalidArgument("frozen primary-key index view is invalid");
  }
  std::vector<FrozenPrimaryKeyIndexEntry> entries;
  entries.reserve(static_cast<std::size_t>(metadata_.row_count));
  const std::size_t control_offset =
      static_cast<std::size_t>(metadata_.control_offset);
  for (std::uint64_t slot = 0; slot < metadata_.capacity; ++slot) {
    const auto control =
        std::to_integer<std::uint8_t>(bytes_[control_offset + slot]);
    if (control == kEmptyControl) {
      continue;
    }
    auto key = ReadKey(slot);
    if (!key.ok()) {
      return key.status();
    }
    auto row_offset = ReadRowOffset(slot);
    if (!row_offset.ok()) {
      return row_offset.status();
    }
    entries.push_back(FrozenPrimaryKeyIndexEntry{
        .primary_key = key.value(),
        .row_offset = row_offset.value(),
    });
  }
  return entries;
}

StatusOr<std::vector<std::byte>> BuildFrozenPrimaryKeyIndex(
    std::span<const FrozenPrimaryKeyIndexEntry> entries,
    FrozenPrimaryKeyIndexBuildOptions options) {
  auto capacity = CalculateCapacity(static_cast<std::uint64_t>(entries.size()),
                                    options.group_width);
  if (!capacity.ok()) {
    return capacity.status();
  }

  std::unordered_set<std::uint64_t> seen_keys;
  seen_keys.reserve(entries.size());
  for (const FrozenPrimaryKeyIndexEntry& entry : entries) {
    if (!seen_keys.insert(entry.primary_key).second) {
      return Status::InvalidArgument("duplicate primary key in frozen index build");
    }
  }

  auto control_bytes = CheckedAdd(capacity.value(), options.group_width);
  if (!control_bytes.ok()) {
    return control_bytes.status();
  }
  auto key_bytes = CheckedMul(capacity.value(), sizeof(std::uint64_t));
  if (!key_bytes.ok()) {
    return key_bytes.status();
  }
  auto row_offset_bytes = CheckedMul(capacity.value(), sizeof(std::uint64_t));
  if (!row_offset_bytes.ok()) {
    return row_offset_bytes.status();
  }

  FrozenPrimaryKeyIndexMetadata metadata{
      .row_count = static_cast<std::uint64_t>(entries.size()),
      .capacity = capacity.value(),
      .hash_seed = options.hash_seed,
      .hash_version = options.hash_version,
      .group_width = options.group_width,
      .control_offset = FrozenPrimaryKeyIndexView::kSerializedHeaderSize,
  };
  auto key_offset = CheckedAdd(metadata.control_offset, control_bytes.value());
  if (!key_offset.ok()) {
    return key_offset.status();
  }
  metadata.key_offset = key_offset.value();
  auto row_offset_offset = CheckedAdd(metadata.key_offset, key_bytes.value());
  if (!row_offset_offset.ok()) {
    return row_offset_offset.status();
  }
  metadata.row_offset_offset = row_offset_offset.value();
  auto total_size = CheckedAdd(metadata.row_offset_offset,
                               row_offset_bytes.value());
  if (!total_size.ok()) {
    return total_size.status();
  }
  if (total_size.value() >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return Status::InvalidArgument("frozen primary-key index is too large");
  }
  metadata.total_size = total_size.value();

  std::vector<std::byte> bytes(static_cast<std::size_t>(metadata.total_size));
  if (const Status status =
          WriteHeader(std::span<std::byte>(bytes), metadata);
      !status.ok()) {
    return status;
  }

  const std::size_t control_offset =
      static_cast<std::size_t>(metadata.control_offset);
  for (std::uint64_t i = 0; i < metadata.capacity + metadata.group_width; ++i) {
    bytes[control_offset + i] = static_cast<std::byte>(kEmptyControl);
  }

  const std::uint64_t mask = metadata.capacity - 1U;
  for (const FrozenPrimaryKeyIndexEntry& entry : entries) {
    const std::uint64_t hash =
        StableHash64(entry.primary_key, metadata.hash_seed,
                     metadata.hash_version);
    std::uint64_t slot = hash & mask;
    while (std::to_integer<std::uint8_t>(bytes[control_offset + slot]) !=
           kEmptyControl) {
      slot = (slot + 1U) & mask;
    }

    bytes[control_offset + slot] = static_cast<std::byte>(H2(hash));
    if (const Status status = internal::WriteLittleEndian(
            entry.primary_key, std::span<std::byte>(bytes),
            SlotOffset(metadata.key_offset, slot));
        !status.ok()) {
      return status;
    }
    if (const Status status = internal::WriteLittleEndian(
            entry.row_offset, std::span<std::byte>(bytes),
            SlotOffset(metadata.row_offset_offset, slot));
        !status.ok()) {
      return status;
    }
  }

  for (std::uint64_t i = 0; i < metadata.group_width; ++i) {
    bytes[control_offset + metadata.capacity + i] = bytes[control_offset + i];
  }

  return bytes;
}

}  // namespace kv_index::core
