#include "src/store/snapshot_builder.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <utility>
#include <vector>

namespace kv_index::core {

SnapshotBuilder::SnapshotBuilder(
    std::shared_ptr<const CompiledRowLayout> layout,
    SnapshotBuildOptions options)
    : layout_(std::move(layout)), options_(options) {}

Status SnapshotBuilder::AddRow(std::uint64_t primary_key,
                               internal::EncodedRow encoded) {
  if (layout_ == nullptr) {
    return Status::FailedPrecondition("snapshot builder has no row layout");
  }
  if (encoded.schema_version != layout_->schema_version() ||
      encoded.layout_fingerprint != layout_->layout_fingerprint()) {
    return Status::FailedPrecondition(
        "encoded row metadata does not match snapshot layout");
  }
  if (encoded.row_slot.size() < layout_->row_slot_size()) {
    return Status::InvalidArgument(
        "encoded row slot is shorter than snapshot layout");
  }
  if (std::find(primary_keys_.begin(), primary_keys_.end(), primary_key) !=
      primary_keys_.end()) {
    return Status::InvalidArgument("duplicate primary key in snapshot build");
  }
  if (row_slot_bytes_.size() >
      static_cast<std::size_t>(std::numeric_limits<std::uint64_t>::max())) {
    return Status::InvalidArgument("snapshot row arena is too large");
  }

  const std::uint64_t row_offset =
      static_cast<std::uint64_t>(row_slot_bytes_.size());
  const std::size_t row_slot_size = layout_->row_slot_size();
  row_slot_bytes_.insert(row_slot_bytes_.end(), encoded.row_slot.begin(),
                         encoded.row_slot.begin() + row_slot_size);
  row_payloads_.push_back(OwnedSnapshotRowPayload{
      .arena = std::move(encoded.arena),
      .string_dictionary = std::move(encoded.string_dictionary),
      .scalar_list_dictionary = std::move(encoded.scalar_list_dictionary),
      .string_list_dictionary = std::move(encoded.string_list_dictionary),
      .string_element_dictionary = std::move(encoded.string_element_dictionary),
  });
  primary_keys_.push_back(primary_key);
  index_entries_.push_back(FrozenPrimaryKeyIndexEntry{
      .primary_key = primary_key,
      .row_offset = row_offset,
  });
  return Status::Ok();
}

StatusOr<std::shared_ptr<const OwnedSnapshotBacking>> SnapshotBuilder::Seal() {
  if (layout_ == nullptr) {
    return Status::FailedPrecondition("snapshot builder has no row layout");
  }

  auto index_bytes = BuildFrozenPrimaryKeyIndex(
      std::span<const FrozenPrimaryKeyIndexEntry>(index_entries_.data(),
                                                  index_entries_.size()),
      FrozenPrimaryKeyIndexBuildOptions{
          .hash_seed = options_.hash_seed,
          .hash_version = options_.hash_version,
      });
  if (!index_bytes.ok()) {
    return index_bytes.status();
  }

  std::shared_ptr<const OwnedSnapshotBacking> backing =
      std::make_shared<OwnedSnapshotBacking>(
          layout_, std::move(index_bytes).value(), std::move(row_slot_bytes_),
          std::move(row_payloads_));
  return backing;
}

}  // namespace kv_index::core
