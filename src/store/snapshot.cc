#include "src/store/snapshot.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace kv_index::internal::store {
namespace {

StatusOr<FrozenPrimaryKeyIndexView> ValidateBackingIndex(
    const std::shared_ptr<const SnapshotBacking>& backing) {
  if (backing == nullptr) {
    return Status::FailedPrecondition("snapshot view has no backing");
  }
  return FrozenPrimaryKeyIndexView::Validate(backing->frozen_index_bytes());
}

}  // namespace

OwnedSnapshotBacking::OwnedSnapshotBacking(
    std::shared_ptr<const CompiledRowLayout> layout,
    std::vector<std::byte> frozen_index_bytes,
    std::vector<std::byte> row_slot_bytes,
    std::vector<OwnedSnapshotRowPayload> row_payloads)
    : layout_(std::move(layout)),
      frozen_index_bytes_(std::move(frozen_index_bytes)),
      row_slot_bytes_(std::move(row_slot_bytes)),
      row_payloads_(std::move(row_payloads)) {}

StatusOr<internal::model::EncodedRow> OwnedSnapshotBacking::EncodedRowAt(
    std::uint64_t row_offset) const {
  if (layout_ == nullptr) {
    return Status::FailedPrecondition("snapshot backing has no row layout");
  }
  const std::size_t row_slot_size = layout_->row_slot_size();
  if (row_slot_size == 0) {
    return Status::InvalidArgument("snapshot row slot size is zero");
  }
  if (row_offset >
      static_cast<std::uint64_t>(row_slot_bytes_.size())) {
    return Status::InvalidArgument("snapshot row offset is out of bounds");
  }
  const std::size_t offset = static_cast<std::size_t>(row_offset);
  if (row_slot_bytes_.size() - offset < row_slot_size) {
    return Status::InvalidArgument("snapshot row slot is truncated");
  }
  if (offset % row_slot_size != 0) {
    return Status::InvalidArgument("snapshot row offset is not slot aligned");
  }
  const std::size_t row_index = offset / row_slot_size;
  if (row_index >= row_payloads_.size()) {
    return Status::InvalidArgument("snapshot row payload is missing");
  }

  const OwnedSnapshotRowPayload& payload = row_payloads_[row_index];
  return internal::model::EncodedRow{
      .schema_version = layout_->schema_version(),
      .layout_fingerprint = layout_->layout_fingerprint(),
      .row_slot =
          std::vector<std::byte>(row_slot_bytes_.begin() + offset,
                                 row_slot_bytes_.begin() + offset +
                                     row_slot_size),
      .arena = payload.arena,
      .string_dictionary = payload.string_dictionary,
      .scalar_list_dictionary = payload.scalar_list_dictionary,
      .string_list_dictionary = payload.string_list_dictionary,
      .string_element_dictionary = payload.string_element_dictionary,
  };
}

StatusOr<std::vector<SnapshotRow>> EnumerateSnapshotRows(
    const SnapshotBacking& backing) {
  auto index = FrozenPrimaryKeyIndexView::Validate(backing.frozen_index_bytes());
  if (!index.ok()) {
    return index.status();
  }
  auto entries = index->Entries();
  if (!entries.ok()) {
    return entries.status();
  }
  std::vector<SnapshotRow> rows;
  rows.reserve(entries->size());
  for (const FrozenPrimaryKeyIndexEntry& entry : entries.value()) {
    auto encoded = backing.EncodedRowAt(entry.row_offset);
    if (!encoded.ok()) {
      return encoded.status();
    }
    rows.push_back(SnapshotRow{
        .primary_key = entry.primary_key,
        .encoded = std::move(encoded).value(),
    });
  }
  return rows;
}

ImmutableRowSnapshotView::ImmutableRowSnapshotView(
    std::shared_ptr<const SnapshotBacking> backing)
    : backing_(std::move(backing)), index_(ValidateBackingIndex(backing_)) {}

StatusOr<std::optional<Row>> ImmutableRowSnapshotView::Get(
    std::uint64_t primary_key) const {
  if (!index_.ok()) {
    return index_.status();
  }

  auto row_offset = index_->Lookup(primary_key);
  if (!row_offset.ok()) {
    return row_offset.status();
  }
  if (!row_offset->has_value()) {
    return std::optional<Row>();
  }

  auto encoded = backing_->EncodedRowAt(row_offset->value());
  if (!encoded.ok()) {
    return encoded.status();
  }
  auto row = internal::model::MaterializeRow(backing_->layout(),
                                      std::move(encoded).value());
  if (!row.ok()) {
    return row.status();
  }
  return std::optional<Row>(std::move(row).value());
}

FullSnapshotView::FullSnapshotView(
    std::shared_ptr<const OwnedSnapshotBacking> backing)
    : backing_(std::move(backing)), view_(backing_) {}

FullSnapshotView::FullSnapshotView(
    std::shared_ptr<const SnapshotBacking> backing)
    : backing_(std::move(backing)), view_(backing_) {}

StatusOr<std::optional<Row>> FullSnapshotView::Get(
    std::uint64_t primary_key) const {
  return view_.Get(primary_key);
}

CompactDeltaSnapshot::CompactDeltaSnapshot(
    std::shared_ptr<const OwnedSnapshotBacking> backing)
    : backing_(std::move(backing)), view_(backing_) {}

StatusOr<std::optional<Row>> CompactDeltaSnapshot::Get(
    std::uint64_t primary_key) const {
  return view_.Get(primary_key);
}

}  // namespace kv_index::internal::store
