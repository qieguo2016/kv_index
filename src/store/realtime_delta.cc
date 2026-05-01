#include "src/store/realtime_delta.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "src/base/hash.h"

namespace kv_index::store {
namespace {

bool IsPowerOfTwo(std::size_t value) noexcept {
  return value != 0 && (value & (value - 1)) == 0;
}

std::uint64_t StringBytes(const std::vector<std::string>& values) noexcept {
  std::uint64_t bytes = 0;
  for (const std::string& value : values) {
    bytes += value.size();
  }
  return bytes;
}

std::uint64_t PayloadPoolBytes(const model::EncodedRow& encoded) noexcept {
  std::uint64_t bytes = encoded.arena.size();
  bytes += StringBytes(encoded.string_dictionary);
  for (const auto& values : encoded.scalar_list_dictionary) {
    bytes += values.size();
  }
  for (const auto& values : encoded.string_list_dictionary) {
    bytes += StringBytes(values);
  }
  bytes += StringBytes(encoded.string_element_dictionary);
  return bytes;
}

}  // namespace

RealtimeAtomicHashMap::RealtimeAtomicHashMap(Options options)
    : slots_(options.capacity == 0 ? nullptr
                                   : std::make_unique<Slot[]>(options.capacity)),
      capacity_(options.capacity),
      hash_seed_(options.hash_seed),
      hash_version_(options.hash_version) {}

double RealtimeAtomicHashMap::load_factor() const noexcept {
  if (capacity_ == 0) {
    return 0.0;
  }
  return static_cast<double>(unique_key_count()) / static_cast<double>(capacity_);
}

std::size_t RealtimeAtomicHashMap::ProbeIndex(
    std::uint64_t primary_key, std::size_t probe) const noexcept {
  const std::uint64_t hash =
      base::StableHash64(primary_key, hash_seed_, hash_version_);
  if (IsPowerOfTwo(capacity_)) {
    return static_cast<std::size_t>((hash + probe) & (capacity_ - 1));
  }
  return static_cast<std::size_t>((hash + probe) % capacity_);
}

StatusOr<bool> RealtimeAtomicHashMap::PublishExisting(
    Slot* slot, const RealtimeRowRef* row_ref) {
  while (true) {
    const RealtimeRowRef* current =
        slot->latest_row.load(std::memory_order_acquire);
    if (current == nullptr) {
      return Status::Internal("occupied realtime slot has no visible row");
    }

    auto decision =
        ClassifySourcePositionUpdate(current->position, row_ref->position);
    if (!decision.ok()) {
      return decision.status();
    }
    if (decision.value() != SourcePositionUpdateDecision::kNewer) {
      return false;
    }

    if (slot->latest_row.compare_exchange_weak(
            current, row_ref, std::memory_order_release,
            std::memory_order_acquire)) {
      return true;
    }
  }
}

Status RealtimeAtomicHashMap::Publish(std::uint64_t primary_key,
                                      const RealtimeRowRef* row_ref) {
  auto result = PublishAndReport(primary_key, row_ref);
  if (!result.ok()) {
    return result.status();
  }
  return Status::Ok();
}

StatusOr<bool> RealtimeAtomicHashMap::PublishAndReport(
    std::uint64_t primary_key, const RealtimeRowRef* row_ref) {
  if (row_ref == nullptr) {
    return Status::InvalidArgument("realtime row ref must not be null");
  }
  if (row_ref->primary_key != primary_key) {
    return Status::InvalidArgument("realtime row ref primary key mismatch");
  }
  if (!IsValidSourcePosition(row_ref->position)) {
    return Status::InvalidArgument("source position is invalid");
  }
  if (capacity_ == 0) {
    return Status::FailedPrecondition("realtime hash map capacity exhausted");
  }

  for (std::size_t probe = 0; probe < capacity_; ++probe) {
    Slot& slot = slots_[ProbeIndex(primary_key, probe)];
    SlotState state = slot.state.load(std::memory_order_acquire);
    if (state == SlotState::kReserved) {
      return Status::Unavailable("realtime hash map slot is reserved");
    }
    if (state == SlotState::kOccupied) {
      if (slot.primary_key == primary_key) {
        return PublishExisting(&slot, row_ref);
      }
      continue;
    }

    SlotState expected = SlotState::kEmpty;
    if (!slot.state.compare_exchange_strong(
            expected, SlotState::kReserved, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      --probe;
      continue;
    }

    slot.primary_key = primary_key;
    slot.latest_row.store(row_ref, std::memory_order_release);
    slot.state.store(SlotState::kOccupied, std::memory_order_release);
    unique_key_count_.fetch_add(1, std::memory_order_release);
    return true;
  }

  return Status::FailedPrecondition("realtime hash map capacity exhausted");
}

StatusOr<std::optional<const RealtimeRowRef*>> RealtimeAtomicHashMap::Get(
    std::uint64_t primary_key) const {
  if (capacity_ == 0) {
    return std::optional<const RealtimeRowRef*>();
  }

  for (std::size_t probe = 0; probe < capacity_; ++probe) {
    const Slot& slot = slots_[ProbeIndex(primary_key, probe)];
    const SlotState state = slot.state.load(std::memory_order_acquire);
    if (state == SlotState::kReserved || state == SlotState::kEmpty) {
      return std::optional<const RealtimeRowRef*>();
    }
    if (slot.primary_key != primary_key) {
      continue;
    }

    const RealtimeRowRef* row =
        slot.latest_row.load(std::memory_order_acquire);
    if (row == nullptr) {
      return Status::Internal("occupied realtime slot has no visible row");
    }
    return std::optional<const RealtimeRowRef*>(row);
  }

  return std::optional<const RealtimeRowRef*>();
}

Status RealtimeAtomicHashMap::ReserveSlotForTesting(
    std::uint64_t primary_key) {
  if (capacity_ == 0) {
    return Status::FailedPrecondition("realtime hash map capacity exhausted");
  }

  for (std::size_t probe = 0; probe < capacity_; ++probe) {
    Slot& slot = slots_[ProbeIndex(primary_key, probe)];
    const SlotState state = slot.state.load(std::memory_order_acquire);
    if (state == SlotState::kReserved) {
      return Status::Unavailable("realtime hash map slot is reserved");
    }
    if (state == SlotState::kOccupied) {
      if (slot.primary_key == primary_key) {
        return Status::FailedPrecondition("realtime hash map slot is occupied");
      }
      continue;
    }

    SlotState expected = SlotState::kEmpty;
    if (slot.state.compare_exchange_strong(
            expected, SlotState::kReserved, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return Status::Ok();
    }
    --probe;
  }

  return Status::FailedPrecondition("realtime hash map capacity exhausted");
}

bool ShouldCompactRealtimeDelta(
    const RealtimeDeltaStats& stats, std::uint64_t full_snapshot_row_count,
    const ThresholdConfig& thresholds) noexcept {
  return stats.load_factor >= thresholds.realtime_delta_load_factor ||
         stats.UniqueKeyRatio(full_snapshot_row_count) >=
             thresholds.realtime_delta_unique_key_ratio ||
         stats.row_slot_bytes >= thresholds.realtime_delta_row_arena_bytes ||
         stats.payload_pool_bytes >=
             thresholds.realtime_delta_payload_pool_bytes;
}

RealtimeDeltaAtomicTable::RealtimeDeltaAtomicTable(Options options)
    : layout_(std::move(options.layout)),
      map_(RealtimeAtomicHashMap::Options{
          .capacity = options.capacity,
          .hash_seed = options.hash_seed,
          .hash_version = options.hash_version,
      }) {}

StatusOr<std::unique_ptr<RealtimeRowRef>> RealtimeDeltaAtomicTable::BuildRowRef(
    std::uint64_t primary_key, SourcePosition position,
    model::EncodedRow encoded) const {
  if (layout_ == nullptr) {
    return Status::FailedPrecondition("realtime table has no row layout");
  }
  if (!IsValidSourcePosition(position)) {
    return Status::InvalidArgument("source position is invalid");
  }
  if (encoded.schema_version != layout_->schema_version() ||
      encoded.layout_fingerprint != layout_->layout_fingerprint()) {
    return Status::FailedPrecondition(
        "encoded row metadata does not match realtime layout");
  }
  if (encoded.row_slot.size() != layout_->row_slot_size()) {
    return Status::InvalidArgument(
        "encoded row slot size does not match realtime layout");
  }

  auto validation = model::MaterializeRow(layout_, encoded);
  if (!validation.ok()) {
    return validation.status();
  }

  const std::uint64_t row_slot_bytes = encoded.row_slot.size();
  const std::uint64_t payload_pool_bytes = PayloadPoolBytes(encoded);
  auto stored = std::make_shared<const model::EncodedRow>(std::move(encoded));
  return std::make_unique<RealtimeRowRef>(RealtimeRowRef{
      .primary_key = primary_key,
      .position = position,
      .encoded = std::move(stored),
      .row_slot_bytes = row_slot_bytes,
      .payload_pool_bytes = payload_pool_bytes,
  });
}

Status RealtimeDeltaAtomicTable::Publish(std::uint64_t primary_key,
                                         SourcePosition position,
                                         model::EncodedRow encoded) {
  auto row_ref = BuildRowRef(primary_key, position, std::move(encoded));
  if (!row_ref.ok()) {
    return row_ref.status();
  }

  const RealtimeRowRef* visible_ref = row_ref->get();
  {
    std::lock_guard<std::mutex> lock(rows_mutex_);
    rows_.push_back(std::move(row_ref).value());
    auto published = map_.PublishAndReport(primary_key, visible_ref);
    if (!published.ok()) {
      return published.status();
    }
    if (published.value()) {
      rows_.back()->sealed_visible = true;
    }
    row_slot_bytes_.fetch_add(visible_ref->row_slot_bytes,
                              std::memory_order_release);
    payload_pool_bytes_.fetch_add(visible_ref->payload_pool_bytes,
                                  std::memory_order_release);
  }

  return Status::Ok();
}

StatusOr<std::optional<Row>> RealtimeDeltaAtomicTable::Get(
    std::uint64_t primary_key) const {
  if (layout_ == nullptr) {
    return Status::FailedPrecondition("realtime table has no row layout");
  }
  auto row_ref = map_.Get(primary_key);
  if (!row_ref.ok()) {
    return row_ref.status();
  }
  if (!row_ref->has_value()) {
    return std::optional<Row>();
  }
  const RealtimeRowRef* ref = row_ref->value();
  if (ref == nullptr || ref->encoded == nullptr) {
    return Status::Internal("realtime row ref has no encoded row");
  }
  return std::optional<Row>(Row(layout_, ref->encoded));
}

RealtimeDeltaBoundary RealtimeDeltaAtomicTable::CaptureCompactionBoundary()
    const {
  std::lock_guard<std::mutex> lock(rows_mutex_);
  return RealtimeDeltaBoundary{.row_count = rows_.size()};
}

StatusOr<std::vector<RealtimeVisibleRow>> RealtimeDeltaAtomicTable::
    ScanVisibleRows(RealtimeDeltaBoundary boundary) const {
  std::lock_guard<std::mutex> lock(rows_mutex_);
  const std::size_t limit = std::min(boundary.row_count, rows_.size());
  std::unordered_map<std::uint64_t, const RealtimeRowRef*> latest;
  latest.reserve(limit);
  for (std::size_t i = 0; i < limit; ++i) {
    const RealtimeRowRef* row = rows_[i].get();
    if (row == nullptr || !row->sealed_visible) {
      continue;
    }
    auto it = latest.find(row->primary_key);
    if (it == latest.end()) {
      latest.emplace(row->primary_key, row);
      continue;
    }
    auto decision =
        ClassifySourcePositionUpdate(it->second->position, row->position);
    if (!decision.ok()) {
      return decision.status();
    }
    if (decision.value() == SourcePositionUpdateDecision::kNewer) {
      it->second = row;
    }
  }

  std::vector<RealtimeVisibleRow> visible;
  visible.reserve(latest.size());
  for (const auto& [primary_key, row] : latest) {
    if (row->encoded == nullptr) {
      return Status::Internal("sealed realtime row has no encoded row");
    }
    visible.push_back(RealtimeVisibleRow{
        .primary_key = primary_key,
        .position = row->position,
        .encoded = row->encoded,
    });
  }
  std::sort(visible.begin(), visible.end(),
            [](const RealtimeVisibleRow& lhs,
               const RealtimeVisibleRow& rhs) {
              return lhs.primary_key < rhs.primary_key;
            });
  return visible;
}

Status RealtimeDeltaAtomicTable::ReserveSlotForTesting(
    std::uint64_t primary_key) {
  return map_.ReserveSlotForTesting(primary_key);
}

RealtimeDeltaStats RealtimeDeltaAtomicTable::stats() const {
  std::lock_guard<std::mutex> lock(rows_mutex_);
  return RealtimeDeltaStats{
      .hash_capacity = map_.capacity(),
      .unique_visible_keys = map_.unique_key_count(),
      .published_row_count = rows_.size(),
      .row_slot_bytes = row_slot_bytes_.load(std::memory_order_acquire),
      .payload_pool_bytes =
          payload_pool_bytes_.load(std::memory_order_acquire),
      .load_factor = map_.load_factor(),
  };
}

}  // namespace kv_index::store
