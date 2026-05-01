#ifndef KV_INDEX_SRC_STORE_REALTIME_DELTA_H_
#define KV_INDEX_SRC_STORE_REALTIME_DELTA_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "kv_index/row.h"
#include "kv_index/schema.h"
#include "kv_index/status.h"
#include "kv_index/types.h"
#include "src/model/row_storage.h"

namespace kv_index::core {

struct RealtimeRowRef {
  std::uint64_t primary_key = 0;
  SourcePosition position;
  std::shared_ptr<const internal::EncodedRow> encoded;
  std::uint64_t row_slot_bytes = 0;
  std::uint64_t payload_pool_bytes = 0;
  bool sealed_visible = false;
};

struct RealtimeDeltaBoundary {
  std::size_t row_count = 0;
};

struct RealtimeVisibleRow {
  std::uint64_t primary_key = 0;
  SourcePosition position;
  std::shared_ptr<const internal::EncodedRow> encoded;
};

class RealtimeAtomicHashMap {
 public:
  struct Options {
    std::size_t capacity = 0;
    std::uint64_t hash_seed = 0;
    std::uint32_t hash_version = 1;
  };

  explicit RealtimeAtomicHashMap(Options options);
  RealtimeAtomicHashMap(const RealtimeAtomicHashMap&) = delete;
  RealtimeAtomicHashMap& operator=(const RealtimeAtomicHashMap&) = delete;

  Status Publish(std::uint64_t primary_key, const RealtimeRowRef* row_ref);
  StatusOr<bool> PublishAndReport(std::uint64_t primary_key,
                                  const RealtimeRowRef* row_ref);
  StatusOr<std::optional<const RealtimeRowRef*>> Get(
      std::uint64_t primary_key) const;

  Status ReserveSlotForTesting(std::uint64_t primary_key);

  std::size_t capacity() const noexcept { return capacity_; }
  std::size_t unique_key_count() const noexcept {
    return unique_key_count_.load(std::memory_order_acquire);
  }
  double load_factor() const noexcept;

 private:
  enum class SlotState : std::uint8_t {
    kEmpty = 0,
    kReserved = 1,
    kOccupied = 2,
  };

  struct Slot {
    std::atomic<SlotState> state = SlotState::kEmpty;
    std::uint64_t primary_key = 0;
    std::atomic<const RealtimeRowRef*> latest_row = nullptr;
  };

  std::size_t ProbeIndex(std::uint64_t primary_key,
                         std::size_t probe) const noexcept;
  StatusOr<bool> PublishExisting(Slot* slot, const RealtimeRowRef* row_ref);

  std::unique_ptr<Slot[]> slots_;
  std::size_t capacity_ = 0;
  std::uint64_t hash_seed_ = 0;
  std::uint32_t hash_version_ = 1;
  std::atomic<std::size_t> unique_key_count_ = 0;
};

bool ShouldCompactRealtimeDelta(const RealtimeDeltaStats& stats,
                                std::uint64_t full_snapshot_row_count,
                                const ThresholdConfig& thresholds) noexcept;

class RealtimeDeltaAtomicTable {
 public:
  struct Options {
    std::shared_ptr<const CompiledRowLayout> layout;
    std::size_t capacity = 0;
    std::uint64_t hash_seed = 0;
    std::uint32_t hash_version = 1;
  };

  explicit RealtimeDeltaAtomicTable(Options options);
  RealtimeDeltaAtomicTable(const RealtimeDeltaAtomicTable&) = delete;
  RealtimeDeltaAtomicTable& operator=(const RealtimeDeltaAtomicTable&) = delete;

  Status Publish(std::uint64_t primary_key, SourcePosition position,
                 internal::EncodedRow encoded);
  StatusOr<std::optional<Row>> Get(std::uint64_t primary_key) const;

  RealtimeDeltaBoundary CaptureCompactionBoundary() const;
  StatusOr<std::vector<RealtimeVisibleRow>> ScanVisibleRows(
      RealtimeDeltaBoundary boundary) const;

  Status ReserveSlotForTesting(std::uint64_t primary_key);
  const std::shared_ptr<const CompiledRowLayout>& layout() const noexcept {
    return layout_;
  }
  RealtimeDeltaStats stats() const;

 private:
  StatusOr<std::unique_ptr<RealtimeRowRef>> BuildRowRef(
      std::uint64_t primary_key, SourcePosition position,
      internal::EncodedRow encoded) const;

  std::shared_ptr<const CompiledRowLayout> layout_;
  RealtimeAtomicHashMap map_;
  mutable std::mutex rows_mutex_;
  std::vector<std::unique_ptr<RealtimeRowRef>> rows_;
  std::atomic<std::uint64_t> row_slot_bytes_ = 0;
  std::atomic<std::uint64_t> payload_pool_bytes_ = 0;
};

}  // namespace kv_index::core

#endif  // KV_INDEX_SRC_STORE_REALTIME_DELTA_H_
