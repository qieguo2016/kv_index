#ifndef KV_INDEX_SRC_CORE_SNAPSHOT_H_
#define KV_INDEX_SRC_CORE_SNAPSHOT_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "kv_index/row.h"
#include "kv_index/schema.h"
#include "kv_index/status.h"
#include "src/core/frozen_primary_key_index.h"
#include "src/core/row_storage.h"

namespace kv_index::core {

struct OwnedSnapshotRowPayload {
  std::vector<std::byte> arena;
  std::vector<std::string> string_dictionary;
  std::vector<std::vector<std::byte>> scalar_list_dictionary;
  std::vector<std::vector<std::string>> string_list_dictionary;
  std::vector<std::string> string_element_dictionary;
};

struct SnapshotRow {
  std::uint64_t primary_key = 0;
  internal::EncodedRow encoded;
};

class SnapshotBacking {
 public:
  virtual ~SnapshotBacking() = default;

  virtual const std::shared_ptr<const CompiledRowLayout>& layout()
      const noexcept = 0;
  virtual std::span<const std::byte> frozen_index_bytes() const noexcept = 0;
  virtual std::uint64_t row_count() const noexcept = 0;
  virtual StatusOr<internal::EncodedRow> EncodedRowAt(
      std::uint64_t row_offset) const = 0;
};

StatusOr<std::vector<SnapshotRow>> EnumerateSnapshotRows(
    const SnapshotBacking& backing);

class OwnedSnapshotBacking final : public SnapshotBacking {
 public:
  OwnedSnapshotBacking(std::shared_ptr<const CompiledRowLayout> layout,
                       std::vector<std::byte> frozen_index_bytes,
                       std::vector<std::byte> row_slot_bytes,
                       std::vector<OwnedSnapshotRowPayload> row_payloads);

  const std::shared_ptr<const CompiledRowLayout>& layout()
      const noexcept override {
    return layout_;
  }
  const std::vector<std::byte>& owned_frozen_index_bytes() const noexcept {
    return frozen_index_bytes_;
  }
  std::span<const std::byte> frozen_index_bytes() const noexcept override {
    return std::span<const std::byte>(frozen_index_bytes_.data(),
                                      frozen_index_bytes_.size());
  }
  const std::vector<std::byte>& row_slot_bytes() const noexcept {
    return row_slot_bytes_;
  }
  const std::vector<OwnedSnapshotRowPayload>& row_payloads() const noexcept {
    return row_payloads_;
  }
  std::uint64_t row_count() const noexcept override {
    return static_cast<std::uint64_t>(row_payloads_.size());
  }

  StatusOr<internal::EncodedRow> EncodedRowAt(
      std::uint64_t row_offset) const override;

 private:
  std::shared_ptr<const CompiledRowLayout> layout_;
  std::vector<std::byte> frozen_index_bytes_;
  std::vector<std::byte> row_slot_bytes_;
  std::vector<OwnedSnapshotRowPayload> row_payloads_;
};

class ImmutableRowSnapshotView {
 public:
  explicit ImmutableRowSnapshotView(
      std::shared_ptr<const SnapshotBacking> backing);

  StatusOr<std::optional<Row>> Get(std::uint64_t primary_key) const;

 private:
  std::shared_ptr<const SnapshotBacking> backing_;
  StatusOr<FrozenPrimaryKeyIndexView> index_;
};

class FullSnapshotView {
 public:
  explicit FullSnapshotView(std::shared_ptr<const OwnedSnapshotBacking> backing);
  explicit FullSnapshotView(std::shared_ptr<const SnapshotBacking> backing);

  StatusOr<std::optional<Row>> Get(std::uint64_t primary_key) const;
  const std::shared_ptr<const SnapshotBacking>& backing() const noexcept {
    return backing_;
  }

 private:
  std::shared_ptr<const SnapshotBacking> backing_;
  ImmutableRowSnapshotView view_;
};

class CompactDeltaSnapshot {
 public:
  explicit CompactDeltaSnapshot(
      std::shared_ptr<const OwnedSnapshotBacking> backing);

  StatusOr<std::optional<Row>> Get(std::uint64_t primary_key) const;
  const std::shared_ptr<const OwnedSnapshotBacking>& backing() const noexcept {
    return backing_;
  }

 private:
  std::shared_ptr<const OwnedSnapshotBacking> backing_;
  ImmutableRowSnapshotView view_;
};

}  // namespace kv_index::core

#endif  // KV_INDEX_SRC_CORE_SNAPSHOT_H_
