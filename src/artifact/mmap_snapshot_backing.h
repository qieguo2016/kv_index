#ifndef KV_INDEX_SRC_ARTIFACT_MMAP_SNAPSHOT_BACKING_H_
#define KV_INDEX_SRC_ARTIFACT_MMAP_SNAPSHOT_BACKING_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>

#include "kv_index/schema.h"
#include "kv_index/status.h"
#include "src/artifact/artifact_format.h"
#include "src/store/snapshot.h"

namespace kv_index::internal::artifact {

struct MmapSnapshotLoadOptions {
  std::uint32_t expected_shard_count = 0;
  std::uint64_t expected_hash_seed = 0;
  std::uint32_t expected_hash_version = 0;
};

class MmapSnapshotBacking final : public store::SnapshotBacking {
 public:
  MmapSnapshotBacking(const MmapSnapshotBacking&) = delete;
  MmapSnapshotBacking& operator=(const MmapSnapshotBacking&) = delete;
  ~MmapSnapshotBacking() override;

  static StatusOr<std::shared_ptr<MmapSnapshotBacking>> LoadShard(
      const std::string& artifact_uri, std::uint32_t shard_id,
      MmapSnapshotLoadOptions options);

  const std::shared_ptr<const CompiledRowLayout>& layout()
      const noexcept override {
    return artifact_.layout;
  }
  std::span<const std::byte> frozen_index_bytes() const noexcept override {
    return frozen_index_bytes_;
  }
  std::uint64_t row_count() const noexcept override { return row_count_; }
  StatusOr<internal::model::EncodedRow> EncodedRowAt(
      std::uint64_t row_offset) const override;

  Status Prewarm();
  bool prewarmed() const noexcept { return prewarmed_; }
  std::uint32_t shard_id() const noexcept { return shard_id_; }
  const ParsedArtifact& artifact() const noexcept { return artifact_; }
  std::size_t prewarm_touched_pages_for_testing() const noexcept {
    return prewarm_touched_pages_;
  }

 private:
  MmapSnapshotBacking(void* mapping, std::size_t mapping_size,
                      ParsedArtifact artifact, std::uint32_t shard_id,
                      std::span<const std::byte> frozen_index_bytes,
                      std::span<const std::byte> row_slot_bytes,
                      std::span<const std::byte> row_payload_bytes,
                      std::vector<std::span<const std::byte>> prewarm_sections);

  void* mapping_ = nullptr;
  std::size_t mapping_size_ = 0;
  ParsedArtifact artifact_;
  std::uint32_t shard_id_ = 0;
  std::span<const std::byte> frozen_index_bytes_;
  std::span<const std::byte> row_slot_bytes_;
  std::span<const std::byte> row_payload_bytes_;
  std::vector<std::span<const std::byte>> prewarm_sections_;
  std::uint64_t row_count_ = 0;
  bool prewarmed_ = false;
  std::size_t prewarm_touched_pages_ = 0;
};

}  // namespace kv_index::internal::artifact

#endif  // KV_INDEX_SRC_ARTIFACT_MMAP_SNAPSHOT_BACKING_H_
