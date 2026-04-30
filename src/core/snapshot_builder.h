#ifndef KV_INDEX_SRC_CORE_SNAPSHOT_BUILDER_H_
#define KV_INDEX_SRC_CORE_SNAPSHOT_BUILDER_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "kv_index/schema.h"
#include "kv_index/status.h"
#include "src/core/frozen_primary_key_index.h"
#include "src/core/row_storage.h"
#include "src/core/snapshot.h"

namespace kv_index::core {

struct SnapshotBuildOptions {
  std::uint64_t hash_seed = 0;
  std::uint32_t hash_version = 1;
};

class SnapshotBuilder {
 public:
  explicit SnapshotBuilder(std::shared_ptr<const CompiledRowLayout> layout,
                           SnapshotBuildOptions options = {});

  Status AddRow(std::uint64_t primary_key, internal::EncodedRow encoded);

  StatusOr<std::shared_ptr<const OwnedSnapshotBacking>> Seal();

 private:
  std::shared_ptr<const CompiledRowLayout> layout_;
  SnapshotBuildOptions options_;
  std::vector<std::uint64_t> primary_keys_;
  std::vector<FrozenPrimaryKeyIndexEntry> index_entries_;
  std::vector<std::byte> row_slot_bytes_;
  std::vector<OwnedSnapshotRowPayload> row_payloads_;
};

}  // namespace kv_index::core

#endif  // KV_INDEX_SRC_CORE_SNAPSHOT_BUILDER_H_
