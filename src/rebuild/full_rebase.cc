#include "src/rebuild/full_rebase.h"

#include <memory>
#include <unordered_set>
#include <utility>

#include "src/store/frozen_primary_key_index.h"

namespace kv_index::rebuild {
namespace {

Status CheckSameLayout(const store::SnapshotBacking& lhs,
                       const store::SnapshotBacking& rhs) {
  if (lhs.layout() == nullptr || rhs.layout() == nullptr) {
    return Status::FailedPrecondition("rebase snapshot has no row layout");
  }
  if (lhs.layout()->schema_version() != rhs.layout()->schema_version() ||
      lhs.layout()->layout_fingerprint() !=
          rhs.layout()->layout_fingerprint() ||
      lhs.layout()->row_slot_size() != rhs.layout()->row_slot_size()) {
    return Status::FailedPrecondition(
        "compact and full snapshots have different row layouts");
  }
  auto lhs_index = store::FrozenPrimaryKeyIndexView::Validate(
      lhs.frozen_index_bytes());
  if (!lhs_index.ok()) {
    return lhs_index.status();
  }
  auto rhs_index = store::FrozenPrimaryKeyIndexView::Validate(
      rhs.frozen_index_bytes());
  if (!rhs_index.ok()) {
    return rhs_index.status();
  }
  if (lhs_index->metadata().hash_seed !=
          rhs_index->metadata().hash_seed ||
      lhs_index->metadata().hash_version !=
          rhs_index->metadata().hash_version ||
      lhs_index->metadata().group_width !=
          rhs_index->metadata().group_width) {
    return Status::FailedPrecondition(
        "compact and full snapshots have different hash settings");
  }
  return Status::Ok();
}

Status AddRowIfNew(store::SnapshotBuilder* builder,
                   std::unordered_set<std::uint64_t>* seen,
                   std::uint64_t primary_key,
                   model::EncodedRow encoded) {
  if (builder == nullptr || seen == nullptr) {
    return Status::InvalidArgument("rebase row sink is null");
  }
  if (!seen->insert(primary_key).second) {
    return Status::Ok();
  }
  return builder->AddRow(primary_key, std::move(encoded));
}

Status EnsurePreviousRealtimeIsEmpty(const runtime::ShardState& previous) {
  const std::shared_ptr<const store::RealtimeDeltaAtomicTable>& realtime =
      previous.realtime_delta();
  if (realtime == nullptr) {
    return Status::Ok();
  }

  auto visible_rows =
      realtime->ScanVisibleRows(realtime->CaptureCompactionBoundary());
  if (!visible_rows.ok()) {
    return visible_rows.status();
  }
  if (!visible_rows->empty()) {
    return Status::FailedPrecondition(
        "full rebase cutover requires previous realtime delta to be empty");
  }
  return Status::Ok();
}

}  // namespace

StatusOr<std::shared_ptr<const store::OwnedSnapshotBacking>> BuildRebasedFullSnapshot(
    const FullRebaseBuildRequest& request) {
  if (request.external_async_load_active) {
    return Status::FailedPrecondition(
        "external async load is active; internal full rebase is deferred");
  }
  if (!request.state.full_snapshot().has_value() ||
      request.state.full_snapshot()->backing() == nullptr) {
    return Status::FailedPrecondition("full rebase requires a full snapshot");
  }

  const std::shared_ptr<const store::SnapshotBacking>& full_backing =
      request.state.full_snapshot()->backing();
  if (request.state.compact_delta().has_value()) {
    const auto& compact = request.state.compact_delta().value();
    if (compact.backing() == nullptr) {
      return Status::FailedPrecondition("compact delta has no backing");
    }
    if (const Status status =
            CheckSameLayout(*compact.backing(), *full_backing);
        !status.ok()) {
      return status;
    }
  }

  store::SnapshotBuilder builder(full_backing->layout(), request.build_options);
  std::unordered_set<std::uint64_t> seen;

  if (request.state.compact_delta().has_value()) {
    auto compact_rows =
        EnumerateSnapshotRows(*request.state.compact_delta()->backing());
    if (!compact_rows.ok()) {
      return compact_rows.status();
    }
    seen.reserve(compact_rows->size());
    for (store::SnapshotRow& row : compact_rows.value()) {
      if (const Status status =
              AddRowIfNew(&builder, &seen, row.primary_key,
                          std::move(row.encoded));
          !status.ok()) {
        return status;
      }
    }
  }

  auto full_rows = EnumerateSnapshotRows(*full_backing);
  if (!full_rows.ok()) {
    return full_rows.status();
  }
  seen.reserve(seen.size() + full_rows->size());
  for (store::SnapshotRow& row : full_rows.value()) {
    if (const Status status =
            AddRowIfNew(&builder, &seen, row.primary_key,
                        std::move(row.encoded));
        !status.ok()) {
      return status;
    }
  }

  return builder.Seal();
}

StatusOr<std::shared_ptr<const runtime::ShardState>> FinishFullRebase(
    FinishFullRebaseRequest request) {
  if (request.full_backing == nullptr) {
    return Status::InvalidArgument("rebased full backing is null");
  }
  if (const Status status = EnsurePreviousRealtimeIsEmpty(request.previous);
      !status.ok()) {
    return status;
  }
  return std::make_shared<const runtime::ShardState>(
      request.previous.ShardId(), request.successor_generation,
      runtime::ShardState::Layers{
          .realtime_delta = std::move(request.rebase_realtime),
          .full_snapshot = store::FullSnapshotView(std::move(request.full_backing)),
      });
}

}  // namespace kv_index::rebuild
