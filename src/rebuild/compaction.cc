#include "src/rebuild/compaction.h"

#include <memory>
#include <unordered_set>
#include <utility>

namespace kv_index::internal::rebuild {
namespace {

StatusOr<std::shared_ptr<const CompiledRowLayout>> ResolveLayout(
    const runtime::ShardState& state) {
  if (state.realtime_delta() != nullptr &&
      state.realtime_delta()->layout() != nullptr) {
    return state.realtime_delta()->layout();
  }
  if (state.compact_delta().has_value() &&
      state.compact_delta()->backing() != nullptr) {
    return state.compact_delta()->backing()->layout();
  }
  if (state.full_snapshot().has_value() &&
      state.full_snapshot()->backing() != nullptr) {
    return state.full_snapshot()->backing()->layout();
  }
  return Status::FailedPrecondition("compaction state has no row layout");
}

Status AddRowIfNew(store::SnapshotBuilder* builder,
                   std::unordered_set<std::uint64_t>* seen,
                   std::uint64_t primary_key,
                   internal::model::EncodedRow encoded) {
  if (builder == nullptr || seen == nullptr) {
    return Status::InvalidArgument("compaction row sink is null");
  }
  if (!seen->insert(primary_key).second) {
    return Status::Ok();
  }
  return builder->AddRow(primary_key, std::move(encoded));
}

}  // namespace

StatusOr<std::shared_ptr<const store::OwnedSnapshotBacking>>
BuildCompactedDeltaSnapshot(const CompactionBuildRequest& request) {
  auto layout = ResolveLayout(request.state);
  if (!layout.ok()) {
    return layout.status();
  }

  store::SnapshotBuilder builder(layout.value(), request.build_options);
  std::unordered_set<std::uint64_t> seen;

  if (request.state.realtime_delta() != nullptr) {
    auto rows = request.state.realtime_delta()->ScanVisibleRows(
        request.boundary);
    if (!rows.ok()) {
      return rows.status();
    }
    seen.reserve(rows->size());
    for (const store::RealtimeVisibleRow& row : rows.value()) {
      if (row.encoded == nullptr) {
        return Status::Internal("compaction realtime row has no encoded row");
      }
      if (const Status status =
              AddRowIfNew(&builder, &seen, row.primary_key, *row.encoded);
          !status.ok()) {
        return status;
      }
    }
  }

  if (request.state.compact_delta().has_value()) {
    const auto& compact = request.state.compact_delta().value();
    if (compact.backing() == nullptr) {
      return Status::FailedPrecondition("compact delta has no backing");
    }
    auto rows = EnumerateSnapshotRows(*compact.backing());
    if (!rows.ok()) {
      return rows.status();
    }
    seen.reserve(seen.size() + rows->size());
    for (store::SnapshotRow& row : rows.value()) {
      if (const Status status =
              AddRowIfNew(&builder, &seen, row.primary_key,
                          std::move(row.encoded));
          !status.ok()) {
        return status;
      }
    }
  }

  return builder.Seal();
}

StatusOr<std::shared_ptr<const runtime::ShardState>> FinishDeltaCompaction(
    FinishDeltaCompactionRequest request) {
  if (request.compact_backing == nullptr) {
    return Status::InvalidArgument("compaction compact backing is null");
  }
  return std::make_shared<const runtime::ShardState>(
      request.previous.ShardId(), request.successor_generation,
      runtime::ShardState::Layers{
          .realtime_delta = std::move(request.successor_realtime),
          .compact_delta = store::CompactDeltaSnapshot(
              std::move(request.compact_backing)),
          .full_snapshot = request.previous.full_snapshot(),
      });
}

}  // namespace kv_index::internal::rebuild
