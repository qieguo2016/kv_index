# W03 Serving Read Path

## Metadata
- Status: pending
- Owner Role: controller
- Depends on: W02
- Retry Count: 0
- Last Updated: 2026-04-30

## Scope
Connect immutable snapshots into shard-local serving state and then into the public `ForwardIndex` read API.

Files in scope:
- Create: `src/core/shard_state.h`
- Create: `src/core/shard_state.cc`
- Create: `src/core/shard_directory.h`
- Create: `src/core/shard_directory.cc`
- Create: `src/core/test_peer.h`
- Modify: `include/kv_index/forward_index.h`
- Modify: `src/core/forward_index.cc`
- Create: `tests/unit/shard_state_test.cc`
- Create: `tests/unit/shard_directory_test.cc`
- Modify: `tests/unit/forward_index_test.cc`
- Modify: `tests/integration/forward_index_integration_test.cc`

Required implementation items:
- Implement `ShardState` precedence for `compact_delta -> full_snapshot` first, with a placeholder empty realtime layer.
- Implement `ShardDirectory` atomic publish/load semantics with `std::atomic<std::shared_ptr<const ShardState>>`.
- Wire public `ForwardIndex::Get` and `MGet` through `ShardDirectory`.
- Preserve input order and duplicate-key behavior for `MGet`.
- Use test peers for installing shard state in white-box tests; do not add any business-facing publish API.
- Run `bazel test //tests:all_tests`.
- Commit with a serving-read-path message only after verify passes and controller requests it.

Exit criteria:
- Public reads route through pinned shard state.
- `MGet` groups by shard internally but preserves caller-visible order.
- Compact rows override full rows and there is never partial field merging.

## Plan Notes
- Pending.

## Implementation Log
- Pending.

## Verification Log
- Pending.

## Decisions
- Pending W02 completion.

## Open Issues
- None yet.

## Next Step
- Wait for W02.
