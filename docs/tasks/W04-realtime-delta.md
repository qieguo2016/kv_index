# W04 Realtime Delta Module

## Metadata
- Status: pending
- Owner Role: controller
- Depends on: W03
- Retry Count: 0
- Last Updated: 2026-04-30

## Scope
Implement one coherent realtime delta module: source-position ordering, append-only row storage, atomic hash map, realtime table stats, and row lifetime rules.

Files in scope:
- Modify: `include/kv_index/types.h`
- Create: `src/core/realtime_delta.h`
- Create: `src/core/realtime_delta.cc`
- Create: `tests/unit/source_position_test.cc`
- Create: `tests/unit/realtime_atomic_hash_map_test.cc`
- Create: `tests/unit/realtime_delta_test.cc`
- Modify: `tests/unit/shard_state_test.cc`

Required implementation items:
- Define `SourcePosition` and document one deterministic ordering policy; treat invalid same-key cross-partition ordering as fail-closed input unless a stronger upstream contract is present.
- Implement append-only realtime row storage using the shared encoded-row materialization contract.
- Implement `RealtimeAtomicHashMap` slot creation, existing-key update ordering, reserved-slot miss semantics, and capacity exhaustion behavior.
- Implement `RealtimeDeltaAtomicTable` by combining append-only storage and the atomic hash map.
- Add threshold helpers and stats for realtime compaction triggers.
- Extend `ShardState` precedence to `realtime_delta -> compact_delta -> full_snapshot`.
- Run `bazel test //tests:unit_tests`.
- Commit with a realtime-delta-module message only after verify passes and controller requests it.

Exit criteria:
- Readers can see either the old row or the new row, never a partial row.
- Reserved slots return realtime miss immediately.
- Older source positions never overwrite newer visible rows.

## Plan Notes
- Pending.

## Implementation Log
- Pending.

## Verification Log
- Pending.

## Decisions
- Pending W03 completion.

## Open Issues
- None yet.

## Next Step
- Wait for W03.
