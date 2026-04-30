# W02 Immutable Snapshot Module

## Metadata
- Status: pending
- Owner Role: controller
- Depends on: W01
- Retry Count: 0
- Last Updated: 2026-04-30

## Scope
Build the shared immutable storage path used by full snapshots and compact snapshots, including frozen primary-key index, owned snapshot backing, and shared lookup/decode view.

Files in scope:
- Create: `src/core/frozen_primary_key_index.h`
- Create: `src/core/frozen_primary_key_index.cc`
- Create: `src/core/snapshot_builder.h`
- Create: `src/core/snapshot_builder.cc`
- Create: `src/core/snapshot.h`
- Create: `src/core/snapshot.cc`
- Create: `tests/unit/frozen_primary_key_index_test.cc`
- Create: `tests/unit/snapshot_test.cc`
- Modify: `tests/BUILD.bazel`
- Modify: `BUILD.bazel`

Required implementation items:
- Implement the frozen SwissTable-style primary-key index layout, validation, and lookup behavior.
- Make `SnapshotBuilder` consume the shared encoded-row materialization contract instead of raw row-slot bytes alone.
- Implement `OwnedSnapshotBacking` so it can hold frozen index bytes, row arena bytes, payload pools, and dictionary/list-dictionary data.
- Implement `ImmutableRowSnapshotView` as the only lookup/decode path for immutable snapshots.
- Add `FullSnapshotView` and `CompactDeltaSnapshot` as thin wrappers differing only in metadata and backing ownership.
- Run `bazel test //tests:unit_tests`.
- Commit with an immutable-snapshot-module message only after verify passes and controller requests it.

Exit criteria:
- A row encoded once through the shared materialization contract can be served from an owned immutable snapshot.
- Full and compact snapshot wrappers return identical decode behavior for the same backing.
- Row lifetime remains valid after local view objects are destroyed as long as the row pin is held.

## Plan Notes
- Pending.

## Implementation Log
- Pending.

## Verification Log
- Pending.

## Decisions
- Pending W01 completion.

## Open Issues
- None yet.

## Next Step
- Wait for W01.
