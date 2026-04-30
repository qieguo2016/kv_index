# W07 Delta Compaction And Internal Full Rebase

## Metadata
- Status: pending
- Owner Role: controller
- Depends on: W06
- Retry Count: 0
- Last Updated: 2026-04-30

## Scope
Implement both internal generation-maintenance workflows as one family of modules that reuse the same routing, row materialization, and lifetime contracts.

Files in scope:
- Create: `src/core/compaction.h`
- Create: `src/core/compaction.cc`
- Create: `src/core/full_rebase.h`
- Create: `src/core/full_rebase.cc`
- Create: `tests/unit/compaction_test.cc`
- Create: `tests/unit/full_rebase_test.cc`
- Create: `tests/integration/compaction_rebase_integration_test.cc`

Required implementation items:
- Implement realtime boundary capture and sealed-row scanning for compaction.
- Build new compact snapshots as "sealed realtime overlay old compact" without scanning full snapshot during compaction.
- Implement compaction cutover by reusing generation routing from the update pipeline.
- Implement full rebase eligibility checks and conflict handling with external async load.
- Build rebased full snapshots as "compact overlay full" using the same immutable snapshot path as owned full snapshots.
- Implement rebase cutover and verify that updates published during rebase remain visible through rebase realtime.
- Run `bazel test //tests:unit_tests //tests:integration_tests`.
- Commit with a compaction-rebase message only after verify passes and controller requests it.

Exit criteria:
- Compaction and rebase both reuse existing generation-routing and row-materialization contracts.
- Old generations remain alive while rows from them are still pinned.
- External async load retains priority over internal full rebase.

## Plan Notes
- Pending.

## Implementation Log
- Pending.

## Verification Log
- Pending.

## Decisions
- Pending W06 completion.

## Open Issues
- None yet.

## Next Step
- Wait for W06.
