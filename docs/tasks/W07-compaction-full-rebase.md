# W07 Delta Compaction And Internal Full Rebase

## Metadata
- Status: completed
- Owner Role: controller
- Depends on: W06
- Retry Count: 1
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
- The W07 section in the detailed implementation plan is specific enough on core semantics; do not split this into separate compaction-only and rebase-only tasks unless the coding agent hits an architectural blocker. Both workflows should share internal snapshot-building, row-copying, generation routing, and cutover helpers.
- Dependency assumptions from W04-W06:
  - Realtime updates are complete-row upserts ordered by `SourcePosition`; same-key cross-partition ordering fails closed.
  - Serving precedence is already `realtime_delta -> compact_delta -> full_snapshot` through pinned `ShardState`.
  - Full and compact snapshots must continue to use `SnapshotBuilder`, `OwnedSnapshotBacking`, and `ImmutableRowSnapshotView`; mmap-backed full snapshots remain valid through `SnapshotBacking`.
  - Update routing already models active/rebuild/rebase/compaction targets through `UpdateTargetRoute`; W07 should reuse this path instead of introducing a second row parser or source-position policy.
  - External `LoadAsync`/async load owns artifact rebuild, catch-up, and cutover; internal full rebase must yield to it.
- Implementation boundaries:
  - In scope from the original W07 list: create `src/core/compaction.{h,cc}`, `src/core/full_rebase.{h,cc}`, `tests/unit/compaction_test.cc`, `tests/unit/full_rebase_test.cc`, and `tests/integration/compaction_rebase_integration_test.cc`.
  - Also in scope because the W07 file list is insufficient: modify root `BUILD.bazel` to add the new core sources/headers; modify `tests/BUILD.bazel` to add the new test targets and include them in `unit_tests`, `integration_tests`, and `all_tests`.
  - Minimal internal contract extensions are allowed in `src/core/realtime_delta.{h,cc}`, `src/core/shard_state.{h,cc}`, `src/core/snapshot.{h,cc}`, `src/core/snapshot_builder.{h,cc}`, `src/core/frozen_primary_key_index.{h,cc}`, `src/core/shard_directory.{h,cc}`, `src/core/update_applier.{h,cc}`, `src/core/update_coordinator.{h,cc}`, and `src/core/async_load.{h,cc}` only when directly needed by compaction/rebase.
  - Public API changes to `ForwardIndex` are out of scope unless they are strictly internal/test-peer plumbing already permitted by W03-W06; do not add public manual compact/rebase APIs in W07.
- Minimum executable breakdown:
  1. Add internal iteration/pinning seams first. Realtime needs a boundary capture plus sealed visible-row scan that cannot include rows appended after the boundary or rows whose publish failed/staled out. Immutable snapshots need an internal way to enumerate `(primary_key, EncodedRow)` from owned and mmap backings without changing public lookup semantics. `ShardState` needs enough internal accessors or friend helpers to build successor states while preserving layer lifetime.
  2. Implement compaction as an internal shard operation. Capture a realtime boundary, scan only sealed realtime rows up to that boundary, build a new compact snapshot as `sealed realtime overlay old compact`, and never scan the full snapshot during compaction. Use duplicate-key filtering before `SnapshotBuilder::AddRow` because the builder correctly rejects duplicates. Cut over by publishing a successor `ShardState` whose realtime layer is the compaction/rebase target for post-boundary updates, whose compact layer is the newly built compact snapshot, and whose full layer is the prior full snapshot.
  3. Implement internal full rebase as a separate internal shard operation sharing the same row-copy helpers. Eligibility should require a full snapshot, matching schema/layout/hash settings across old full and compact, no incompatible in-flight external async load, and an operation-local rebase realtime target. Build the rebased full as `compact overlay full` through the same immutable snapshot path used by owned full snapshots. After cutover, the new state should be `rebase realtime -> rebased full` with the compact layer cleared; updates published during rebase must remain visible through the rebase realtime layer.
  4. Reuse generation routing instead of inventing an operation-specific updater. Tests may construct `UpdateApplier` with active plus `kCompaction` or `kRebase` target routes to prove post-boundary updates land in the successor realtime table. Production-facing broad refactors to async load or coordinator orchestration are out of scope unless a tiny internal guard is required for the external-load-priority rule.
  5. Wire Bazel targets only after the first failing tests exist. Follow existing test style and helpers; add small unit tests for pure compaction/rebase behavior and one integration test that routes updates through `UpdateApplier` and validates serving cutover through `ShardDirectory`/`ForwardIndexTestPeer`.
- Compaction acceptance criteria:
  - Boundary capture is deterministic under concurrent append: rows after the boundary are not folded into compact.
  - Only visible sealed realtime rows are compacted; stale, idempotent, failed, or uncommitted append records do not produce compact rows.
  - New compact rows override old compact rows for the same key, and old compact rows are retained when not overwritten.
  - The full snapshot is not enumerated during compaction; full rows remain served only through the full layer.
  - Rows pinned from old realtime/compact/full layers remain valid after cutover.
- Full rebase acceptance criteria:
  - Rebase is rejected or deferred while an external async load is active; external async load has priority.
  - Rebased full contains compact rows overlaid on full rows, with no partial field merging and no schema change.
  - The compact layer is cleared after successful rebase cutover.
  - Updates routed during rebase are visible from the successor realtime layer after cutover.
  - Rebase uses immutable owned snapshot construction, not a new bespoke row storage path.
- Focused verification commands for the coding agent:
  - `bazel test //tests:compaction_test //tests:full_rebase_test`
  - `bazel test //tests:compaction_rebase_integration_test`
  - `bazel test //tests:unit_tests //tests:integration_tests`
  - `bazel build //...`
- Explicitly out of scope for W07:
  - W08 observability, final docs, benchmark work, and broad lifetime stress coverage beyond focused cutover pinning tests.
  - Public manual compaction/rebase APIs.
  - Live Kafka/broker tests.
  - Remote artifact format changes or artifact publishing changes.
  - Broad async load/coordinator refactors unless strictly required to enforce external-load priority for internal rebase.

## Implementation Log
- 2026-04-30 W07 fix retry 1 coding agent:
  - RED `bazel test //tests:full_rebase_test`: failed at `FinishFullRebaseRejectsVisiblePreviousRealtimeToAvoidDataLoss` because `FinishFullRebase` returned success when key `100` existed only in `previous.realtime_delta`, proving the previous serving realtime layer could be dropped at cutover.
  - GREEN `bazel test //tests:full_rebase_test`: added a minimal `FinishFullRebase` cutover guard that captures the previous realtime boundary, scans sealed visible rows, and returns `FailedPrecondition` if any are present. The regression also verifies the old `ShardState` still serves the previous realtime-only row after rejection.
  - GREEN `bazel test //tests:compaction_test //tests:compaction_rebase_integration_test`: updated routed integration coverage to drain the compaction successor realtime into compact before starting full rebase, proving successful rebase cutover still works when previous realtime is empty.
  - Residual risk: W07 now fails closed for any visible previous realtime rows; it does not add a broader coordinator proof that rebase realtime caught up to prior realtime. A future coordinator can add explicit evidence if it wants to permit non-empty previous realtime at rebase cutover.
- 2026-04-30 W07 coding agent:
  - RED `bazel test //tests:compaction_test`: failed to build because `src/core/compaction.h` did not exist.
  - GREEN `bazel test //tests:compaction_test`: added realtime `CaptureCompactionBoundary`/`ScanVisibleRows`, sealed-visible row tracking, snapshot enumeration, compact build, and compaction cutover.
  - RED `bazel test //tests:full_rebase_test`: failed to build because `src/core/full_rebase.h` did not exist.
  - GREEN `bazel test //tests:full_rebase_test`: added full rebase build/cutover, external async-load conflict check, compact-over-full overlay, compact clearing after cutover, and rebase realtime preservation.
  - RED `bazel test //tests:full_rebase_test`: added hash-settings mismatch eligibility coverage; failed because compact/full frozen-index hash metadata was not checked.
  - GREEN `bazel test //tests:full_rebase_test`: added compact/full frozen-index metadata compatibility validation.
  - Added integration coverage through `ForwardIndexTestPeer`, `ShardState`, `ShardDirectory`, and `UpdateApplier` routes for `kCompaction`/`kRebase`; this passed on first run because the unit-driven implementation already satisfied the routed cutover path.
- Implementation summary:
  - Created internal `compaction.{h,cc}` and `full_rebase.{h,cc}` modules.
  - Added internal realtime sealed-row scan seams that exclude stale, idempotent, failed, reserved-only, and post-boundary append records from compaction.
  - Added internal immutable snapshot enumeration for `SnapshotBacking` and backing accessors for full/compact views without changing public `ForwardIndex` lookup APIs.
  - Built compaction as sealed realtime overlay old compact, preserving full as a lower layer without scanning it.
  - Built full rebase as compact overlay full, rejected external async-load conflicts and incompatible compact/full layouts/hash settings, and cleared compact on successful cutover.
- Changed files:
  - `src/core/compaction.{h,cc}`
  - `src/core/full_rebase.{h,cc}`
  - `src/core/realtime_delta.{h,cc}`
  - `src/core/snapshot.{h,cc}`
  - `src/core/frozen_primary_key_index.{h,cc}`
  - `src/core/shard_state.h`
  - `BUILD.bazel`
  - `tests/BUILD.bazel`
  - `tests/unit/compaction_test.cc`
  - `tests/unit/full_rebase_test.cc`
  - `tests/integration/compaction_rebase_integration_test.cc`
- Known risks/concerns:
  - Mmap snapshot enumeration remains correctness-focused and may be inefficient because `EncodedRowAt` can scan payload metadata; defer optimization to W08 unless profiling demands earlier work.
  - Internal rebase conflict handling is exposed as an operation flag rather than a broad async-load coordinator refactor, preserving W07 scope.

## Verification Log
- 2026-04-30 W07 coding agent:
  - `bazel test //tests:compaction_test` - PASSED
  - `bazel test //tests:full_rebase_test` - PASSED
  - `bazel test //tests:compaction_rebase_integration_test` - PASSED
  - `bazel test //tests:unit_tests //tests:integration_tests` - PASSED
  - `bazel test //tests:all_tests` - PASSED
  - `bazel build //...` - PASSED
- 2026-04-30 independent W07 verify agent:
  - Scope reviewed:
    - Read the W07 plan section plus W04-W06 context in `docs/superpowers/plans/2026-04-30-in-memory-forward-index-detailed-implementation.md`.
    - Read this task file and inspected tracked diffs plus untracked W07 source/test files directly.
    - Reviewed `src/core/compaction.{h,cc}`, `src/core/full_rebase.{h,cc}`, realtime delta sealed-row changes, snapshot enumeration changes, frozen index enumeration, shard state accessors, update routing reuse, async-load interaction, Bazel wiring, and focused tests.
  - Commands run:
    - `git status --short` - PASSED, workspace has expected W07 edits plus pre-existing unrelated edits in W06/controller docs.
    - `git diff --check` - PASSED, no whitespace errors.
    - `bazel test //tests:compaction_test` - PASSED, cached.
    - `bazel test //tests:full_rebase_test` - PASSED, cached.
    - `bazel test //tests:compaction_rebase_integration_test` - PASSED, cached.
    - `bazel test //tests:unit_tests //tests:integration_tests` - PASSED, cached, 22 tests pass.
    - `bazel test //tests:all_tests` - PASSED, cached, 23 tests pass.
    - `bazel build //...` - PASSED.
  - Findings:
    - BLOCKING: `FinishFullRebase` can drop the previous serving realtime layer without proving that all pre-switch realtime rows are sealed into compact or caught up in the rebase realtime target. `BuildRebasedFullSnapshot` intentionally scans only compact/full, but no W07 eligibility check rejects a `ShardState` with existing realtime data, and `FinishFullRebase` publishes only `request.rebase_realtime` plus the rebased full. A fix agent should add an internal eligibility/cutover guard for the design requirement that realtime data before the rebase switch point has entered compact, or that the rebase realtime has caught up with the current serving realtime, and add a regression test where a key exists only in the previous realtime layer and must not disappear after rebase.
  - Risk assessment:
    - Compaction boundary capture and sealed-row scanning look safe with append-before-map-publication because append, map publication, and `sealed_visible` marking are under the same realtime rows mutex; stale, idempotent, failed, reserved-only, and post-boundary append records are excluded from compacted rows.
    - Snapshot enumeration is internal-only and uses the shared `SnapshotBacking::EncodedRowAt` path, so it should work for owned and mmap backings, though mmap enumeration remains intentionally inefficient.
    - Compaction builds sealed realtime over old compact and preserves full as a lower layer without scanning it.
    - Full rebase correctly overlays compact over full, rejects external async-load conflicts and compact/full schema/hash mismatches, and clears compact after cutover, but the missing previous-realtime/catch-up guard is a data-loss risk.
    - No public `ForwardIndex` manual compaction/rebase APIs or W08 docs/benchmark/observability scope creep were found.
  - Final outcome: fail
- 2026-04-30 independent W07 verify retry 1 agent:
  - Scope reviewed:
    - Re-read this task file, including the previous independent fail finding and the fix retry 1 implementation log.
    - Re-read the W07 section of `docs/superpowers/plans/2026-04-30-in-memory-forward-index-detailed-implementation.md`.
    - Inspected `src/core/full_rebase.{h,cc}`, `tests/unit/full_rebase_test.cc`, `tests/integration/compaction_rebase_integration_test.cc`, `src/core/realtime_delta.{h,cc}`, `src/core/compaction.{h,cc}`, snapshot enumeration, frozen-index enumeration, shard-state accessors, and Bazel wiring.
  - Commands run:
    - `git status --short` - PASSED; workspace contains W07 edits plus pre-existing task/controller doc edits.
    - `git diff --check` - PASSED; no whitespace errors.
    - Inspected targeted tracked diff and untracked W07 source/test files directly.
    - `bazel test //tests:full_rebase_test` - PASSED, cached.
    - `bazel test //tests:compaction_test //tests:compaction_rebase_integration_test` - PASSED, cached.
    - `bazel test //tests:unit_tests //tests:integration_tests` - PASSED, cached; 22 tests pass.
    - `bazel test //tests:all_tests` - PASSED, cached; 23 tests pass.
    - `bazel build //...` - PASSED.
  - Findings:
    - The previous blocker is resolved. `FinishFullRebase` now fails closed before cutover when the previous serving realtime layer has sealed visible rows and no explicit caught-up proof, and the regression verifies that a previous realtime-only row remains served by the old state after rejection.
    - The cutover guard uses `RealtimeDeltaAtomicTable::ScanVisibleRows` at a captured boundary, so it only considers rows that were successfully published and marked `sealed_visible`; stale, idempotent, failed, reserved-only, uncommitted, and post-boundary rows are not included by the scan.
    - Successful full rebase still works when previous realtime is absent or empty, compact overlays full, compact is cleared after cutover, and rebase realtime remains the top serving layer above the rebased full.
    - The integration adjustment is safe: it drains the compaction successor realtime into compact before full rebase, so the successful rebase path satisfies the new fail-closed requirement rather than hiding the original data-loss case.
    - Broader W07 checks pass: compaction scans sealed realtime rows up to a boundary, does not enumerate the full snapshot, overlays realtime over old compact, preserves generation lifetime through shared snapshot/table ownership, reuses update routing for compaction/rebase realtime targets, preserves external async-load priority, and avoids public API or W08 scope creep.
  - Risk assessment:
    - No blocking issues found.
    - The current internal rebase policy safely rejects any non-empty visible previous realtime layer; a future coordinator that wants to allow non-empty previous realtime must add explicit caught-up proof before relaxing this guard.
    - Mmap-backed snapshot enumeration remains correctness-focused and may be inefficient because `EncodedRowAt` scans payload metadata; this is a non-blocking W08 optimization risk already noted.
  - Final outcome: pass

## Decisions
- 2026-04-30: W06 completed in commit `f76fb14`; W07 selected as the next active task.
- 2026-04-30: W07 is ready for implementation as an internal maintenance workflow task. The original W07 file list is intentionally expanded to include Bazel wiring and minimal internal W04-W06 contract extensions where required by sealed-row scanning, immutable snapshot enumeration, generation routing, and async-load conflict checks.
- 2026-04-30: Controller authorized root `BUILD.bazel` and `tests/BUILD.bazel` edits for W07 source/test wiring, plus minimal internal-only seam additions in realtime delta, snapshot, shard state/directory, update routing, and async-load conflict checking. Public `ForwardIndex` manual compaction/rebase APIs remain out of scope.

## Open Issues
- Resolved in fix retry 1: full rebase now fails closed when previous serving realtime has visible sealed rows and no explicit caught-up proof.
- Resolved by retry verification: realtime sealed-row scanning excludes failed/stale/uncommitted records.
- Resolved by retry verification: immutable snapshots have internal-only enumeration helpers.
- Rebase over mmap-backed full snapshots may be functionally correct but inefficient if `EncodedRowAt` repeatedly scans payload metadata. Keep W07 correctness-focused; defer broad mmap payload indexing or benchmark-driven optimization to W08 unless tests reveal unacceptable behavior.
- Delete/tombstone semantics are not present in current W04-W06 contracts. W07 compaction/rebase should cover complete-row upserts only and should not invent delete semantics.

## Next Step
- Independent W07 verify retry 1 passed. Controller authorized a commit-only coding agent to create the W07 atomic commit with exactly one Codex trailer.
