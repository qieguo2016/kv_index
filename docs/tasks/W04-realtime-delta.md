# W04 Realtime Delta Module

## Metadata
- Status: completed
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
Planning conclusion:
- The original implementation plan is specific enough to define W04's module boundary, required files, and exit criteria. W04 is too broad for one undifferentiated coding pass, so keep W04 as the single active task and execute the serial sub-stages below.
- Current code context: W01 provides the shared `kv_index::internal::EncodedRow` materialization contract plus `Row(std::shared_ptr<const CompiledRowLayout>, std::shared_ptr<const EncodedRow>)`; W02 immutable snapshots decode through `ImmutableRowSnapshotView`; W03 `ShardState` currently serves `compact_delta -> full_snapshot` with an intentionally empty realtime placeholder.
- Required scope clarification: the W04 file list omits `BUILD.bazel` and `tests/BUILD.bazel`, but new realtime source files and test targets cannot compile without Bazel wiring. The controller should authorize minimal edits to those two BUILD files for W04 wiring only.

Minimal executable breakdown for coding agent:
1. Baseline, Bazel wiring, and API shape:
   - Inspect `include/kv_index/types.h`, `src/core/row_storage.h`, `src/core/snapshot.h`, `src/core/shard_state.h`, `tests/BUILD.bazel`, and current W03 shard-state tests before editing.
   - Add W04 source/test files to Bazel early enough to support test-first work: `src/core/realtime_delta.{h,cc}` in `//:kv_index_internal`, plus `source_position_test`, `realtime_atomic_hash_map_test`, and `realtime_delta_test` in `tests/BUILD.bazel`, `unit_tests`, and `all_tests`.
   - Keep all realtime implementation types internal under `src/core` except `SourcePosition` and any public threshold/stat structs that genuinely belong in `include/kv_index/types.h`.
2. SourcePosition contract:
   - Modify `include/kv_index/types.h` to define `SourcePosition` and document the deterministic ordering policy.
   - Recommended first policy: a valid position has non-negative `partition` and `offset`; positions are comparable for the same key only when they are from the same partition; higher offset wins; equal position is an idempotent no-op; lower offset is stale and must not overwrite; same-key cross-partition comparison is invalid and returns non-OK fail-closed status.
   - Add helper APIs that tests can use directly, for example validity plus an explicit comparison enum/status helper. Avoid relying on tuple ordering that would accidentally make cross-partition updates look valid.
3. Append-only realtime row storage:
   - Implement a small append-only storage owner in `src/core/realtime_delta.{h,cc}` that accepts complete `EncodedRow` values and validates schema version, layout fingerprint, row-slot size, and required-field presence against the supplied `CompiledRowLayout`.
   - Store immutable row refs with stable addresses for the lifetime of the realtime table generation. A practical first version can keep `std::shared_ptr<const EncodedRow>` inside each `RowRef` and own `RowRef` objects in an append-only container protected by a writer-side mutex. Readers must never touch partially initialized rows.
   - Track row-slot bytes and payload/dictionary bytes for stats. Rejected stale appends may remain unreachable if the implementation encodes before the ordering CAS, but they must never become visible and should be accounted for consistently.
4. RealtimeAtomicHashMap:
   - Implement a fixed-capacity, no-erase open-addressed map from primary key to atomic `RowRef*`. Use `kv_index::core::StableHash64(primary_key, hash_seed, hash_version)` for placement; do not use `std::hash`, `absl::Hash`, pointer hashing, or process-dependent hash state.
   - Allocate atomic slots/control bytes at final capacity without relying on move/copy of atomic-containing slot objects. Prefer `std::unique_ptr<Slot[]>` or another stable storage shape over a resizable vector of non-movable atomics.
   - Cover slot states `empty`, `reserved`, and `occupied`. Lookup must return realtime miss immediately when it observes `reserved`, without reading key or row pointer and without waiting/retrying.
   - For existing-key updates, compare the current and candidate `SourcePosition` before publishing. Only a strictly newer same-partition position may CAS the visible pointer; equal/lower positions must leave the visible pointer unchanged; same-key cross-partition updates must fail closed.
   - Capacity exhaustion must return a non-OK status, preferably `FailedPrecondition` with the current `StatusCode` set, and must not rehash, resize, or block in the read/write hot path.
5. RealtimeDeltaAtomicTable:
   - Combine append-only storage and `RealtimeAtomicHashMap` behind a table API such as `Publish(primary_key, position, encoded_row)` and `Get(primary_key)`.
   - `Publish` must fully validate and materialize the immutable row ref before any pointer can become visible, then publish via the map. `Get` should acquire-load the visible `RowRef*`, construct a `Row` using the shared layout/materialization contract, and return `StatusOr<std::optional<Row>>`.
   - Add realtime stats and threshold helpers for compaction triggers: hash capacity/load factor, unique visible keys, row arena bytes, payload pool bytes, and a helper that evaluates the current `ThresholdConfig` plus shard/full row count. Add `ThresholdConfig::realtime_delta_load_factor` with default `0.60` if needed to represent the design threshold.
6. ShardState integration:
   - Modify `src/core/shard_state.h` and `src/core/shard_state.cc` so `ShardState::Layers` can hold a realtime delta table above the compact/full layers.
   - Precedence must be exactly `realtime_delta -> compact_delta -> full_snapshot`. A realtime miss, including reserved-slot miss, falls through; a non-OK realtime status fails closed and must not fall through to compact/full.
   - Keep `MGet` order/duplicate behavior from W03. It may continue to loop through `Get` unless a simple batch path falls out naturally.
7. Focused verification before handoff:
   - Run focused tests after each sub-stage, then run `bazel test //tests:unit_tests`.
   - Do not commit unless the controller later requests it after verify passes.

Implementation boundaries:
- In scope for coding: `include/kv_index/types.h`, `src/core/realtime_delta.h`, `src/core/realtime_delta.cc`, the three new W04 unit tests, `tests/unit/shard_state_test.cc`, and the minimal `BUILD.bazel` / `tests/BUILD.bazel` wiring clarified above.
- Out of scope: Kafka consumer/update coordinator behavior, async load cutover, delta compaction execution, full rebase, mmap artifact loading, public manual publish APIs, and index-layer delete operations.
- Do not change W01 row/schema APIs unless realtime cannot satisfy row lifetime or validation without a narrow helper. If that happens, record the blocker instead of silently expanding scope.
- Keep serving-visible reads lock-free at the application layer. Writer-side mutexes for append-only storage ownership are acceptable if readers only use immutable row refs and atomic pointer loads.
- Preserve the rule that deletes are ordinary upsert rows with business fields; do not add delete/tombstone semantics to the realtime map.

Dependency assumptions:
- W03 commit `265e59f` is present and provides `ShardState`, `ShardDirectory`, and test peer patterns.
- W01/W02 materialization currently copies enough data into `EncodedRow`/`Row` for lifetime safety. W04 can use that contract for realtime rows instead of inventing a second encoding.
- The first realtime map can use linear probing with stable hash metadata; full SwissTable group probing is not required unless it remains simple and tested.
- The update pipeline in W05 will supply complete encoded rows and source positions. W04 only needs the local realtime table contract and tests with synthetic rows.
- Existing `Status` / `StatusOr<T>` is the fail-closed error surface for invalid ordering metadata, capacity exhaustion, and malformed encoded rows.

Risks:
- Source-position semantics can easily drift into unsafe tuple ordering. Tests must prove same-key cross-partition updates fail closed and never publish.
- Existing-key update CAS loops can accidentally let a stale row overwrite a newer one if the implementation compares once and then publishes after another writer wins. Re-read and re-compare inside the CAS loop.
- `reserved` semantics are intentionally lossy for concurrent reads. The test should assert miss/no-wait behavior, not linearizability.
- Appending before discovering a stale update can leak unreachable rows until generation cleanup. This is acceptable only if documented, bounded by compaction thresholds, and invisible to readers.
- Atomic-containing slot storage is toolchain-sensitive, similar to W03's atomic shared-pointer issue. Avoid container operations that require atomics to be movable/copyable.
- BUILD wiring is a real scope gap in the original W04 file list; without controller authorization, the coding agent cannot compile the new tests.

Concrete acceptance criteria:
- `SourcePosition` rejects invalid positions and treats same-key cross-partition ordering as fail-closed input.
- Realtime rows are appended only as complete immutable row refs using the shared `EncodedRow`/layout contract.
- Readers can see either the old complete row or the new complete row, never a partially initialized row.
- Reserved slots return realtime miss immediately and allow `ShardState` to continue to compact/full lookup.
- Older or equal source positions never overwrite newer visible rows; equal positions are idempotent no-ops.
- Capacity exhaustion fails closed without resize/rehash on the hot path.
- Realtime stats and threshold helpers report enough information to trigger compaction on load factor, unique-key ratio, row arena bytes, or payload pool bytes.
- `ShardState` precedence is `realtime_delta -> compact_delta -> full_snapshot`, with non-OK higher-layer statuses preventing lower-layer fallback.
- `bazel test //tests:unit_tests` passes before handoff to verify.

Prioritized focused tests:
- Source-position ordering and invalid metadata: `bazel test //tests:source_position_test`.
- Slot creation, reserved miss, existing-key ordering, stale/equal no-op, cross-partition fail-closed, and capacity exhaustion: `bazel test //tests:realtime_atomic_hash_map_test`.
- Complete row publication, row lifetime after table temporaries, row/payload stats, compaction threshold helper behavior, and malformed encoded-row rejection: `bazel test //tests:realtime_delta_test`.
- Read precedence and fallback behavior: `bazel test //tests:shard_state_test`.
- Task gate before handoff: `bazel test //tests:unit_tests`.

## Implementation Log
- 2026-04-30 coding agent:
  - Baseline guard: ran `bazel test //tests:unit_tests` before behavior-changing edits; result passed.
  - TDD red/green notes:
    - SourcePosition: added `tests/unit/source_position_test.cc`; RED was expected compile failure for missing `SourcePosition` / helpers; GREEN after adding `SourcePosition`, validity helper, and same-key update classifier in `include/kv_index/types.h`.
    - RealtimeAtomicHashMap: added `tests/unit/realtime_atomic_hash_map_test.cc`; RED was expected compile failure for missing `src/core/realtime_delta.h`; GREEN after adding fixed-capacity no-erase atomic map with stable-hash probing, reserved miss, stale/equal no-op, cross-partition fail-closed, and capacity exhaustion behavior.
    - RealtimeDeltaAtomicTable: added `tests/unit/realtime_delta_test.cc`; RED was expected compile failure for missing table/stats APIs; GREEN after adding append-only immutable `RealtimeRowRef` ownership, `EncodedRow` validation through the shared materialization path, `Publish` / `Get`, stats, and threshold helper.
    - ShardState integration: extended `tests/unit/shard_state_test.cc`; RED was expected compile failure for missing realtime layer / reserved-slot test hook; GREEN after adding realtime layer precedence `realtime_delta -> compact_delta -> full_snapshot`, reserved miss fallback, non-OK realtime fail-closed behavior, and MGet duplicate/order coverage.
  - Implementation summary:
    - Added public `SourcePosition`, `SourcePositionUpdateDecision`, `IsValidSourcePosition`, and `ClassifySourcePositionUpdate`; added `ThresholdConfig::realtime_delta_load_factor = 0.60`.
    - Added internal `src/core/realtime_delta.{h,cc}` with `RealtimeAtomicHashMap`, `RealtimeDeltaAtomicTable`, append-only row-ref ownership, realtime stats, and `ShouldCompactRealtimeDelta`.
    - Wired realtime source and tests through Bazel; no Abseil dependency added and no W01/W02 row/schema API changes made.
    - Extended `ShardState::Layers` with a realtime delta pointer and updated lookup precedence/fail-closed behavior.
  - Files changed:
    - `include/kv_index/types.h`
    - `src/core/realtime_delta.h`
    - `src/core/realtime_delta.cc`
    - `src/core/shard_state.h`
    - `src/core/shard_state.cc`
    - `BUILD.bazel`
    - `tests/BUILD.bazel`
    - `tests/unit/source_position_test.cc`
    - `tests/unit/realtime_atomic_hash_map_test.cc`
    - `tests/unit/realtime_delta_test.cc`
    - `tests/unit/shard_state_test.cc`
    - `docs/tasks/W04-realtime-delta.md`
  - Commands/tests run:
    - `bazel test //tests:unit_tests` before edits: passed.
    - `bazel test //tests:source_position_test`: RED compile failure before implementation, then passed; final focused rerun passed.
    - `bazel test //tests:realtime_atomic_hash_map_test`: RED missing-header compile failure before implementation, then passed; final focused rerun passed.
    - `bazel test //tests:realtime_delta_test`: RED missing-API compile failure before implementation, then passed; final focused rerun passed.
    - `bazel test //tests:shard_state_test`: RED missing-realtime-layer compile failure before implementation, then passed; final focused rerun passed.
    - `bazel test //tests:unit_tests`: passed with 12/12 test targets.
    - `bazel test //tests:all_tests`: passed with 14/14 test targets.
  - Known risks:
    - `RealtimeDeltaAtomicTable::Publish` validates and appends a complete immutable row ref before the map publish CAS, so stale/equal/cross-partition/capacity-rejected rows can remain unreachable until generation cleanup/compaction; they are invisible to readers and included in append-only byte/count stats.
    - `RealtimeAtomicHashMap` uses simple fixed-capacity linear probing rather than SwissTable-style grouped control bytes; it preserves the required stable hash, no-resize/no-erase, reserved-miss, and source-position behavior for W04.

## Verification Log
- 2026-04-30 verify agent:
  - Final decision: pass.
  - Diff/scope inspection:
    - `git rev-parse --short HEAD`: confirmed baseline `265e59f`.
    - `git status --short`: tracked edits are `BUILD.bazel`, `docs/tasks/W04-realtime-delta.md`, `docs/tasks/controller.md`, `include/kv_index/types.h`, `src/core/shard_state.cc`, `src/core/shard_state.h`, `tests/BUILD.bazel`, and `tests/unit/shard_state_test.cc`; untracked W04 files are `src/core/realtime_delta.cc`, `src/core/realtime_delta.h`, `tests/unit/realtime_atomic_hash_map_test.cc`, `tests/unit/realtime_delta_test.cc`, and `tests/unit/source_position_test.cc`.
    - `git diff --name-status HEAD`: tracked implementation edits are within W04 scope plus authorized Bazel wiring and controller status edits.
    - `git ls-files --others --exclude-standard`: listed only the expected untracked W04 realtime source/test files above.
  - Implementation review:
    - `SourcePosition` rejects negative partition/offset values, documents same-partition-only ordering, treats higher offsets as newer, treats equal/lower offsets as no-overwrite decisions, and fails closed for same-key cross-partition comparisons.
    - `RealtimeAtomicHashMap` uses `kv_index::core::StableHash64(primary_key, hash_seed, hash_version)` for placement, fixed-capacity linear probing, no erase/no resize, and `std::unique_ptr<Slot[]>` storage so atomic-containing slots are not moved or copied.
    - Reserved slots return realtime miss immediately on read without reading key or row pointer. The simplified linear-probing approach can be more lossy than SwissTable-style H2-filtered probing during concurrent slot creation, but W04 explicitly allows reserved-slot miss/no-wait behavior and the implementation keeps this fail-closed.
    - Existing-key publication re-loads the currently visible `RowRef*` inside the CAS loop, re-classifies source position before each overwrite attempt, and only publishes strictly newer same-partition rows. Equal/lower rows return OK without replacing the visible row; cross-partition rows return non-OK and keep the previous row visible.
    - `RealtimeDeltaAtomicTable::Publish` validates schema version, layout fingerprint, row-slot size, required fields, and materialization before the row can become visible. It appends a complete immutable row ref before map publish, so stale/equal/cross-partition/capacity-rejected rows may remain unreachable until generation cleanup; by inspection they are invisible to readers because reads only follow map-visible refs, and append-only row count/byte stats include them consistently.
    - Capacity exhaustion returns non-OK and does not create a visible mapping for the rejected key. The rejected row may still be retained in append-only storage, matching the documented append-before-CAS tradeoff.
    - Stats report hash capacity, visible unique keys, append-only row count, row-slot bytes, payload-pool bytes, and load factor. `ThresholdConfig::realtime_delta_load_factor` defaults to `0.60`, and `UniqueKeyRatio(0)` returns `0.0`, avoiding division by zero.
    - `ShardState` precedence is exactly `realtime_delta -> compact_delta -> full_snapshot`; realtime miss/reserved miss falls through, realtime non-OK fails closed without lower-layer fallback, and `MGet` preserves order and duplicate positions by looping through `Get`.
    - No W01/W02 row/schema/materialization APIs were changed, no public business-facing publish API was added, and no index-layer delete/tombstone semantics were introduced.
  - Commands/tests run:
    - `bazel test //tests:source_position_test`: passed from cache, 1/1 target passing.
    - `bazel test //tests:realtime_atomic_hash_map_test`: passed from cache, 1/1 target passing.
    - `bazel test //tests:realtime_delta_test`: passed from cache, 1/1 target passing.
    - `bazel test //tests:shard_state_test`: passed from cache, 1/1 target passing.
    - `bazel test //tests:unit_tests`: passed from cache, 12/12 targets passing.
    - `bazel test //tests:all_tests`: passed from cache, 14/14 targets passing.
    - `bazel test //tests:all_tests --nocache_test_results`: passed, executed 14/14 targets.
    - `git diff --check`: passed with no whitespace errors.
  - Review notes:
    - No blocking W04 plan-compliance issues, regressions, or test failures found.
    - Fixed-capacity linear probing is accepted for W04 because the task notes explicitly allowed a first realtime map without full SwissTable group probing, and the required stable hash, fixed capacity, reserved miss, source-position, and capacity behavior are implemented and covered.
    - Append-before-CAS unreachable rows are accepted for W04 because they are complete immutable refs, reader-invisible until map publication, generation-bounded, and reflected in append-only stats. Future compaction/accounting work should preserve the distinction between visible unique keys and appended rows.

## Decisions
- 2026-04-30: W03 completed in commit `265e59f`; W04 selected as the next active task.
- 2026-04-30: Plan agent concluded the original W04 implementation plan is specific enough, but W04 should execute as serial sub-stages inside the single active task. The plan also identified a required Bazel wiring scope gap.
- 2026-04-30: Verify agent returned `pass` after focused tests, `unit_tests`, `all_tests`, and uncached `all_tests`. Controller accepted append-before-CAS unreachable rows and fixed-capacity linear probing as documented W04 tradeoffs and authorized the W04 commit.

## Open Issues
- None blocking. Carry forward the documented W04 tradeoffs: append-before-CAS unreachable rows are generation-bounded and counted in append-only stats, and linear probing is functionally acceptable for W04 but leaves SwissTable-style probe optimization for future performance work.

## Next Step
- W04 completed. Create the W04 atomic commit, then controller may select W05.
