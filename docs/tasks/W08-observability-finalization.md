# W08 Observability, Lifetime Verification, Integration, Benchmarks, And Docs

## Metadata
- Status: completed
- Owner Role: controller
- Depends on: W07
- Retry Count: 1
- Last Updated: 2026-04-30

## Scope
Close the loop on runtime status, fail-closed paths, lifetime guarantees, end-to-end integration, benchmarks, and user-facing docs.

Files in scope:
- Modify: `include/kv_index/types.h`
- Modify: `include/kv_index/forward_index.h`
- Modify: `src/core/forward_index.cc`
- Modify: `src/core/shard_state.h`
- Modify: `src/core/realtime_delta.h`
- Create: `tests/unit/runtime_status_test.cc`
- Create: `tests/unit/lifetime_test.cc`
- Create: `tests/integration/read_precedence_integration_test.cc`
- Create: `tests/integration/async_load_cutover_integration_test.cc`
- Create: `tests/integration/schema_evolution_integration_test.cc`
- Create: `tests/benchmark/get_benchmark.cc`
- Create: `tests/benchmark/mget_benchmark.cc`
- Create: `tests/benchmark/realtime_delta_benchmark.cc`
- Create: `tests/benchmark/snapshot_decode_benchmark.cc`
- Modify: `README.md`
- Optionally create: `examples/basic_lookup.cc`

Required implementation items:
- Add runtime status structs for shard generation, schema version, artifact id, delta stats, accessor mismatch count, and last error.
- Harden cancellation, checksum failure, schema failure, and cutover-failure paths so serving shards stay unchanged on error.
- Verify lifetime pinning for old full, old compact, and old realtime rows after cutover.
- Add end-to-end integration coverage for read precedence, cross-shard `MGet`, async load windows, schema evolution, compaction, and rebase.
- Add focused non-blocking benchmarks for `Get`, `MGet`, snapshot decode, and realtime update cost.
- Update `README.md` and optional examples only after the implementation is actually present.
- Run `bazel test //tests:all_tests` and `bazel build //...`.
- Commit with an observability/docs/finalization message only after verify passes and controller requests it.

Exit criteria:
- Runtime status is sufficient to debug accessor mismatch, load failure, and cutover progress.
- Generation lifetime is proven by tests, not only by design intent.
- Public docs no longer describe implemented functionality as missing.

## Plan Notes
Planning conclusion:
- W08 is the final correctness/docs closure task, not a new subsystem. Keep it to small observability/status snapshots, fail-closed regressions, lifetime/integration proof, non-blocking benchmarks, and README/example updates. Do not introduce a metrics exporter, background scheduler, public manual publish API, production artifact builder, remote artifact downloader, live Kafka test, or compaction/rebase orchestrator.

Implementation boundaries:
- In scope from the original W08 list: `include/kv_index/types.h`, `include/kv_index/forward_index.h`, `src/core/forward_index.cc`, `src/core/shard_state.h`, `src/core/realtime_delta.h`, new focused unit/integration/benchmark tests, and `README.md`; optionally `examples/basic_lookup.cc`.
- The W08 file list is likely insufficient for wiring and implementation details. The coding agent may also modify `src/core/shard_state.cc`, `src/core/realtime_delta.cc`, `src/core/async_load.{h,cc}`, `src/core/compaction.{h,cc}`, `src/core/full_rebase.{h,cc}`, root `BUILD.bazel`, and `tests/BUILD.bazel` only as needed for status propagation, fail-closed regressions, benchmark/example targets, and test wiring.
- Out of scope: `MODULE.bazel`, `docs/tasks/controller.md`, non-W08 task docs, source behavior unrelated to W08 acceptance, broad performance rewrites, delete/tombstone semantics, public compaction/rebase APIs, and commits unless the controller explicitly requests one.

Dependency assumptions from W00-W07:
- W00-W03 already cover Bazel/public-internal target split, `Status`/`StatusOr`, deterministic sharding, pinned `ShardState` reads, `Get`, and `MGet` order/duplicate behavior.
- W01 already makes accessor mismatch observable through `Row` accessors; W08 should add a counter/status hook for mismatches only if there is a narrow, testable place to count failed public accessor reads. Do not convert accessor errors into field absence.
- W02 already provides the shared immutable snapshot lookup/decode path for full and compact snapshots and row lifetime through shared backing ownership.
- W04 already provides realtime source-position ordering, reserved-slot miss behavior, lock-free hash-map reads, and `RealtimeDeltaStats`.
- W05 already keeps Kafka broker-free tests and commit-after-local-publish semantics through fake consumer/update coordinator tests.
- W06 already provides local/file artifact validation, mmap full snapshot loading, prewarm, async load worker ownership, `LoadState` progress, catch-up, cancellation, and fail-closed load paths. W08 should add regressions only where final acceptance is not yet explicitly proven.
- W07 already provides compaction/rebase builders, sealed-row scanning, generation routing reuse, and the full-rebase non-empty-previous-realtime fail-closed guard. W08 should prove lifetime and integration across these workflows rather than redesign them.

Final acceptance checklist triage:
- Already substantially covered by W00-W07 tests: default `shard_count = 128`, explicit hash version, non-power-of-two rejection, pinned shard reads, `MGet` order/duplicates, row backing lifetime basics, immutable snapshot sharing, realtime miss for reserved slots, source-position staleness, compact overlay semantics, full-rebase compact-over-full semantics, external async schema/layout cutover, cutover prewarm/safe-position guards, Kafka commit semantics, no live Kafka dependency, `librdkafka` encapsulation, and no CMake/Makefile.
- Needs W08 status/tests/docs closure: runtime status fields for generation/schema/artifact/delta/cutover/error diagnostics, explicit fail-closed regressions for cancellation/checksum/schema/cutover failure leaving serving shards unchanged, old full/compact/realtime row pinning after cutover, end-to-end read precedence/cross-shard `MGet`/async-load window/schema evolution/compaction/rebase coverage, benchmark target wiring, README removal of stale bootstrap wording, and final `bazel test`/`bazel build` verification.

Runtime status semantics:
- Add a minimal public snapshot model in `include/kv_index/types.h`, for example `ShardRuntimeStatus` plus any nested compact structs needed. Keep it plain value types only: `shard_id`, active `generation`, `schema_version`, `layout_fingerprint` if already available, `artifact_id` for externally loaded full snapshots when known, presence flags for realtime/compact/full layers, `RealtimeDeltaStats`, `accessor_mismatch_count`, `last_error`, and optional per-shard load/cutover phase.
- Prefer extending `LoadState` for load/cutover progress rather than adding a second load API. Existing fields already cover id/code/terminal/artifact uri/id/source progress/shard progress/last error; W08 may add per-shard generation/schema/artifact/status detail if required by tests.
- If a public `ForwardIndex` status API is added, keep it read-only and synchronous, for example `RuntimeStatus GetRuntimeStatus() const` or `std::vector<ShardRuntimeStatus> GetShardRuntimeStatus() const`. It should load each shard once and summarize current state without retaining pins in the returned object.
- `ShardState` should own enough immutable metadata to report status consistently: generation, schema version/layout fingerprint derived from the top available layout, artifact id when the full snapshot came from an artifact, delta stats from realtime, and last internal error only when an operation records one. Avoid global registries and async metric streams.
- Accessor mismatch count is a debug counter, not a replacement for `StatusOr`/throwing behavior. If counting cannot be wired without changing `Row` semantics or adding shared mutable state to every row, record the limitation in `Open Issues` and keep mismatch observability through existing accessor error tests.

Fail-closed hardening targets:
- Cancellation: cancelling a running external load before final publish must leave every previously serving shard generation and row value unchanged; terminal state must be `kCancelled` or a clearly cancelled failure with `last_error`.
- Checksum failure: corrupted artifact section/checksum must fail before any shard publish; previous serving rows remain visible and `cutover_shard_count == 0`.
- Schema/layout failure: incompatible row metadata, unsupported schema/layout, or decode/prewarm validation failure must fail before publish for that shard. If any shard fails in v1, the implementation should leave the whole serving set unchanged unless an already-existing per-shard cutover contract explicitly allows partial success; document the chosen behavior in tests/status.
- Cutover failure: inject a publish failure through an internal/test callback or shard-directory boundary if available. The failed shard must remain on the old generation; status must show failure and the last error. If earlier shards can already have cut over before a later publish failure, the status/test must make that partial-cutover behavior explicit and prove no corrupted/null shard is served.

Lifetime verification:
- Add `tests/unit/lifetime_test.cc` for pinned row lifetime after replacement of each layer type. Pin a row, publish/finish a successor state that no longer owns that layer directly, then read the pinned row after cutover.
- Cover old full snapshot row pinning after external full replacement.
- Cover old compact snapshot row pinning after compaction/rebase replacement.
- Cover old realtime row pinning after compaction or full rebase cutover. The old `RealtimeDeltaAtomicTable` and encoded row must stay alive through the `Row` pin even if the active shard now points to successor realtime/full/compact layers.
- Use string/list fields in at least one lifetime case so the test proves payload/view lifetime, not only fixed-width row-slot bytes.

End-to-end integration coverage:
- Add or extend integration tests for read precedence: realtime overrides compact, compact overrides full, and there is no field-level merging.
- Add cross-shard `MGet` integration with multiple shards, duplicate keys, missing keys, and keys served from different layers while preserving input order.
- Add async-load window integration proving reads before cutover return old/missing data, reads during catch-up remain old, and reads after success return the new artifact/rebuild generation.
- Add schema evolution integration proving old rows decode with the old layout before cutover and new artifact/rebuild rows decode with the new schema after cutover.
- Add compaction/rebase integration coverage only where current W07 integration does not already prove the final checklist: compaction sealed realtime over compact, rebase compact over full, and updates routed to successor realtime remain visible after cutover.
- Prefer extending existing integration tests if that avoids redundant setup; create the named W08 integration files only when the scenario is clearer as a separate target.

Benchmark scope:
- Add focused benchmark binaries for `Get`, cross-shard `MGet`, snapshot decode/enumeration, and realtime publish/update cost. They should be deterministic microbenchmarks using existing in-repo builders and should print simple timing/throughput results.
- Benchmarks are non-blocking correctness aids. Wire them as `cc_binary` targets or tagged/manual tests that are included in `bazel build //...`, not in `//tests:all_tests`, `//tests:unit_tests`, `//tests:smoke_tests`, or `//tests:integration_tests`.
- Do not tune implementation based on benchmark results in W08 unless a result exposes a correctness bug or catastrophic build/runtime issue. The known mmap enumeration inefficiency may be documented as a benchmark-observed follow-up risk.

README/docs/example boundaries:
- Update `README.md` only after status/tests are implemented. Remove stale bootstrap wording that says snapshot loading, realtime publication, Kafka consumption, and cutover are missing.
- Document the supported v1 surface: Bazel commands, shard-count/hash requirements, `Get`/`MGet`, `LoadAsync`/`GetLoadState`/`CancelLoad`, local/file artifact support, no live broker requirement for tests, and final limitations.
- Optional `examples/basic_lookup.cc` should compile through Bazel if added. Keep it minimal: construct an index, show empty miss or test-supported artifact load if practical. Do not add a fake production data-source abstraction for the example.

Minimal executable breakdown for coding agent:
1. Baseline and scope guard:
   - Run `git status --short` and inspect existing edits without reverting them.
   - Read this W08 task note, W06/W07 task notes, the W08 section and final checklist in `docs/superpowers/plans/2026-04-30-in-memory-forward-index-detailed-implementation.md`, and current `types.h`, `forward_index.h`, `forward_index.cc`, `shard_state`, `realtime_delta`, `async_load`, `compaction`, `full_rebase`, tests, Bazel files, and `README.md`.
2. Status API TDD:
   - Add failing `tests/unit/runtime_status_test.cc` for empty shards, a shard with full/compact/realtime layers, async load running/succeeded/failed status, delta stats, generation/schema/artifact id, and last error.
   - Implement the smallest status structs/API and internal metadata needed to pass. Keep returned status as a snapshot value with no ownership of live rows.
   - Wire `//tests:runtime_status_test` into `tests/BUILD.bazel` and `unit_tests`/`all_tests`.
3. Fail-closed regressions:
   - Extend async-load/status tests or add focused W08 tests for cancellation, corrupted checksum, schema/layout failure, and cutover publish failure.
   - Verify old serving rows/generations remain visible after every failure. If v1 permits partial per-shard cutover on late publish failure, assert exactly which shards changed and which did not.
4. Lifetime TDD:
   - Add `tests/unit/lifetime_test.cc` with old full, old compact, and old realtime pinned-row cases after successor publish/cutover.
   - Include payload-backed strings/lists in at least one case.
   - Wire `//tests:lifetime_test` into `unit_tests`/`all_tests`.
5. Integration closure:
   - Add or extend W08 integration tests for read precedence, cross-shard `MGet`, async-load windows, schema evolution, compaction, and rebase.
   - Wire new targets into `integration_tests` and `all_tests`.
6. Benchmark wiring:
   - Add `tests/benchmark/get_benchmark.cc`, `mget_benchmark.cc`, `realtime_delta_benchmark.cc`, and `snapshot_decode_benchmark.cc` or combine only if Bazel target names remain clear.
   - Wire benchmark targets so `bazel build //...` builds them. Keep them out of aggregate test suites unless explicitly tagged/manual and not run by default.
7. README and optional example:
   - Update `README.md` to match implemented functionality and limitations.
   - Add `examples/basic_lookup.cc` only if it stays small and buildable without inventing new APIs. Wire it into root `BUILD.bazel` if created.
8. Final verification:
   - Run focused tests as slices are implemented: `bazel test //tests:runtime_status_test`, `bazel test //tests:lifetime_test`, and each new integration target.
   - Run final required gates: `bazel test //tests:unit_tests`, `bazel test //tests:smoke_tests`, `bazel test //tests:integration_tests`, optionally `bazel test //tests:all_tests`, and `bazel build //...`.
   - Update this task file's `Implementation Log` and `Verification Log` with exact commands and outcomes. Do not commit unless the controller requests it.

Acceptance criteria:
- Runtime status can answer: which generation/schema/artifact each shard is serving, whether realtime/compact/full layers exist, current delta stats, load/cutover progress, last load/cutover error, and whether accessor mismatches remain observable.
- Status additions are bounded to debug snapshots; no broad metrics/export subsystem is introduced.
- Cancellation, checksum failure, schema/layout failure, and cutover failure are fail-closed with serving shards unchanged unless an explicitly tested partial-cutover rule applies.
- Old full, compact, and realtime rows remain valid after cutover while the caller holds `Row` pins.
- End-to-end tests cover read precedence, cross-shard `MGet`, async-load windows, schema evolution, compaction, and rebase without requiring a live Kafka broker.
- Benchmarks are buildable, deterministic, and non-blocking; they are not part of default test suites.
- README no longer describes implemented functionality as missing and accurately documents v1 limits.
- Final project verification passes: `bazel test //tests:unit_tests`, `bazel test //tests:smoke_tests`, `bazel test //tests:integration_tests`, preferably `bazel test //tests:all_tests`, and `bazel build //...`.

## Implementation Log
- 2026-04-30 RED: Added `tests/unit/runtime_status_test.cc` and
  `//tests:runtime_status_test`; `bazel test //tests:runtime_status_test`
  failed to compile because `ForwardIndex::GetRuntimeStatus()` did not exist.
- 2026-04-30 GREEN: Added minimal public runtime snapshot structs in
  `include/kv_index/types.h`, `ForwardIndex::GetRuntimeStatus()`, and
  `ShardState::GetRuntimeStatus()`. Runtime status now reports shard
  generation, schema version, layout fingerprint, artifact id for mmap full
  snapshots, layer presence, row counts, realtime delta stats, load states, and
  last error. `bazel test //tests:runtime_status_test` passed.
- 2026-04-30 Added W08 fail-closed regressions in
  `tests/unit/runtime_status_test.cc`: checksum failure, row-slot layout
  validation failure, and injected cutover publish failure. Checksum/layout
  failures leave serving rows unchanged with `cutover_shard_count == 0`.
  Injected publish failure documents the current sequential partial-cutover
  rule: earlier published shards can move to the new generation, failed/later
  shards remain on the old generation, and load state reports
  `cutover_shard_count` plus `last_error`.
- 2026-04-30 Added `tests/unit/lifetime_test.cc` for pinned old full, compact,
  and realtime rows after successor cutover. The full/compact/realtime cases
  include string and int-list payload-backed fields. The tests passed without
  production lifetime changes.
- 2026-04-30 Added W08 integration targets:
  `read_precedence_integration_test`, `async_load_cutover_integration_test`,
  and `schema_evolution_integration_test`. They cover whole-row read
  precedence, cross-shard `MGet` order/duplicates/missing/layers, async-load
  old/during/after cutover windows, and schema-version replacement through
  external artifact load. Existing `compaction_rebase_integration_test`
  continues to cover compaction/rebase closure.
- 2026-04-30 Added build-only benchmark binaries:
  `get_benchmark`, `mget_benchmark`, `realtime_delta_benchmark`, and
  `snapshot_decode_benchmark`. They are not included in aggregate test suites.
- 2026-04-30 Updated `README.md` to remove stale bootstrap wording and document
  implemented v1 behavior, status APIs, benchmark targets, and limitations.
- 2026-04-30 fix retry 1 RED/GREEN:
  `LayoutValidationFailureLeavesServingShardUnchanged` first installed the
  no-op catch-up factory while keeping the old `row_slot.push_back` corruption;
  `bazel test //tests:runtime_status_test` failed at
  `state.code == LoadStateCode::kFailed`, proving the old corruption was
  normalized by the test artifact writer and did not exercise artifact
  validation. The GREEN rewrites the artifact file after `WriteTestArtifact`
  to make shard 1's row-slot section misaligned while updating section offsets
  and checksum, so `MmapSnapshotBacking::LoadShard` rejects it as
  `mmap row slot section is misaligned`. The test now asserts
  `state.code == kFailed`, `cutover_shard_count == 0`, a row-slot/layout/schema
  failure class in `last_error`, old row values, and old generation still
  active through runtime status and `ForwardIndexTestPeer`.
- 2026-04-30 fix retry 1 RED/GREEN:
  `CancellationBeforeCutoverPreventsPublish` first changed the assertion from
  "new artifact key missing" to "old row still served"; `bazel test
  //tests:async_load_test` failed at `row.has_value()`, proving the existing
  coverage had no old serving baseline. The GREEN publishes an old full
  snapshot generation before `LoadAsync`, cancels during catch-up before
  cutover, and asserts terminal cancelled/failed state with cancel semantics,
  `cutover_shard_count == 0`, old row value still served, and old generation
  still active through runtime status and `ForwardIndexTestPeer`.
- 2026-04-30 fix retry 1 also strengthened
  `CutoverPublishFailureReportsExplicitPartialCutover`: after the injected
  publish failure, the test now proves the shard that already cut over reads
  the new artifact value and the failed shard still reads the old serving value.
- Changed files: `README.md`, `include/kv_index/forward_index.h`,
  `include/kv_index/types.h`, `src/core/forward_index.cc`,
  `src/core/realtime_delta.{h,cc}`, `src/core/shard_state.{h,cc}`,
  `tests/BUILD.bazel`, `tests/unit/async_load_test.cc`,
  `tests/unit/runtime_status_test.cc`, `tests/unit/lifetime_test.cc`,
  `tests/integration/read_precedence_integration_test.cc`,
  `tests/integration/async_load_cutover_integration_test.cc`,
  `tests/integration/schema_evolution_integration_test.cc`,
  `tests/benchmark/get_benchmark.cc`, `tests/benchmark/mget_benchmark.cc`,
  `tests/benchmark/realtime_delta_benchmark.cc`, and
  `tests/benchmark/snapshot_decode_benchmark.cc`.
- Known limitations: aggregate accessor mismatch counting is not wired because
  doing so would require shared mutable state in `Row` ownership/accessor
  paths; accessor mismatch remains observable through existing accessor errors.
  Async load cutover remains sequential and can partially cut over if a later
  publish fails; W08 tests and README document the explicit status contract.

## Verification Log
- 2026-04-30 independent verify agent: `git status --short` showed existing
  uncommitted W08 edits plus pre-existing edits in `docs/tasks/W07-compaction-full-rebase.md`
  and `docs/tasks/controller.md`; no files were reverted.
- 2026-04-30 independent verify agent: `git diff --check` passed.
- 2026-04-30 independent verify agent: inspected the W08 plan/final acceptance
  checklist, targeted tracked diffs, untracked W08 tests/benchmarks, README,
  and relevant W06/W07 async-load/compaction/rebase code.
- 2026-04-30 independent verify agent: `bazel test //tests:runtime_status_test`
  passed, cached.
- 2026-04-30 independent verify agent: `bazel test //tests:lifetime_test`
  passed, cached.
- 2026-04-30 independent verify agent: `bazel test //tests:read_precedence_integration_test`
  passed, cached.
- 2026-04-30 independent verify agent: `bazel test //tests:async_load_cutover_integration_test`
  passed, cached.
- 2026-04-30 independent verify agent: `bazel test //tests:schema_evolution_integration_test`
  passed, cached.
- 2026-04-30 independent verify agent: `bazel test //tests:unit_tests`
  passed, 22/22 tests, cached.
- 2026-04-30 independent verify agent: `bazel test //tests:smoke_tests`
  passed, 1/1 test, cached.
- 2026-04-30 independent verify agent: `bazel test //tests:integration_tests`
  passed, 5/5 tests, cached.
- 2026-04-30 independent verify agent: `bazel test //tests:all_tests`
  passed, 28/28 tests, cached.
- 2026-04-30 independent verify agent: `bazel build //...` passed, 37
  targets.
- 2026-04-30 independent verify findings:
  - Blocking: the W08 schema/layout fail-closed regression does not actually
    prove schema/layout artifact failure. `LayoutValidationFailureLeavesServingShardUnchanged`
    appends one extra byte to an encoded row before `WriteTestArtifact`, but
    the test artifact path rebuilds through `SnapshotBuilder::AddRow`, which
    accepts longer row slots and truncates to the compiled row-slot size. The
    test can still pass because no no-op catch-up runner is installed, so
    `LoadAsync` fails before publish for an unrelated missing production Kafka
    consumer path. A fix agent should make the artifact invalid in a way
    `ParseArtifact`/`MmapSnapshotBacking::LoadShard` rejects as schema/layout
    or row-layout validation, install the no-op catch-up runner if needed, and
    assert `last_error` matches that failure class with `cutover_shard_count == 0`
    and old serving rows/generations unchanged.
  - Blocking: cancellation fail-closed coverage does not prove an existing
    serving shard remains unchanged. The existing cancellation regression starts
    from an empty index and only asserts the new artifact key remains missing.
    W08 acceptance requires cancellation before final publish to leave
    previously serving shards/rows/generations unchanged. A fix agent should
    publish old full rows first, cancel during catch-up before cutover, then
    assert terminal cancelled/failed state, `cutover_shard_count == 0`, old row
    values still served, and old shard generations still active.
  - Non-blocking risk: the explicit partial cutover test documents
    `cutover_shard_count` and generation split, and README documents the
    partial rule, but the test would be stronger if it read old/new row values
    through the same serving abstraction after the injected publish failure.
  - Non-blocking risk: aggregate `accessor_mismatch_count` is documented as
    always zero and accessor mismatches remain observable through accessor
    errors, which is acceptable for v1 as long as README/task documentation is
    preserved.
- 2026-04-30 independent verify risk assessment: runtime status, lifetime,
  integration, benchmarks, README scope, no live Kafka tests, and no broad
  metrics/public manual compaction or rebase APIs look within W08 boundaries.
  The remaining blockers are test-evidence gaps in final fail-closed plan
  compliance, not command failures.
- 2026-04-30 independent verify conclusion: fail.
- 2026-04-30 controller: moved W08 to fix retry 1 for focused fail-closed
  evidence repair. The fix scope is limited to proving true schema/layout
  artifact rejection and cancellation with pre-existing serving rows/generations
  unchanged, plus strengthening the existing injected publish-failure assertion
  if feasible without broadening W08 behavior.
- 2026-04-30 `bazel test //tests:runtime_status_test` RED: failed to compile
  with missing `ForwardIndex::GetRuntimeStatus()`.
- 2026-04-30 `bazel test //tests:runtime_status_test` GREEN: passed.
- 2026-04-30 `bazel test //tests:lifetime_test`: passed.
- 2026-04-30 `bazel test //tests:read_precedence_integration_test`: passed.
- 2026-04-30 `bazel test //tests:async_load_cutover_integration_test`:
  passed.
- 2026-04-30 `bazel test //tests:schema_evolution_integration_test`: passed.
- 2026-04-30 `bazel build //tests:get_benchmark //tests:mget_benchmark //tests:realtime_delta_benchmark //tests:snapshot_decode_benchmark`:
  passed.
- 2026-04-30 `bazel test //tests:unit_tests`: passed, 22/22 tests.
- 2026-04-30 `bazel test //tests:smoke_tests`: passed, 1/1 test.
- 2026-04-30 `bazel test //tests:integration_tests`: passed, 5/5 tests.
- 2026-04-30 `bazel test //tests:all_tests`: passed, 28/28 tests.
- 2026-04-30 `bazel build //...`: passed, 37 targets.
- 2026-04-30 fix retry 1 RED:
  `bazel test //tests:runtime_status_test` failed at
  `tests/unit/runtime_status_test.cc:371` on
  `state.code == LoadStateCode::kFailed` after installing the no-op catch-up
  factory against the old row-slot `push_back` corruption. This confirmed the
  previous schema/layout regression did not create a real artifact validation
  failure.
- 2026-04-30 fix retry 1 GREEN:
  `bazel test //tests:runtime_status_test` passed after post-write artifact
  mutation made the row-slot section genuinely misaligned and the test asserted
  failure class, zero cutover, old row value, and old generation.
- 2026-04-30 fix retry 1 RED:
  `bazel test //tests:async_load_test` failed at
  `tests/unit/async_load_test.cc:443` on `row.has_value()` after changing the
  cancellation check to require an old served row. This confirmed the previous
  cancellation regression started from an empty index.
- 2026-04-30 fix retry 1 GREEN:
  `bazel test //tests:async_load_test` passed after publishing an old full
  snapshot generation before cancellation and asserting old row/generation
  preservation.
- 2026-04-30 fix retry 1 verification:
  `bazel test //tests:runtime_status_test` passed.
- 2026-04-30 fix retry 1 verification:
  `bazel test //tests:lifetime_test` passed, cached.
- 2026-04-30 fix retry 1 verification:
  `bazel test //tests:read_precedence_integration_test` passed, cached.
- 2026-04-30 fix retry 1 verification:
  `bazel test //tests:async_load_cutover_integration_test` passed, cached.
- 2026-04-30 fix retry 1 verification:
  `bazel test //tests:schema_evolution_integration_test` passed, cached.
- 2026-04-30 fix retry 1 verification:
  `bazel test //tests:unit_tests` passed, 22/22 tests.
- 2026-04-30 fix retry 1 verification:
  `bazel test //tests:smoke_tests` passed, 1/1 test, cached.
- 2026-04-30 fix retry 1 verification:
  `bazel test //tests:integration_tests` passed, 5/5 tests, cached.
- 2026-04-30 fix retry 1 verification:
  `bazel test //tests:all_tests` passed, 28/28 tests, cached.
- 2026-04-30 fix retry 1 verification:
  `bazel build //...` passed, 37 targets.
- 2026-04-30 verify retry 1 agent: `git status --short` showed the expected
  uncommitted W08 source/test/README/benchmark edits, existing task-ledger
  edits in `docs/tasks/W08-observability-finalization.md`,
  `docs/tasks/controller.md`, and `docs/tasks/W07-compaction-full-rebase.md`,
  and the new W08 test/benchmark files; no files were reverted, no worktree was
  created, and no commit was made.
- 2026-04-30 verify retry 1 agent: `git diff --check` passed.
- 2026-04-30 verify retry 1 agent: independently re-read the controller W08
  dispatch record, W08 task notes, W08/final acceptance checklist, W08 source
  diffs, W08 unit/integration/benchmark tests, and the relevant async-load,
  artifact, mmap, snapshot, row lifetime, compaction, and rebase code.
- 2026-04-30 verify retry 1 finding: prior blocking schema/layout evidence gap
  is resolved. `LayoutValidationFailureLeavesServingShardUnchanged` now writes
  a valid artifact first, mutates shard 1's serialized row-slot section after
  writing, updates section offsets and the row-slot checksum, installs a no-op
  catch-up factory, and fails through mmap row-slot/layout validation rather
  than artifact-writer normalization or missing Kafka catch-up setup. The test
  asserts `kFailed`, `cutover_shard_count == 0`, layout-class `last_error`,
  old row values, and the old shard generation still active.
- 2026-04-30 verify retry 1 finding: prior blocking cancellation evidence gap
  is resolved. `CancellationBeforeCutoverPreventsPublish` now publishes an old
  full snapshot generation before `LoadAsync`, cancels while catch-up is running
  and before cutover, and asserts cancelled/failed terminal state with cancel
  semantics, `cutover_shard_count == 0`, old row value still served, and old
  generation still active through runtime status and `ForwardIndexTestPeer`.
- 2026-04-30 verify retry 1 acceptance review: runtime status exposes shard
  generation, schema/layout, mmap artifact id, layer presence, row counts,
  realtime delta stats, load states, and last load/cutover errors. The
  fail-closed checksum/schema/cancel/cutover paths leave old serving state
  unchanged or explicitly document the sequential partial-cutover rule for late
  publish failure; the strengthened cutover test reads the new value on the
  cutover shard and the old value on the failed shard.
- 2026-04-30 verify retry 1 acceptance review: lifetime tests cover pinned old
  full, compact, and realtime rows with string/list payload views after
  successor publication; integration coverage covers whole-row read precedence,
  cross-shard `MGet` order/duplicates/missing/layers, async-load old/during/after
  windows, schema evolution with old pinned rows and new schema status, and the
  existing compaction/rebase closure path.
- 2026-04-30 verify retry 1 scope review: benchmarks are build-only
  `cc_binary` targets and are not in aggregate test suites; README no longer
  has stale bootstrap/missing wording and documents v1 limits. No metrics
  exporter, public manual compaction/rebase API, live Kafka test dependency,
  `MODULE.bazel` change, worktree, or commit was introduced by this verify
  pass.
- 2026-04-30 verify retry 1 command: `bazel test //tests:runtime_status_test`
  passed, cached, 1/1 test.
- 2026-04-30 verify retry 1 command: `bazel test //tests:async_load_test`
  passed, cached, 1/1 test.
- 2026-04-30 verify retry 1 command: `bazel test //tests:lifetime_test`
  passed, cached, 1/1 test.
- 2026-04-30 verify retry 1 command:
  `bazel test //tests:read_precedence_integration_test` passed, cached, 1/1
  test.
- 2026-04-30 verify retry 1 command:
  `bazel test //tests:async_load_cutover_integration_test` passed, cached, 1/1
  test.
- 2026-04-30 verify retry 1 command:
  `bazel test //tests:schema_evolution_integration_test` passed, cached, 1/1
  test.
- 2026-04-30 verify retry 1 command: `bazel test //tests:unit_tests` passed,
  cached, 22/22 tests.
- 2026-04-30 verify retry 1 command: `bazel test //tests:smoke_tests` passed,
  cached, 1/1 test.
- 2026-04-30 verify retry 1 command:
  `bazel test //tests:integration_tests` passed, cached, 5/5 tests.
- 2026-04-30 verify retry 1 command: `bazel test //tests:all_tests` passed,
  cached, 28/28 tests.
- 2026-04-30 verify retry 1 command: `bazel build //...` passed, 37 targets.
- 2026-04-30 verify retry 1 risk assessment: aggregate
  `accessor_mismatch_count` remains documented as always zero because real
  counting would require shared mutable state in `Row`/accessor ownership;
  accessor mismatches remain observable through accessor errors. Runtime
  status' top-level `last_error` is a debug summary while per-load `last_error`
  carries the actionable failure detail. Async load still permits the
  documented sequential partial-cutover rule on late publish failure.
- 2026-04-30 verify retry 1 final conclusion: pass.

## Decisions
- 2026-04-30: W07 completed in commit `b3ff052`; W08 selected as the next active task.
- 2026-04-30: W08 planning is ready for implementation. Keep runtime status as value snapshots on existing `ForwardIndex`/`LoadState`/`ShardState` concepts rather than a metrics system.
- 2026-04-30: Controller authorizes minimal root `BUILD.bazel` and `tests/BUILD.bazel` edits for W08 test/benchmark/example wiring if needed.
- 2026-04-30: Benchmarks are build targets only for W08 acceptance; they should not gate `//tests:all_tests`.
- 2026-04-30: Controller authorizes minimal W08 implementation edits in `src/core/shard_state.cc`, `src/core/realtime_delta.cc`, `src/core/async_load.{h,cc}`, `src/core/compaction.{h,cc}`, and `src/core/full_rebase.{h,cc}` only where directly needed for runtime status, fail-closed regressions, lifetime tests, and final integration. Do not modify `MODULE.bazel`.
- 2026-04-30: W08 verify failed on test-evidence gaps, not Bazel gate
  failures. Controller requires fix retry 1 before another independent verify.
- 2026-04-30: W08 verify retry 1 passed after focused and aggregate Bazel
  gates. Controller marked W08 completed and authorized a commit-only coding
  agent to create the W08 atomic commit with exactly one Codex trailer.

## Open Issues
- Resolved in fix retry 1: schema/layout fail-closed coverage now rejects a
  post-write corrupted row-slot section through mmap row-layout validation,
  with no-op catch-up installed and old serving row/generation assertions.
- Resolved in fix retry 1: cancellation fail-closed coverage now starts from
  an existing full snapshot row/generation and proves cancellation before
  cutover preserves the old served value and active generation.
- V1 accessor mismatch counting may not be practical without changing `Row` lifetime/ownership or accessor semantics. If so, keep existing accessor mismatch errors as the observable contract and document why no aggregate mismatch counter was added.
- Async load currently performs per-shard publish sequentially. W08 documents
  the explicit partial-cutover fail-closed rule; fix retry 1 strengthened the
  test to prove cutover shards serve new values and failed shards serve old
  values after an injected publish failure.
- Mmap-backed snapshot enumeration is known to be correctness-focused and may be inefficient. W08 benchmarks may quantify this, but broad optimization is not required for final acceptance.
- Live Kafka broker behavior remains outside automated tests; keep broker-free fake seams as the acceptance boundary.

## Next Step
- Completed. Create the W08 atomic commit, then run final full-project
  verification from the controller workflow.
