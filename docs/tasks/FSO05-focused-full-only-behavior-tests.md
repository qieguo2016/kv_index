# FSO05 Focused Full-Only Behavior Tests

## Metadata
- Status: completed
- Owner Role: controller
- Depends on: FSO04
- Retry Count: 0
- Last Updated: 2026-05-02 CST

## Scope
Add focused end-to-end full-snapshot-only tests for replacement semantics, runtime status, schema cutover, and pinned-row lifetime.

## Plan Notes

### Implementation Boundary

- Primary edit target: `tests/unit/full_snapshot_only_load_test.cc`.
- Keep the test target and source files added by FSO04; do not add new Bazel targets unless the existing FSO04 target is unexpectedly missing.
- Add behavior tests for `ForwardIndexMode::kFullSnapshotOnly` only:
  - full snapshot replacement semantics;
  - runtime status after full-only load;
  - schema cutover across full artifacts;
  - pinned row lifetime across full-only replacement.
- Use `TestArtifactSpec::include_source_progress_section = false` for full-only artifacts where the missing source-progress behavior matters.
- If a test reveals a real production bug, coding may make the minimal source fix needed for that FSO05 behavior. Avoid broad async-load, default-mode, README, or FSO06 regression changes.
- Do not implement FSO06 default realtime-mode regression tests.
- Do not implement FSO07 README or aggregate final verification work.

### Dependencies And Context

- FSO01 added `ForwardIndexMode` and constructor validation for full-only mode.
- FSO02 added optional artifact source-progress parsing policy.
- FSO03 added `TestArtifactSpec::include_source_progress_section`.
- FSO04 added `tests/unit/full_snapshot_only_load_test.cc`, `//tests:full_snapshot_only_load_test`, and the first full-only async-load branch test.
- Relevant patterns:
  - `tests/unit/full_snapshot_only_load_test.cc` already has full-only options, artifact writing, catch-up factory guard, and `WaitForTerminalState`.
  - `tests/integration/schema_evolution_integration_test.cc` shows old-row pinning during schema cutover.
  - `tests/unit/runtime_status_test.cc` shows `GetRuntimeStatus()` layer assertions.
  - `tests/unit/lifetime_test.cc` shows pinned `Row` assertions after replacement.

### Risk Points

- Full-only replacement should replace the entire per-shard full snapshot. A key absent from the new artifact must disappear after cutover.
- Runtime status must report only the full snapshot layer for full-only mode: no realtime delta and no compact delta.
- Mmap-backed full snapshots should surface their artifact id in shard runtime status.
- Pinned rows must keep the old backing alive after a newer mmap full snapshot replaces the shard.
- Schema cutover should verify both the new row layout for fresh reads and continued readability of the pinned old row.
- Keep artifact shard ids aligned with `options.shard_count`, `hash_seed`, and `hash_version`; use `options.shard_count = 1` unless testing multi-shard behavior is necessary.
- Avoid relying on Kafka catch-up factories except as a guard that full-only mode must not create one.

### Acceptance Criteria

- `//tests:full_snapshot_only_load_test` contains focused coverage for:
  - first artifact load succeeds and serves old data;
  - second artifact load replaces the full snapshot, removes missing old keys, and serves new keys;
  - runtime status reports `has_full_snapshot == true`, `has_realtime_delta == false`, `has_compact_delta == false`, correct `full_row_count`, expected schema metadata, and mmap artifact id;
  - schema version/layout cutover is visible to new reads;
  - a row pinned before cutover remains readable with old values after the new artifact is loaded.
- All new FSO05 tests run in full-only mode and do not require Kafka config or source-progress sections unless a specific assertion requires otherwise.
- The focused test target passes with:

```bash
bazel test //tests:full_snapshot_only_load_test
```

### Focused Tests

- Add `FullOnlyLoadReplacesPreviousFullSnapshot`:
  - configure full-only `ForwardIndex` with `shard_count = 1`;
  - write artifact A with `old_key` and load it;
  - verify `old_key` hits;
  - write artifact B with a different `artifact_id`, omitting `old_key` and containing `new_key`;
  - load B and verify `old_key` misses while `new_key` hits.
- Add `FullOnlyRuntimeStatusReportsOnlyMmapFullSnapshot`:
  - load a full-only artifact with known row count and artifact id;
  - assert shard 0 has full snapshot only, correct row count, correct artifact id, and no realtime/compact layers.
- Add `FullOnlySchemaCutoverKeepsPinnedOldRowReadable`:
  - load old schema artifact and pin a row;
  - load new schema artifact with schema version `N + 1` and an added string field;
  - verify fresh read sees the new schema/value and runtime status reports the new schema version;
  - verify pinned old row still returns the old field value and does not expose the newly added field.

## Implementation Log
- Added focused full-only behavior coverage in
  `tests/unit/full_snapshot_only_load_test.cc`:
  - `FullOnlyLoadReplacesPreviousFullSnapshot` loads artifact A with
    `old_key`, then artifact B without `old_key` and with `new_key`; the old
    key misses after B and the new key hits.
  - `FullOnlyRuntimeStatusReportsOnlyMmapFullSnapshot` asserts the loaded shard
    reports only an mmap full snapshot, correct `full_row_count`, artifact id,
    schema version, and layout fingerprint.
  - `FullOnlySchemaCutoverKeepsPinnedOldRowReadable` loads old schema, pins an
    old row, loads schema version N+1 with an added string field, verifies fresh
    reads/status use the new schema, and verifies the pinned old row remains
    readable without the new field.
- Reused full-only mode with `include_source_progress_section = false` for all
  new artifact loads. No Kafka config was used.
- Commands/results:
  - `bazel test //tests:full_snapshot_only_load_test` after replacement test:
    passed immediately; no production change needed for replacement semantics.
  - `bazel test //tests:full_snapshot_only_load_test` after runtime-status test:
    passed immediately; no production change needed for runtime status.
  - `bazel test //tests:full_snapshot_only_load_test` after schema/lifetime test:
    passed; no production change needed for schema cutover or pinned lifetime.
  - `git diff --check -- tests/unit/full_snapshot_only_load_test.cc
    docs/tasks/FSO05-focused-full-only-behavior-tests.md`: passed with no
    output.
  - `bazel test //tests:full_snapshot_only_load_test`: cached pass after final
    edits.
  - `bazel test --cache_test_results=no //tests:full_snapshot_only_load_test`:
    passed, executed 1/1 test.
  - `git diff --check --no-index /dev/null
    tests/unit/full_snapshot_only_load_test.cc`: no whitespace output; exit code
    1 is expected for no-index file differences.
  - `git diff --check --no-index /dev/null
    docs/tasks/FSO05-focused-full-only-behavior-tests.md`: no whitespace output;
    exit code 1 is expected for no-index file differences.
- Files changed:
  - `tests/unit/full_snapshot_only_load_test.cc`
  - `docs/tasks/FSO05-focused-full-only-behavior-tests.md`
- Risks:
  - Focused target only so far; aggregate/final suites remain out of scope for
    FSO05.
  - FSO06 default-mode regressions and FSO07 docs/final verification were not
    implemented.

## Verification Log
- Verified `tests/unit/full_snapshot_only_load_test.cc` directly because the
  file is currently untracked. The focused coverage matches FSO05 acceptance
  criteria:
  - `FullOnlyLoadReplacesPreviousFullSnapshot` covers first-load serving,
    second full-artifact replacement, missing old key removal, and new key
    serving.
  - `FullOnlyRuntimeStatusReportsOnlyMmapFullSnapshot` checks full-only layer
    status, row count, mmap artifact id, schema version, and layout
    fingerprint.
  - `FullOnlySchemaCutoverKeepsPinnedOldRowReadable` checks schema/layout
    cutover for fresh reads and keeps a pinned old row readable with the old
    values and without the newly added field.
- Confirmed all FSO05 tests construct `ForwardIndexMode::kFullSnapshotOnly`
  indexes and use `include_source_progress_section = false` through the shared
  full-only artifact helper.
- Confirmed no FSO06 default realtime-mode regression tests or FSO07 README /
  aggregate-final-verification scope was added in this file.
- Commands/results:
  - `git status --short`: confirmed the expected untracked
    `tests/unit/full_snapshot_only_load_test.cc` and task docs, plus prior
    staged/unstaged project changes outside this verify scope.
  - `git diff --check -- tests/unit/full_snapshot_only_load_test.cc
    docs/tasks/FSO05-focused-full-only-behavior-tests.md`: passed with no
    output before verification-log edits.
  - `git diff --check --no-index /dev/null
    tests/unit/full_snapshot_only_load_test.cc`: no whitespace output; exit
    code 1 is expected for no-index file differences.
  - `git diff --check --no-index /dev/null
    docs/tasks/FSO05-focused-full-only-behavior-tests.md`: no whitespace
    output before verification-log edits; exit code 1 is expected for no-index
    file differences.
  - `bazel test //tests:full_snapshot_only_load_test`: passed from cache,
    executed 0/1 tests.
  - `bazel test --cache_test_results=no //tests:full_snapshot_only_load_test`:
    passed, executed 1/1 test.
  - Post-log `git diff --check -- tests/unit/full_snapshot_only_load_test.cc
    docs/tasks/FSO05-focused-full-only-behavior-tests.md`: passed with no
    output.
  - Post-log `git diff --check --no-index /dev/null
    tests/unit/full_snapshot_only_load_test.cc`: no whitespace output; exit
    code 1 is expected for no-index file differences.
  - Post-log `git diff --check --no-index /dev/null
    docs/tasks/FSO05-focused-full-only-behavior-tests.md`: no whitespace
    output; exit code 1 is expected for no-index file differences.
- Conclusion: pass.

## Decisions
- Tests should use artifacts without source-progress sections where relevant.
- Keep all FSO05 behavior coverage in `tests/unit/full_snapshot_only_load_test.cc` to avoid broadening the task into integration/default-mode work.

## Open Issues
- None for FSO05 verification.

## Next Step
- Controller selects FSO06.
