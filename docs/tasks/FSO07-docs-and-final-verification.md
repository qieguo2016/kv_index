# FSO07 Docs And Final Verification

## Metadata
- Status: completed
- Owner Role: controller
- Depends on: FSO06
- Retry Count: 0
- Last Updated: 2026-05-02 CST

## Scope
Document serving modes in README and run focused, aggregate, and full build verification.

## Plan Notes
### Implementation Boundary
- Only implement FSO07 from
  `docs/superpowers/plans/2026-05-02-full-snapshot-only-mode.md`.
- Modify `README.md` to document the two serving modes and the full-snapshot-only
  operational limitations.
- Update this task document's `Implementation Log` during coding and leave
  final pass/fail judgment to the verify agent.
- Do not modify production source, tests, `tests/BUILD.bazel`, artifact writer
  support, or earlier FSO task behavior. If final verification finds a source
  or test failure, record the failure in this task document and hand back to
  controller for a scoped fix task rather than expanding FSO07 documentation
  work silently.
- Do not create a commit in the coding phase unless the controller later
  authorizes it after verify passes.

### Dependencies And Context
- FSO01 added `ForwardIndexMode` and defaulted
  `ForwardIndexOptions::mode` to `ForwardIndexMode::kRealtimeDelta`; it also
  made `ForwardIndexMode::kFullSnapshotOnly` reject non-empty Kafka consumer
  config at construction.
- FSO02 and FSO03 added source-progress parser policy and test artifact writer
  support so missing `kSourceProgress` can be generated and accepted only when
  explicitly allowed.
- FSO04 added the full-snapshot-only async-load branch and mmap policy
  threading. In full-only mode, async load publishes full snapshots after load
  and prewarm without Kafka catch-up, realtime deltas, compact deltas, or
  internal full rebase setup.
- FSO05 added focused full-only behavior coverage for replacement semantics,
  runtime status, schema cutover, and pinned-row lifetime.
- FSO06 added default-mode regressions proving source progress remains required
  and the existing catch-up path still runs in `kRealtimeDelta` mode.
- README currently documents whole-row precedence and sequential async cutover;
  FSO07 should extend that existing surface rather than reorganizing unrelated
  project documentation.

### README Documentation Requirements
- Document that the default `kRealtimeDelta` mode reads whole rows in this
  precedence order: `realtime_delta -> compact_delta -> full_snapshot`.
- Document that `kFullSnapshotOnly` reads only full snapshots published through
  explicit async full-artifact load.
- Document that `kFullSnapshotOnly` rejects non-empty Kafka consumer config.
- Document that `kFullSnapshotOnly` does not use Kafka updates, realtime
  deltas, delta compaction, or internal full rebase.
- Document that `kFullSnapshotOnly` allows artifacts without a
  `kSourceProgress` section, while malformed present source-progress remains
  invalid.
- Document that full-only async cutover is per shard; it is not a global
  all-shard atomic version switch.

### Risk Points
- Do not imply full-snapshot-only mode is a general replacement for realtime
  mode: it has no Kafka updates, no realtime delta layer, no compact delta
  layer, no delta compaction, and no internal full rebase.
- Do not imply async load cutover is globally atomic across all shards. The
  existing serving model publishes shard states sequentially, so documentation
  must keep the per-shard cutover limitation explicit.
- Keep default-mode documentation backward compatible. Existing users should
  still see `kRealtimeDelta` as the default and its read precedence unchanged.
- Be precise about source progress: absent `kSourceProgress` is accepted only
  for full-snapshot-only artifact loading; malformed present source-progress
  still fails closed.
- Final verification may take longer than focused task tests because it includes
  aggregate Bazel suites and `bazel build //...`.

### Acceptance Criteria
- `README.md` documents both serving modes with clear default-mode and
  full-snapshot-only semantics.
- README explicitly states the default read precedence:
  `realtime_delta -> compact_delta -> full_snapshot`.
- README explicitly states full-snapshot-only reads only full snapshots loaded
  by async full-artifact load.
- README explicitly states full-snapshot-only rejects Kafka config and does not
  support Kafka updates, realtime deltas, delta compaction, or internal full
  rebase.
- README explicitly states full-snapshot-only allows artifacts without
  `kSourceProgress`, while present malformed source-progress still fails.
- README explicitly states full-only async cutover is per shard rather than a
  global all-shard atomic version switch.
- Focused tests, aggregate Bazel suites, and full build verification pass.

### Exact Verification Commands
- Focused tests:

```bash
bazel test //tests:full_snapshot_only_load_test
bazel test //tests:artifact_format_test //tests:mmap_snapshot_backing_test
bazel test //tests:async_load_test //tests:forward_index_unit_test
```

- Aggregate Bazel suites:

```bash
bazel test //tests:unit_tests //tests:integration_tests //tests:smoke_tests
```

- Full build:

```bash
bazel build //...
```

## Implementation Log
- 2026-05-02 CST coding:
  - Updated `README.md` with a new `Serving Modes` section.
  - Documented default `ForwardIndexMode::kRealtimeDelta` read precedence as
    `realtime_delta -> compact_delta -> full_snapshot`.
  - Documented `ForwardIndexMode::kFullSnapshotOnly` as full-snapshot reads
    from explicit async full-artifact loads only.
  - Documented full-only limitations: rejects non-empty Kafka consumer config,
    no Kafka updates, no realtime deltas, no delta compaction, and no internal
    full rebase.
  - Documented full-only source-progress behavior: missing `kSourceProgress`
    is allowed, but malformed present source progress still fails closed.
  - Documented full-only async cutover as per shard, not a global all-shard
    atomic version switch.
  - Files changed:
    - `README.md`
    - `docs/tasks/FSO07-docs-and-final-verification.md`
  - Commands and results:
    - `bazel test //tests:full_snapshot_only_load_test`: passed; 1/1 test
      passed from cache.
    - `bazel test //tests:artifact_format_test //tests:mmap_snapshot_backing_test`:
      passed; 2/2 tests passed from cache.
    - `bazel test //tests:async_load_test //tests:forward_index_unit_test`:
      passed; 2/2 tests passed, with 1/2 executed.
    - `bazel test //tests:unit_tests //tests:integration_tests //tests:smoke_tests`:
      passed; 29/29 tests passed, with 12/29 executed.
    - `bazel build //...`: passed; 47 targets analyzed and build completed
      successfully.
    - `git diff --check -- README.md docs/tasks/FSO07-docs-and-final-verification.md`:
      passed.
  - Risks:
    - Several Bazel link actions emitted the existing macOS/librdkafka version
      warning, but all requested verification commands exited successfully.
    - Many focused test results were served from Bazel cache; the aggregate
      command still executed 12 tests and all requested commands completed with
      exit code 0.

## Verification Log
- 2026-05-02 CST verify:
  - Reviewed `README.md` against FSO07 documentation acceptance criteria.
    The README documents default `kRealtimeDelta` read precedence as
    `realtime_delta -> compact_delta -> full_snapshot`, full-snapshot-only
    reads from explicit async full-artifact loads only, Kafka config rejection,
    unsupported Kafka/realtime/compaction/internal-rebase paths, optional
    missing `kSourceProgress`, malformed present source-progress failure, and
    per-shard rather than global all-shard atomic cutover.
  - Commands and results:
    - `git diff --check -- README.md docs/tasks/FSO07-docs-and-final-verification.md`:
      passed with exit code 0.
    - `bazel test //tests:full_snapshot_only_load_test`: passed; 1/1 test
      passed from cache.
    - `bazel test //tests:artifact_format_test //tests:mmap_snapshot_backing_test`:
      passed; 2/2 tests passed from cache.
    - `bazel test //tests:async_load_test //tests:forward_index_unit_test`:
      passed; 2/2 tests passed from cache.
    - `bazel test //tests:unit_tests //tests:integration_tests //tests:smoke_tests`:
      passed; 29/29 tests passed from cache.
    - `bazel build //...`: passed; 47 targets analyzed and build completed
      successfully.
  - Conclusion: pass.

## Decisions
- Final verification must include focused tests, aggregate Bazel suites, and `bazel build //...`.

## Open Issues
- None from FSO07 verification.

## Next Step
- Controller finalizes FSO plan.
