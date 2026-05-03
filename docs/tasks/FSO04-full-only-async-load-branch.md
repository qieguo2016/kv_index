# FSO04 Full-Only Async Load Branch

## Metadata
- Status: completed
- Owner Role: controller
- Depends on: FSO03
- Retry Count: 0
- Last Updated: 2026-05-02 CST

## Scope
Thread source-progress policy through mmap load and add full-snapshot-only async-load publication that skips Kafka catch-up and publishes shard states containing only full snapshots.

## Plan Notes
### Implementation Boundary
- Only implement Task 4 from
  `docs/superpowers/plans/2026-05-02-full-snapshot-only-mode.md`.
- Thread the FSO02 parser policy through mmap loading:
  - add `source_progress_policy` to
    `kv_index::artifact::MmapSnapshotLoadOptions`, defaulting to
    `ArtifactSourceProgressPolicy::kRequired`;
  - pass that field into `ParseArtifact` from
    `MmapSnapshotBacking::LoadShard`.
- In `RunExternalArtifactLoad`, choose mmap source-progress policy from
  `ForwardIndexOptions::mode`:
  - `kRealtimeDelta` keeps `kRequired`;
  - `kFullSnapshotOnly` uses `kOptional`.
- Add a full-snapshot-only branch after every shard has loaded and prewarmed,
  and before `BuildSafeProgress`, `CreateRealtimeShards`, or
  `CreateCatchUpRunner`.
- The full-only branch should publish one `runtime::ShardState` per shard with
  only `full_snapshot` populated. It must not allocate realtime delta tables,
  create a catch-up runner, run Kafka catch-up, or require non-empty
  source-progress.
- Preserve the existing realtime/Kafka branch after the full-only branch. Do
  not alter default catch-up semantics, safe-progress checks, or publication
  layering for `kRealtimeDelta`.
- Create only the first focused FSO04 behavior test in
  `tests/unit/full_snapshot_only_load_test.cc` and add only the Bazel target
  needed to run it. Do not add FSO05 replacement-semantics, runtime-status,
  schema-cutover, or pinned-row-lifetime tests in this task.
- Do not modify README, default-mode regression tests, mmap policy tests, or
  any FSO05+ behavior in this task.

### Dependencies
- FSO01 is verified and provides `ForwardIndexMode` plus constructor rejection
  of Kafka config in full-snapshot-only mode.
- FSO02 is verified and provides
  `ArtifactSourceProgressPolicy::{kRequired, kOptional}` at parser level.
- FSO03 is verified and provides
  `TestArtifactSpec::include_source_progress_section = false` for generating
  valid no-source-progress artifacts.
- Current `RunExternalArtifactLoad` already:
  - loads every shard with `MmapSnapshotBacking::LoadShard`;
  - prewarms every backing;
  - then builds Kafka progress, creates realtime shards, creates the catch-up
    runner, and publishes states with realtime + full layers.
- Current `ShardState::Layers` can represent full-only publication by leaving
  `realtime_delta` null and `compact_delta` empty while setting
  `full_snapshot`.

### Risk Points
- Branch placement matters: full-only must run after mmap load and prewarm so
  the published snapshots are ready, but before any Kafka progress validation
  or catch-up factory invocation.
- Optional source-progress must be selected only for full-snapshot-only async
  loads. The default mode must still fail closed when source progress is
  missing.
- Missing source-progress in full-only mode should leave
  `LoadState::source_progress.partitions` empty and still allow successful
  cutover.
- Present malformed source-progress remains a parser failure through the
  existing FSO02 path; do not catch or downgrade parser errors in mmap load.
- Cancellation and publish errors should use the existing `CheckCancelled`,
  `MarkFailed`, and `StoreState` patterns so terminal load state remains
  consistent with the realtime branch.
- The full-only branch should keep generation equal to the load id and should
  preserve `artifact_id` behavior from the mmap artifact/request.
- Be careful with C++20 designated initializers: if
  `MmapSnapshotLoadOptions` gains a field, keep initializer field order aligned
  with the struct declaration.
- The new test installs the global catch-up factory override. Use an RAII
  helper like `tests/unit/async_load_test.cc` so the factory is restored even
  if the test fails.

### First Focused Test Guidance
- Create `tests/unit/full_snapshot_only_load_test.cc`.
- Keep the first test intentionally narrow, for example
  `FullOnlyLoadPublishesNoSourceProgressArtifactWithoutCatchUp`:
  - construct `ForwardIndexOptions` with `mode =
    ForwardIndexMode::kFullSnapshotOnly`, `shard_count = 1`, and a simple hash
    seed/version;
  - build a one-field schema and one row using the same local row-encoding
    helpers as `tests/unit/mmap_snapshot_backing_test.cc` or
    `tests/unit/async_load_test.cc`;
  - write a one-shard `TestArtifactSpec` with
    `include_source_progress_section = false`, an empty `source_progress`
    vector, and the row in shard 0;
  - install `SetAsyncCatchUpRunnerFactoryForTesting` with a lambda that sets an
    `std::atomic_bool catch_up_factory_called` and returns
    `Status::Internal("catch-up should not be created in full-only mode")`;
  - call `index.LoadAsync({.artifact_uri = path, .artifact_id =
    spec.artifact_id})`;
  - wait for terminal state using the existing polling style from
    `tests/unit/async_load_test.cc`;
  - assert `LoadStateCode::kSucceeded`, `terminal == true`,
    `loaded_shard_count == 1`, `prewarmed_shard_count == 1`,
    `cutover_shard_count == 1`, and
    `state.source_progress.partitions.empty()`;
  - assert `catch_up_factory_called == false`;
  - assert `index.Get(primary_key)` returns the artifact row and expected field
    value.
- Add this Bazel target in `tests/BUILD.bazel`:

  ```python
  cc_test(
      name = "full_snapshot_only_load_test",
      srcs = ["unit/full_snapshot_only_load_test.cc"],
      size = "small",
      deps = [
          ":test_support",
          "//:kv_index_internal",
          "//:kv_index",
      ],
  )
  ```

- Add `:full_snapshot_only_load_test` to both `unit_tests` and `all_tests`.

### Acceptance Criteria
- `MmapSnapshotLoadOptions{}` keeps requiring source progress by default.
- Full-snapshot-only async load passes optional source-progress policy into
  mmap parsing.
- A full-snapshot-only `ForwardIndex` can `LoadAsync` a valid artifact that has
  no `kSourceProgress` section.
- That load succeeds after all shards are loaded, prewarmed, and cut over.
- The full-only load does not invoke the async catch-up factory and does not
  allocate or publish realtime delta tables.
- Published full-only shard states contain `full_snapshot` only, so `Get`
  serves rows from the mmap-backed artifact.
- Existing realtime/Kafka async-load code path remains structurally unchanged
  after the full-only branch and still uses required source progress.
- No FSO05+ replacement, runtime-status, schema-evolution, README, or
  default-mode regression work is included.

### Focused Tests
- Coding-agent loop:
  - Add the new test file and Bazel target first.
  - Run `bazel test //tests:full_snapshot_only_load_test` and confirm RED.
    Expected failure: missing mmap policy threading and/or default async-load
    path rejects empty source progress before publishing.
  - Add mmap policy threading and the full-only async-load branch.
  - Run `bazel test //tests:full_snapshot_only_load_test` and confirm GREEN.
  - Run `git diff --check -- src/artifact/mmap_snapshot_backing.h src/artifact/mmap_snapshot_backing.cc src/rebuild/async_load.cc tests/unit/full_snapshot_only_load_test.cc tests/BUILD.bazel docs/tasks/FSO04-full-only-async-load-branch.md`.
- Do not run aggregate or final acceptance suites for FSO04.

## Implementation Log
- 2026-05-02 CST coding:
  - Added `tests/unit/full_snapshot_only_load_test.cc` with
    `FullOnlyLoadPublishesNoSourceProgressArtifactWithoutCatchUp`.
    The test constructs a one-shard full-snapshot-only `ForwardIndex`, writes an
    artifact without a `kSourceProgress` section, installs an RAII-restored
    catch-up factory override that records/fails if invoked, runs `LoadAsync`,
    verifies succeeded terminal load state/counts with empty source progress,
    verifies the catch-up factory was not called, and verifies `Get` serves the
    artifact row.
  - Added `//tests:full_snapshot_only_load_test` to `tests/BUILD.bazel` and
    included it in `unit_tests` and `all_tests`.
  - Added `source_progress_policy` to
    `artifact::MmapSnapshotLoadOptions`, defaulting to
    `ArtifactSourceProgressPolicy::kRequired`, and passed it into
    `ParseArtifact`.
  - In `RunExternalArtifactLoad`, selected optional source-progress policy only
    when `ForwardIndexOptions::mode == ForwardIndexMode::kFullSnapshotOnly`.
  - Added the full-snapshot-only branch after all shard mmap loads and prewarms
    and before Kafka safe-progress/realtime/catch-up setup. The branch clears
    load-state source progress, publishes per-shard `ShardState` values with
    only `full_snapshot`, updates cutover counts, stores succeeded terminal
    state, and returns `Status::Ok()`.

### Files Changed
- `src/artifact/mmap_snapshot_backing.h`
- `src/artifact/mmap_snapshot_backing.cc`
- `src/rebuild/async_load.cc`
- `tests/unit/full_snapshot_only_load_test.cc`
- `tests/BUILD.bazel`
- `docs/tasks/FSO04-full-only-async-load-branch.md`

### Commands Run
- `bazel test //tests:full_snapshot_only_load_test`
  - RED: failed as expected at
    `tests/unit/full_snapshot_only_load_test.cc:158` because terminal load
    state was not `LoadStateCode::kSucceeded` before FSO04 implementation.
- `bazel test //tests:full_snapshot_only_load_test`
  - GREEN: passed, executed 1/1 test.
- `bazel test --cache_test_results=no //tests:full_snapshot_only_load_test`
  - GREEN fresh rerun: passed, executed 1/1 test.
- `git diff --check -- src/artifact/mmap_snapshot_backing.h src/artifact/mmap_snapshot_backing.cc src/rebuild/async_load.cc tests/unit/full_snapshot_only_load_test.cc tests/BUILD.bazel docs/tasks/FSO04-full-only-async-load-branch.md`
  - Passed with no whitespace errors.
- `git diff -- src/artifact/mmap_snapshot_backing.h src/artifact/mmap_snapshot_backing.cc src/rebuild/async_load.cc tests/unit/full_snapshot_only_load_test.cc tests/BUILD.bazel`
  - Reviewed scoped implementation diff.
- `git status --short`
  - Confirmed FSO04 files are modified/untracked alongside prior FSO01-FSO03
    and controller/doc work; no commits created.

### Risks
- Aggregate suites were not run, per FSO04 scope.
- No FSO05+ replacement semantics, runtime-status, schema/pinned-lifetime
  coverage, README updates, or default-mode regression tests were added.

## Verification Log
- 2026-05-02 CST verify:
  - Reviewed the scoped FSO04 diff for
    `src/artifact/mmap_snapshot_backing.h`,
    `src/artifact/mmap_snapshot_backing.cc`, `src/rebuild/async_load.cc`,
    `tests/unit/full_snapshot_only_load_test.cc`, and
    `tests/BUILD.bazel` against the acceptance criteria.
  - Confirmed `MmapSnapshotLoadOptions{}` still defaults
    `source_progress_policy` to
    `ArtifactSourceProgressPolicy::kRequired`.
  - Confirmed `RunExternalArtifactLoad` selects optional source-progress
    parsing only for `ForwardIndexMode::kFullSnapshotOnly`.
  - Confirmed the full-only branch is after mmap load and prewarm, before
    `BuildSafeProgress`, `CreateRealtimeShards`, and `CreateCatchUpRunner`,
    and publishes `ShardState` layers with only `full_snapshot` populated.
  - `git diff --check -- src/artifact/mmap_snapshot_backing.h src/artifact/mmap_snapshot_backing.cc src/rebuild/async_load.cc tests/unit/full_snapshot_only_load_test.cc tests/BUILD.bazel`
    passed with no whitespace errors.
  - `bazel test //tests:full_snapshot_only_load_test` passed from cache.
  - `bazel test --cache_test_results=no //tests:full_snapshot_only_load_test`
    passed, executed 1/1 test.
  - Conclusion: pass.

## Decisions
- Full-only branch must happen after mmap load and prewarm, before realtime/Kafka catch-up setup.
- The first FSO04 test should prove both successful read from a no-source-
  progress artifact and zero catch-up factory invocation.

## Open Issues
- None for FSO04 planning.

## Next Step
- Controller selects FSO05.
