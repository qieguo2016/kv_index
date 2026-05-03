# FSO06 Existing-Mode Regressions

## Metadata
- Status: completed
- Owner Role: controller
- Depends on: FSO05
- Retry Count: 0
- Last Updated: 2026-05-02 CST

## Scope
Add default-mode regressions proving source progress remains required and Kafka catch-up behavior remains unchanged.

## Plan Notes
### Implementation Boundary
- Only implement FSO06 from
  `docs/superpowers/plans/2026-05-02-full-snapshot-only-mode.md`.
- Modify only these test files:
  - `tests/unit/async_load_test.cc`
  - `tests/unit/mmap_snapshot_backing_test.cc`
- Do not modify production source, `tests/BUILD.bazel`, `README.md`, or
  full-snapshot-only behavior tests. If a regression test exposes a production
  bug, stop and record the failure for verify/controller rather than expanding
  this task into a source fix without direction.
- Keep the focus on the existing default `ForwardIndexMode::kRealtimeDelta`
  behavior:
  - missing `kSourceProgress` remains a failed async load;
  - default async load still creates and runs Kafka catch-up even when artifact
    checkpoint and artifact high watermark are equal;
  - default mmap load still requires source progress unless explicitly
    configured with optional source-progress policy.
- Do not add FSO05 replacement/runtime-status/schema/pinned-lifetime coverage
  and do not do FSO07 README or aggregate final verification work.

### Dependencies And Context
- FSO02 added
  `artifact::ArtifactSourceProgressPolicy::{kRequired, kOptional}` and keeps
  `ArtifactValidationOptions::source_progress_policy` defaulting to required.
- FSO03 added `TestArtifactSpec::include_source_progress_section = false`,
  which should be used to create missing-source-progress artifacts.
- FSO04 threaded source-progress policy through
  `artifact::MmapSnapshotLoadOptions`, defaulting to required, and selects
  optional policy only for `ForwardIndexMode::kFullSnapshotOnly` in async load.
- FSO05 completed full-only behavior coverage in
  `tests/unit/full_snapshot_only_load_test.cc`; do not duplicate that work.
- `tests/unit/async_load_test.cc` already has useful helpers:
  `TwoShardSpec`, `ScopedCatchUpRunnerFactory`, `FakeCatchUpControl`,
  `SinglePartitionProgress`, `WaitForTerminalState`, `WaitForPollCalls`, and
  `ReleaseCatchUp`.
- `tests/unit/mmap_snapshot_backing_test.cc` already has `SpecWithOneRow()` and
  `OptionsFor(spec)` helpers for focused mmap loader policy tests.

### Risk Points
- The default async-load path must continue using required source-progress
  policy. A no-source-progress artifact in default mode should fail during mmap
  load/parsing, before prewarm or cutover.
- A failed missing-source-progress default load should not publish any shard:
  `cutover_shard_count == 0` and `index.Get(key)` should still miss.
- The catch-up-required regression should prove the existing realtime branch
  still observes post-start safe progress. The existing
  `CatchUpCapturesPostStartSafeWatermarkEvenWhenArtifactLooksCaughtUp` test
  already covers this; coding may strengthen assertions there instead of
  adding a duplicate test.
- If adding a new catch-up regression, avoid timing-flaky assertions. Reuse
  `WaitForPollCalls(control, 1)` to prove catch-up entered polling while load
  remains running and the row is not visible yet.
- In mmap tests, use default `MmapSnapshotLoadOptions` or `OptionsFor(spec)` to
  prove required-by-default behavior, then set only
  `.source_progress_policy = ArtifactSourceProgressPolicy::kOptional` for the
  positive optional-policy case.
- Keep C++20 designated initializers in declaration order if adding explicit
  `MmapSnapshotLoadOptions` literals.

### Acceptance Criteria
- `tests/unit/async_load_test.cc` has a default-mode regression where an
  artifact with `include_source_progress_section = false` causes `LoadAsync` to
  finish with `LoadStateCode::kFailed`, zero cutover shards, and no newly
  visible row.
- `tests/unit/async_load_test.cc` continues to prove default mode creates and
  runs catch-up even when the artifact itself looks caught up. The assertion
  should confirm load state remains `kRunning` after catch-up polling starts
  and before catch-up advances to the observed safe watermark.
- `tests/unit/mmap_snapshot_backing_test.cc` has coverage that default mmap
  load rejects missing source progress with `StatusCode::kInvalidArgument`.
- `tests/unit/mmap_snapshot_backing_test.cc` has coverage that explicitly
  optional mmap source-progress policy accepts the same missing-progress
  artifact and serves the row through `FullSnapshotView`.
- No source, BUILD, README, or full-only behavior-test files are changed for
  FSO06.

### Focused Tests
- Add `DefaultModeRejectsArtifactMissingSourceProgress` to
  `tests/unit/async_load_test.cc`:
  - construct default `ForwardIndexOptions` with `shard_count = 2` and known
    hash seed;
  - choose a key in shard 1 with `FindKeyForShard`;
  - build `TwoShardSpec(options, key, score, checkpoint, high_watermark)`;
  - set `spec.include_source_progress_section = false`;
  - write the artifact and call `index.LoadAsync`;
  - wait for terminal state;
  - assert `LoadStateCode::kFailed`, `terminal == true`,
    `loaded_shard_count == 0`, `cutover_shard_count == 0`, and
    `!index.Get(key).has_value()`.
- Keep or strengthen `CatchUpCapturesPostStartSafeWatermarkEvenWhenArtifactLooksCaughtUp`
  in `tests/unit/async_load_test.cc`:
  - artifact checkpoint/high watermark should be equal, for example `5/5`;
  - fake catch-up should observe a later safe watermark such as `8/8`;
  - after `WaitForPollCalls(control, 1)`, assert the load is still running and
    the artifact row is not visible;
  - after releasing catch-up, assert success and source progress advances to
    offset/high watermark `8`.
- Add `DefaultMmapLoadRejectsMissingSourceProgress` to
  `tests/unit/mmap_snapshot_backing_test.cc`:
  - create `SpecWithOneRow()`;
  - set `spec.include_source_progress_section = false`;
  - write the artifact;
  - call `MmapSnapshotBacking::LoadShard(path, 0, OptionsFor(spec))`;
  - assert it fails with `StatusCode::kInvalidArgument`.
- Add `OptionalMmapLoadAcceptsMissingSourceProgress` to
  `tests/unit/mmap_snapshot_backing_test.cc`:
  - reuse the same missing-progress artifact shape;
  - set `options.source_progress_policy =
    kv_index::artifact::ArtifactSourceProgressPolicy::kOptional`;
  - assert `LoadShard` succeeds and `FullSnapshotView` returns key `42` with
    field `1 == 88`.
- Run focused verification:

```bash
bazel test //tests:async_load_test //tests:mmap_snapshot_backing_test
```

- Also run a whitespace check on only the touched FSO06 files:

```bash
git diff --check -- tests/unit/async_load_test.cc tests/unit/mmap_snapshot_backing_test.cc docs/tasks/FSO06-existing-mode-regressions.md
```

## Implementation Log
- 2026-05-02 CST coding:
  - Added
    `DefaultModeRejectsArtifactMissingSourceProgress` in
    `tests/unit/async_load_test.cc`. The test writes a default-mode artifact
    without `kSourceProgress`, verifies terminal `kFailed`, keeps
    `loaded_shard_count == 0` and `cutover_shard_count == 0`, and confirms the
    artifact row is not visible.
  - Strengthened
    `CatchUpCapturesPostStartSafeWatermarkEvenWhenArtifactLooksCaughtUp` to
    assert the load remains `kRunning` and non-terminal after catch-up polling
    starts and before release.
  - Added `DefaultMmapLoadRejectsMissingSourceProgress` and
    `OptionalMmapLoadAcceptsMissingSourceProgress` in
    `tests/unit/mmap_snapshot_backing_test.cc`, covering required-by-default
    mmap behavior and explicit optional-policy serving through
    `FullSnapshotView`.
  - Files changed:
    `tests/unit/async_load_test.cc`,
    `tests/unit/mmap_snapshot_backing_test.cc`, and this task document.
  - Commands/results:
    `bazel test //tests:async_load_test //tests:mmap_snapshot_backing_test`
    passed, executing 2/2 tests.
  - Risks:
    aggregate/final verification remains out of scope for FSO06. No source,
    BUILD, README, or `tests/unit/full_snapshot_only_load_test.cc` changes were
    made.

## Verification Log
- 2026-05-02 CST verify:
  - Reviewed the scoped FSO06 diff for
    `tests/unit/async_load_test.cc` and
    `tests/unit/mmap_snapshot_backing_test.cc` against the acceptance
    criteria.
  - Confirmed `DefaultModeRejectsArtifactMissingSourceProgress` covers default
    realtime mode failing a no-source-progress artifact with terminal
    `LoadStateCode::kFailed`, zero loaded/cutover shards, and no visible row.
  - Confirmed
    `CatchUpCapturesPostStartSafeWatermarkEvenWhenArtifactLooksCaughtUp`
    still proves default mode enters catch-up polling, remains non-terminal
    `kRunning` before release, and advances source progress to offset/high
    watermark `8` after catch-up completes.
  - Confirmed mmap coverage rejects missing source progress by default with
    `StatusCode::kInvalidArgument`, while explicitly optional policy accepts
    the same artifact and serves the row through `FullSnapshotView`.
  - Confirmed FSO06 did not add source, BUILD, README, or full-only
    behavior-test scope; broader worktree changes belong to previous FSO tasks.
  - Commands/results:
    `git diff --check -- tests/unit/async_load_test.cc tests/unit/mmap_snapshot_backing_test.cc docs/tasks/FSO06-existing-mode-regressions.md`
    passed with no output.
  - Commands/results:
    `git diff --check --no-index /dev/null docs/tasks/FSO06-existing-mode-regressions.md`
    emitted no whitespace diagnostics. The command exited 1 because the
    untracked file differs from `/dev/null`.
  - Commands/results:
    `bazel test //tests:async_load_test //tests:mmap_snapshot_backing_test`
    passed from cache, executing 0/2 tests.
  - Commands/results:
    `bazel test --cache_test_results=no //tests:async_load_test //tests:mmap_snapshot_backing_test`
    passed, executing 2/2 tests.
  - Conclusion: pass.

## Decisions
- Do not weaken default realtime/Kafka async-load semantics.

## Open Issues
- None for FSO06 verification.

## Next Step
- Controller selects FSO07.
