# FSO01 Public Mode Option

## Metadata
- Status: completed
- Owner Role: controller
- Depends on: none
- Retry Count: 0
- Last Updated: 2026-05-02 CST

## Scope
Add the public constructor-level serving mode option and fail-fast validation that `kFullSnapshotOnly` rejects non-empty Kafka consumer configuration. Preserve the default realtime mode.

## Plan Notes
### Implementation Boundary
- Only implement Task 1 from `docs/superpowers/plans/2026-05-02-full-snapshot-only-mode.md`.
- Modify the public constructor-level API by adding `kv_index::ForwardIndexMode` and `ForwardIndexOptions::mode`.
- Preserve backward compatibility by defaulting `ForwardIndexOptions::mode` to realtime delta mode.
- Add constructor fail-fast validation in `ForwardIndex::ForwardIndex` so `kFullSnapshotOnly` rejects non-empty `KafkaConsumerConfig`.
- Keep all read APIs, async load behavior, artifact parsing, mmap loading, source-progress policy, docs, BUILD files, and later full-snapshot-only load behavior out of scope for FSO01.

### Dependencies
- No task dependency; this is the first full-snapshot-only task.
- Existing API files to inspect/modify during implementation:
  - `include/kv_index/forward_index.h`
  - `src/api/forward_index.cc`
- Existing focused test file and target:
  - `tests/unit/forward_index_test.cc`
  - `bazel test //tests:forward_index_unit_test`
- `KafkaConsumerConfig` currently exposes `bootstrap_servers`, `group_id`, and `topics`; treat any non-empty one as configured.

### Risk Points
- Do not accidentally change the default serving mode; existing users of `ForwardIndexOptions{}` must remain realtime delta mode.
- Keep the enum in the public `kv_index` namespace and include any required standard header already available in the public header.
- Constructor validation should run after existing shard-count validation is fine, but it must happen before a usable index is constructed.
- Avoid over-validating Kafka config in default realtime mode; existing realtime behavior must remain unchanged.
- Tests should use the repository's simple `KV_INDEX_CHECK*` macros and direct function calls from `main()`.

### Acceptance Criteria
- `ForwardIndexOptions{}` has `mode == ForwardIndexMode::kRealtimeDelta`.
- `ForwardIndexOptions` can be set to `ForwardIndexMode::kFullSnapshotOnly` and construct `ForwardIndex` successfully when `kafka_consumer` is empty.
- Constructing `ForwardIndex` with `mode == kFullSnapshotOnly` throws `std::invalid_argument` when any Kafka config field is non-empty.
- Existing invalid shard-count validation and existing read-path tests in `forward_index_test.cc` still pass.
- No source-progress parsing, async-load branching, artifact-writer, README, or BUILD changes are included in this task.

### Focused Tests
- Add tests in `tests/unit/forward_index_test.cc`:
  - `DefaultModeRemainsRealtimeDelta`
  - `FullSnapshotOnlyModeAllowsEmptyKafkaConfig`
  - `FullSnapshotOnlyModeRejectsKafkaConfig`
- Preferred coding-agent loop:
  - Run `bazel test //tests:forward_index_unit_test` before implementation if using TDD to confirm RED/compile failure.
  - Run `bazel test //tests:forward_index_unit_test` after implementation and record the result in `Implementation Log`.
  - Do not run final acceptance or aggregate test suites for FSO01.

## Implementation Log
- Coding agent implemented FSO01 only.
- Tests added in `tests/unit/forward_index_test.cc`:
  - `DefaultModeRemainsRealtimeDelta`
  - `FullSnapshotOnlyModeAllowsEmptyKafkaConfig`
  - `FullSnapshotOnlyModeRejectsKafkaConfig`
- Files changed:
  - `include/kv_index/forward_index.h`
  - `src/api/forward_index.cc`
  - `tests/unit/forward_index_test.cc`
  - `docs/tasks/FSO01-public-mode-option.md`
- Commands run:
  - `bazel test //tests:forward_index_unit_test`
    - RED result before implementation: failed to build as expected because `kv_index::ForwardIndexMode` and `ForwardIndexOptions::mode` did not exist.
  - `bazel test //tests:forward_index_unit_test`
    - GREEN result after implementation: passed. Bazel emitted the existing macOS/librdkafka linker warning, but the target completed successfully.
- Implementation summary:
  - Added public `enum class ForwardIndexMode : std::uint8_t` with `kRealtimeDelta` and `kFullSnapshotOnly`.
  - Added `ForwardIndexOptions::mode`, defaulting to `ForwardIndexMode::kRealtimeDelta`.
  - Added constructor validation so `ForwardIndexMode::kFullSnapshotOnly` rejects non-empty `KafkaConsumerConfig::bootstrap_servers`, `group_id`, or `topics` by throwing `std::invalid_argument`.
- Risks:
  - No final or aggregate acceptance tests were run per FSO01 instructions.
  - Full-snapshot-only artifact parsing, mmap loading, async-load branching, BUILD updates, and README updates remain intentionally out of scope for later tasks.

## Verification Log
- 2026-05-02 CST verify agent reviewed the current FSO01 diff for:
  - `include/kv_index/forward_index.h`
  - `src/api/forward_index.cc`
  - `tests/unit/forward_index_test.cc`
- Acceptance review:
  - `ForwardIndexMode` is public in namespace `kv_index`.
  - `ForwardIndexOptions::mode` defaults to `ForwardIndexMode::kRealtimeDelta`.
  - `ForwardIndex` construction succeeds for `kFullSnapshotOnly` with empty `KafkaConsumerConfig`.
  - `ForwardIndex` construction throws `std::invalid_argument` for `kFullSnapshotOnly` when `bootstrap_servers`, `group_id`, or `topics` is non-empty.
  - Existing invalid shard-count and read-path coverage remains in `forward_index_test.cc`.
  - No source-progress parsing, async-load branching, artifact-writer, README, or BUILD changes were included in the reviewed FSO01 source/test diff.
- Commands/results:
  - `git diff -- include/kv_index/forward_index.h src/api/forward_index.cc tests/unit/forward_index_test.cc`: reviewed, no blocking issues found.
  - `bazel test //tests:forward_index_unit_test`: passed from Bazel cache.
  - `bazel test --cache_test_results=no //tests:forward_index_unit_test`: passed, executed 1 out of 1 test.
- Conclusion: pass.

## Decisions
- Only FSO01 is active. Later optional source-progress and async-load behavior is out of scope for this task.

## Open Issues
- None.

## Next Step
- Controller selects FSO02.
