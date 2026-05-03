# Full Snapshot Only Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a constructor-selected full-snapshot-only serving mode where reads use only full snapshots and updates happen only through explicit async full-artifact loads.

**Architecture:** Keep `ForwardIndex` as the public API and add a serving mode to `ForwardIndexOptions`. The existing realtime mode continues to load artifacts with Kafka catch-up and publish `realtime_delta -> compact_delta -> full_snapshot` shard states, while full-snapshot-only mode loads mmap full artifacts, prewarms them, and publishes shard states containing only `full_snapshot`. Artifact source progress remains required by default, but full-snapshot-only loads allow the source-progress section to be missing.

**Tech Stack:** C++20, Bazel only, standard library first, existing mmap artifact loader, existing `ShardDirectory`/`ShardState` publication model, existing test macros and test artifact writer.

---

## Decisions Locked By Review

- Full-snapshot-only mode is selected at construction time through `ForwardIndexOptions`, not per `LoadRequest`.
- Full-snapshot-only mode rejects any non-empty `KafkaConsumerConfig` during `ForwardIndex` construction.
- Full-snapshot-only mode does not create Kafka consumers, catch-up runners, realtime delta tables, compact deltas, or internal full rebase state.
- Full-snapshot-only mode tolerates artifacts with no `kSourceProgress` section.
- If a `kSourceProgress` section is present but malformed, artifact parsing still fails closed.
- The default mode keeps the existing behavior and continues to require source progress for external async load.

## File Map

- Modify `include/kv_index/forward_index.h`
  - Add the public serving-mode enum.
  - Add `ForwardIndexOptions::mode` with a backward-compatible default.
- Modify `src/api/forward_index.cc`
  - Validate full-snapshot-only options at construction.
  - Keep read APIs unchanged.
  - Pass the selected mode through existing async-load options.
- Modify `src/artifact/artifact_format.h`
  - Add source-progress policy to `ArtifactValidationOptions`.
- Modify `src/artifact/artifact_format.cc`
  - Make missing source progress optional only when the parser policy allows it.
- Modify `src/artifact/mmap_snapshot_backing.h`
  - Add source-progress policy to `MmapSnapshotLoadOptions`.
- Modify `src/artifact/mmap_snapshot_backing.cc`
  - Pass the policy into `ParseArtifact`.
- Modify `src/rebuild/async_load.cc`
  - Branch after mmap load and prewarm.
  - Keep existing Kafka catch-up path for default mode.
  - Add a full-only publish path that creates shard states containing only full snapshots.
- Modify `tests/test_support/artifact_writer.h`
  - Add a test-only way to omit the source-progress section.
- Modify `tests/test_support/artifact_writer.cc`
  - Honor the omit-source-progress option and write a correct section count.
- Modify `tests/BUILD.bazel`
  - Add the new focused test target to `unit_tests` and `all_tests`.
- Create `tests/unit/full_snapshot_only_load_test.cc`
  - Cover full-only constructor policy, optional source progress, no catch-up, full-only publication, full replacement semantics, and runtime status.
- Modify `tests/unit/artifact_format_test.cc`
  - Cover source-progress policy in the artifact parser.
- Modify `tests/unit/mmap_snapshot_backing_test.cc`
  - Cover optional source progress through the mmap loader.
- Modify `tests/unit/async_load_test.cc`
  - Add default-mode regressions proving source progress and catch-up are still required.
- Modify `README.md`
  - Document the two serving modes and full-only limitations.

## Task List

| ID | Task Name | Description |
|----|-----------|-------------|
| 1 | Public Mode Option | Add constructor-level serving mode and fail-fast Kafka config validation. |
| 2 | Optional Source Progress Parsing | Allow missing `kSourceProgress` only when requested by parser options. |
| 3 | Test Artifact Writer Support | Let tests generate artifacts without source-progress sections. |
| 4 | Full-Only Async Load Branch | Publish full-only shard states without Kafka catch-up or realtime deltas. |
| 5 | Focused Full-Only Tests | Add end-to-end unit coverage for the new mode. |
| 6 | Existing-Mode Regressions | Prove the default Kafka catch-up mode remains unchanged. |
| 7 | Docs And Final Verification | Document the mode and run aggregate Bazel checks. |

---

## Task 1: Public Mode Option

**Files:**
- Modify `include/kv_index/forward_index.h`
- Modify `src/api/forward_index.cc`
- Test `tests/unit/forward_index_test.cc`

- [ ] **Step 1: Write failing option tests**

Add tests to `tests/unit/forward_index_test.cc`:

- `DefaultModeRemainsRealtimeDelta`
- `FullSnapshotOnlyModeAllowsEmptyKafkaConfig`
- `FullSnapshotOnlyModeRejectsKafkaConfig`

Test intent:

```cpp
void DefaultModeRemainsRealtimeDelta() {
  kv_index::ForwardIndexOptions options;
  KV_INDEX_CHECK_EQ(options.mode, kv_index::ForwardIndexMode::kRealtimeDelta);
}

void FullSnapshotOnlyModeAllowsEmptyKafkaConfig() {
  kv_index::ForwardIndexOptions options;
  options.mode = kv_index::ForwardIndexMode::kFullSnapshotOnly;
  kv_index::ForwardIndex index(options);
  KV_INDEX_CHECK_EQ(index.options().mode,
                    kv_index::ForwardIndexMode::kFullSnapshotOnly);
}

void FullSnapshotOnlyModeRejectsKafkaConfig() {
  kv_index::ForwardIndexOptions options;
  options.mode = kv_index::ForwardIndexMode::kFullSnapshotOnly;
  options.kafka_consumer.bootstrap_servers = "localhost:9092";
  KV_INDEX_CHECK_THROWS(kv_index::ForwardIndex index(options),
                        std::invalid_argument);
}
```

- [ ] **Step 2: Run the focused test and confirm RED**

Run:

```bash
bazel test //tests:forward_index_unit_test
```

Expected: compile failure because `ForwardIndexMode` and `ForwardIndexOptions::mode` do not exist.

- [ ] **Step 3: Add the public enum and option**

In `include/kv_index/forward_index.h`, add:

```cpp
enum class ForwardIndexMode : std::uint8_t {
  kRealtimeDelta = 0,
  kFullSnapshotOnly = 1,
};
```

Then add this field to `ForwardIndexOptions`:

```cpp
ForwardIndexMode mode = ForwardIndexMode::kRealtimeDelta;
```

- [ ] **Step 4: Add constructor validation**

In `src/api/forward_index.cc`, add a small anonymous-namespace helper:

```cpp
bool HasKafkaConsumerConfig(const KafkaConsumerConfig& config) {
  return !config.bootstrap_servers.empty() || !config.group_id.empty() ||
         !config.topics.empty();
}
```

In `ForwardIndex::ForwardIndex`, after shard-count validation:

```cpp
if (options_.mode == ForwardIndexMode::kFullSnapshotOnly &&
    HasKafkaConsumerConfig(options_.kafka_consumer)) {
  throw std::invalid_argument(
      "full-snapshot-only mode does not accept Kafka consumer config");
}
```

- [ ] **Step 5: Run the focused test and confirm GREEN**

Run:

```bash
bazel test //tests:forward_index_unit_test
```

Expected: PASS.

## Task 2: Optional Source Progress Parsing

**Files:**
- Modify `src/artifact/artifact_format.h`
- Modify `src/artifact/artifact_format.cc`
- Test `tests/unit/artifact_format_test.cc`

- [ ] **Step 1: Write failing parser policy tests**

Add coverage to `tests/unit/artifact_format_test.cc`:

- default parser rejects an artifact with no `kSourceProgress` section;
- parser with optional source-progress policy accepts the same artifact;
- parser with optional source-progress policy still rejects malformed source-progress bytes when the section exists.

Use the test writer support from Task 3 once available. If implementing this task before Task 3, first write the tests with a local helper that removes the source-progress section from serialized bytes.

- [ ] **Step 2: Run parser tests and confirm RED**

Run:

```bash
bazel test //tests:artifact_format_test
```

Expected: compile failure because the parser policy does not exist, or behavior failure because missing source progress is always rejected.

- [ ] **Step 3: Add the parser policy**

In `src/artifact/artifact_format.h`, add:

```cpp
enum class ArtifactSourceProgressPolicy : std::uint8_t {
  kRequired = 0,
  kOptional = 1,
};
```

Add to `ArtifactValidationOptions`:

```cpp
ArtifactSourceProgressPolicy source_progress_policy =
    ArtifactSourceProgressPolicy::kRequired;
```

- [ ] **Step 4: Implement optional missing-section behavior**

In `ParseArtifact`, keep the current required behavior by default. Change only the missing-section branch:

```cpp
auto progress_section =
    artifact.FindSection(ArtifactSectionType::kSourceProgress,
                         kArtifactGlobalShardId);
if (!progress_section.has_value()) {
  if (options.source_progress_policy ==
      ArtifactSourceProgressPolicy::kOptional) {
    artifact.source_progress.clear();
    return artifact;
  }
  return Status::InvalidArgument("artifact source progress section missing");
}
```

If the section exists, continue to call `ParseSourceProgressSection` and return any parsing error unchanged.

- [ ] **Step 5: Run parser tests and confirm GREEN**

Run:

```bash
bazel test //tests:artifact_format_test
```

Expected: PASS.

## Task 3: Test Artifact Writer Support

**Files:**
- Modify `tests/test_support/artifact_writer.h`
- Modify `tests/test_support/artifact_writer.cc`
- Test `tests/unit/artifact_format_test.cc`

- [ ] **Step 1: Write failing test-writer use**

Update the Task 2 tests to request an artifact with no source-progress section through the shared test writer.

Test intent:

```cpp
TestArtifactSpec spec = ValidSpec();
spec.include_source_progress_section = false;
KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());
```

- [ ] **Step 2: Run artifact tests and confirm RED**

Run:

```bash
bazel test //tests:artifact_format_test
```

Expected: compile failure because the writer option does not exist.

- [ ] **Step 3: Add writer option**

In `tests/test_support/artifact_writer.h`, add:

```cpp
bool include_source_progress_section = true;
```

to `TestArtifactSpec`.

- [ ] **Step 4: Honor the writer option**

In `WriteTestArtifact`, calculate section count as:

```cpp
const std::uint32_t section_count =
    static_cast<std::uint32_t>(1 +
                               (spec.include_source_progress_section ? 1 : 0) +
                               spec.shard_count * 7);
```

Always write schema metadata. Write `ArtifactSectionType::kSourceProgress` only when `include_source_progress_section` is true.

- [ ] **Step 5: Run artifact tests and confirm GREEN**

Run:

```bash
bazel test //tests:artifact_format_test
```

Expected: PASS.

## Task 4: Full-Only Async Load Branch

**Files:**
- Modify `src/artifact/mmap_snapshot_backing.h`
- Modify `src/artifact/mmap_snapshot_backing.cc`
- Modify `src/rebuild/async_load.cc`
- Test `tests/unit/full_snapshot_only_load_test.cc`

- [ ] **Step 1: Write failing full-only load tests**

Create `tests/unit/full_snapshot_only_load_test.cc` with a first test:

- construct `ForwardIndex` with `options.mode = ForwardIndexMode::kFullSnapshotOnly`;
- generate an artifact with no source-progress section;
- install a catch-up factory that fails if called;
- call `LoadAsync`;
- wait for terminal state;
- expect `LoadStateCode::kSucceeded`;
- expect `index.Get(key)` returns the artifact row.

- [ ] **Step 2: Add Bazel target and confirm RED**

In `tests/BUILD.bazel`, add:

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

Add it to `unit_tests` and `all_tests`.

Run:

```bash
bazel test //tests:full_snapshot_only_load_test
```

Expected: failure because `MmapSnapshotBacking` still requires source progress through default parsing and async load still creates a catch-up runner.

- [ ] **Step 3: Thread source-progress policy through mmap loading**

In `MmapSnapshotLoadOptions`, add:

```cpp
artifact::ArtifactSourceProgressPolicy source_progress_policy =
    artifact::ArtifactSourceProgressPolicy::kRequired;
```

In `MmapSnapshotBacking::LoadShard`, pass the policy to `ParseArtifact` through `ArtifactValidationOptions`.

- [ ] **Step 4: Select load policy from mode**

In `RunExternalArtifactLoad`, set:

```cpp
const artifact::MmapSnapshotLoadOptions load_options{
    .expected_shard_count = options.shard_count,
    .expected_hash_seed = options.hash_seed,
    .expected_hash_version = options.hash_version,
    .source_progress_policy =
        options.mode == ForwardIndexMode::kFullSnapshotOnly
            ? artifact::ArtifactSourceProgressPolicy::kOptional
            : artifact::ArtifactSourceProgressPolicy::kRequired,
};
```

- [ ] **Step 5: Add full-only publish helper**

In `src/rebuild/async_load.cc`, after all shards are loaded and prewarmed, branch before `BuildSafeProgress`:

```cpp
if (options.mode == ForwardIndexMode::kFullSnapshotOnly) {
  state->source_progress.partitions.clear();
  state->message = "full snapshot load publishing";
  StoreState(callbacks, *state);

  for (std::uint32_t shard_id = 0; shard_id < options.shard_count; ++shard_id) {
    // Check cancellation, build FullSnapshotView, publish ShardState with no
    // realtime_delta and no compact_delta, update cutover progress.
  }

  state->code = LoadStateCode::kSucceeded;
  state->terminal = true;
  state->message = "full snapshot load succeeded";
  StoreState(callbacks, *state);
  return Status::Ok();
}
```

Keep the existing realtime/Kafka catch-up branch unchanged after this point.

- [ ] **Step 6: Run focused full-only test and confirm GREEN**

Run:

```bash
bazel test //tests:full_snapshot_only_load_test
```

Expected: PASS.

## Task 5: Focused Full-Only Behavior Tests

**Files:**
- Modify `tests/unit/full_snapshot_only_load_test.cc`

- [ ] **Step 1: Add replacement semantics test**

Test:

- load artifact A containing `old_key`;
- verify `old_key` hits;
- load artifact B that omits `old_key` and contains `new_key`;
- verify `old_key` misses and `new_key` hits after B succeeds.

Expected semantics: the full snapshot is replaced per shard. Missing keys in the new artifact disappear after cutover.

- [ ] **Step 2: Add runtime status test**

After a successful full-only load, assert:

- `has_full_snapshot == true`;
- `has_realtime_delta == false`;
- `has_compact_delta == false`;
- `full_row_count` matches the shard artifact;
- `artifact_id` is reported for mmap-backed full snapshots.

- [ ] **Step 3: Add schema cutover and pinned-row lifetime test**

Use `schema_evolution_integration_test.cc` as the model, but run in full-only mode and use an artifact without source progress:

- publish or load old schema;
- pin old row;
- load new full artifact with schema version N+1;
- verify new reads use new schema;
- verify pinned old row remains readable.

- [ ] **Step 4: Run full-only test and confirm GREEN**

Run:

```bash
bazel test //tests:full_snapshot_only_load_test
```

Expected: PASS.

## Task 6: Existing-Mode Regressions

**Files:**
- Modify `tests/unit/async_load_test.cc`
- Modify `tests/unit/mmap_snapshot_backing_test.cc`

- [ ] **Step 1: Add default-mode missing-source-progress regression**

In `tests/unit/async_load_test.cc`, add a default-mode test:

- construct `ForwardIndex` with default options;
- write an artifact with no source-progress section;
- call `LoadAsync`;
- wait for terminal state;
- expect `kFailed`;
- expect no shard cutover.

- [ ] **Step 2: Add default-mode catch-up-required regression**

Keep or add a test proving default mode still creates the catch-up runner even when artifact checkpoint equals artifact high watermark.

Expected: the existing `CatchUpCapturesPostStartSafeWatermarkEvenWhenArtifactLooksCaughtUp` behavior remains unchanged.

- [ ] **Step 3: Add mmap loader policy coverage**

In `tests/unit/mmap_snapshot_backing_test.cc`:

- default `MmapSnapshotLoadOptions` rejects missing source progress;
- `source_progress_policy = kOptional` accepts missing source progress.

- [ ] **Step 4: Run affected tests and confirm GREEN**

Run:

```bash
bazel test //tests:async_load_test //tests:mmap_snapshot_backing_test
```

Expected: PASS.

## Task 7: Docs And Final Verification

**Files:**
- Modify `README.md`

- [ ] **Step 1: Update README mode documentation**

Document:

- default `kRealtimeDelta` mode reads `realtime_delta -> compact_delta -> full_snapshot`;
- `kFullSnapshotOnly` mode reads only full snapshots published by async load;
- full-only mode rejects Kafka config;
- full-only mode does not support Kafka updates, realtime deltas, delta compaction, or internal full rebase;
- full-only mode allows artifacts without `kSourceProgress`;
- full-only async cutover remains per shard and is not a global all-shard atomic version switch.

- [ ] **Step 2: Run focused tests**

Run:

```bash
bazel test //tests:full_snapshot_only_load_test
bazel test //tests:artifact_format_test //tests:mmap_snapshot_backing_test
bazel test //tests:async_load_test //tests:forward_index_unit_test
```

Expected: PASS.

- [ ] **Step 3: Run aggregate tests**

Run:

```bash
bazel test //tests:unit_tests //tests:integration_tests //tests:smoke_tests
```

Expected: PASS.

- [ ] **Step 4: Run full build**

Run:

```bash
bazel build //...
```

Expected: PASS.

## Acceptance Criteria

- `ForwardIndexOptions` exposes a constructor-level mode with default behavior unchanged.
- Full-snapshot-only mode rejects non-empty Kafka consumer config at construction time.
- Full-snapshot-only async load succeeds with artifacts that omit `kSourceProgress`.
- Default async load still fails closed when source progress is missing.
- Malformed present source-progress sections fail closed in every mode.
- Full-snapshot-only async load never creates a catch-up runner or realtime delta table.
- Full-snapshot-only published shard states contain only `full_snapshot`.
- Full replacement semantics are covered: keys absent from the new full artifact disappear after cutover.
- Runtime status reports full snapshot presence and no realtime/compact layers in full-only mode.
- Existing realtime/Kafka tests remain green.
