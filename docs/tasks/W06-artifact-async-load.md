# W06 Artifact Format, Mmap Loading, And External AsyncLoad

## Metadata
- Status: completed
- Owner Role: controller
- Depends on: W05
- Retry Count: 1
- Last Updated: 2026-04-30

## Scope
Implement the external full-artifact path, including artifact format, mmap loading, rebuild generation, per-partition catch-up, and guarded per-shard cutover.

Files in scope:
- Create: `src/core/artifact_format.h`
- Create: `src/core/artifact_format.cc`
- Create: `src/core/mmap_snapshot_backing.h`
- Create: `src/core/mmap_snapshot_backing.cc`
- Create: `src/core/async_load.h`
- Create: `src/core/async_load.cc`
- Create: `tests/test_support/artifact_writer.h`
- Create: `tests/test_support/artifact_writer.cc`
- Create: `tests/unit/artifact_format_test.cc`
- Create: `tests/unit/mmap_snapshot_backing_test.cc`
- Create: `tests/unit/async_load_test.cc`
- Modify: `src/core/forward_index.cc`
- Modify: `include/kv_index/forward_index.h`

Required implementation items:
- Define the sharded artifact header, section directory, checksum rules, hash metadata, and per-partition source watermark/checkpoint representation.
- Add a test artifact writer so integration tests use real tiny artifacts instead of hand-built mocks.
- Implement read-only private mmap loading and `LoadFullShardFromArtifact`.
- Implement shard prewarm for mmap-backed full snapshots by touching each page of the frozen index, row arena, string pools, and list pools before cutover.
- Implement one external load state machine that owns rebuild generation creation, rebuild catch-up consumer startup, cutover guards, cancellation, and fail-closed behavior.
- Keep the current invalid-path smoke behavior while growing real load-state fields for artifact id, progress, errors, shard counts, and source progress.
- Reject shard-count or hash-function mismatches in v1.
- Guard per-shard cutover on loaded-and-prewarmed shard readiness plus documented per-partition catch-up and safe-position conditions.
- Add schema-version cutover behavior so old generations decode with old layout until cutover and rebuild generations decode with the new layout.
- Run `bazel test //tests:unit_tests //tests:integration_tests`.
- Commit with an async-load-module message only after verify passes and controller requests it.

Exit criteria:
- Full artifact loading is mmap-backed and fail-closed.
- A shard is not eligible for external cutover until its mmap-backed full snapshot has completed prewarm.
- External rebuild progress is modeled around per-partition progress and per-shard cutover state.
- Schema-version cutover does not require any public manual publish API.

## Plan Notes
- The W06 section of the detailed implementation plan is already specific enough about the major modules. Do not split this into separate artifact, mmap, and async-load workstreams unless implementation stalls; they need one shared contract for artifact source progress, snapshot lifetime, and guarded cutover.

### Implementation Boundaries
- In scope from the current W06 file list: create `src/core/artifact_format.{h,cc}`, `src/core/mmap_snapshot_backing.{h,cc}`, `src/core/async_load.{h,cc}`, `tests/test_support/artifact_writer.{h,cc}`, `tests/unit/artifact_format_test.cc`, `tests/unit/mmap_snapshot_backing_test.cc`, and `tests/unit/async_load_test.cc`; modify `src/core/forward_index.cc` and `include/kv_index/forward_index.h`.
- The current W06 file list is likely insufficient. Expect to also modify `include/kv_index/types.h` for real `LoadState` progress fields, `src/core/snapshot.{h,cc}` so `FullSnapshotView` can hold mmap-backed data without copying, root `BUILD.bazel` for new internal sources/headers, and `tests/BUILD.bazel` for test support plus new unit/smoke/integration targets.
- Avoid modifying W07-owned internals except where W06 must define reusable interfaces. W06 may create generation/cutover seams that W07 later reuses, but it should not implement compaction, full rebase, or rebase conflict policy.
- Keep W05 contracts as the source of truth: `SourcePosition` is message partition plus message offset, `KafkaCheckpoint` stores next offsets, `KafkaProgress` is per topic/partition, `KafkaUpdateConsumer::CreateForTesting` is the fake-consumer seam, and `UpdateApplier` owns schema/layout validation before publishing realtime rows.

### Artifact Format
- Define a v1 little-endian artifact with a fixed header, section directory, and per-section checksums. The header should include magic, format version, header size, section count, artifact id/hash metadata, checksum algorithm, shard count, hash seed, hash version, schema version, layout fingerprint, and source-progress section id.
- Represent sections with type, shard id or global marker, file offset, byte length, checksum, and alignment. Reject duplicate sections, overlapping ranges, out-of-bounds ranges, unsupported section types, unknown required version bits, and checksum failures before any shard is publishable.
- Required section types should cover schema/layout metadata, per-partition source progress, and per-shard full snapshot data: frozen primary-key index, row slots, row payload arena, string pools, scalar list pools, string list pools, and string element pools. Keep the row materialization shape aligned with `src/core/row_storage.h`; do not introduce a second logical row encoding.
- Store per-partition source progress as topic, partition, artifact checkpoint next offset, and optionally captured high watermark. This is the catch-up start position for the rebuild consumer and the baseline used by cutover guards.
- In v1, reject shard-count mismatches and hash seed/hash-version mismatches against `ForwardIndexOptions`. New schema/layout versions are allowed only through generation cutover: old shards keep decoding with their old layout until that shard is published.
- A test artifact writer is enough for W06. Do not build a production artifact builder, remote artifact service, public data-source abstraction, or public manual publish API.

### Mmap Loading And Prewarm
- Implement local-file read-only private mmap loading (`PROT_READ`, private mapping). Treat unsupported URI schemes, missing files, malformed headers, checksum mismatches, and mapping/prewarm failures as terminal load failures; do not partially publish failed artifacts.
- `MmapSnapshotBacking` should own the file descriptor/mapping lifetime and expose validated typed views over the frozen index and row payload sections. It must not copy the full snapshot into `OwnedSnapshotBacking`, because mmap-backed loading is an explicit W06 requirement.
- `FullSnapshotView`/snapshot plumbing should accept either owned backing or mmap backing while preserving existing read precedence in `ShardState`: realtime delta, compact delta, then full snapshot.
- Prewarm readiness means synchronously touching at least one byte per page for the frozen index, row slots, row arena, string pools, scalar list pools, string list pools, and string element pools for a shard. `madvise`/`posix_madvise` may be added, but it is not sufficient as the readiness condition.
- A shard may become cutover-eligible only after its artifact sections are validated, mmap views are built, and prewarm has completed successfully.

### External AsyncLoad State Machine
- `ForwardIndex::LoadAsync` should create an external load record, return a non-zero `LoadId`, and run the state machine without blocking normal reads. Preserve the invalid-path smoke behavior: `file:///tmp/kv_index/nonexistent-artifact` still produces a terminal `kFailed` state with a non-empty message.
- Grow `LoadState` with enough public status to debug real loads: artifact id/uri, total shard count, loaded/prewarmed/cutover shard counts, last error, terminal flag, source progress/checkpoint, and per-shard cutover state. Keep `LoadStateCode` coarse (`Pending`, `Running`, `Succeeded`, `Failed`, `Cancelled`) unless a finer enum is needed internally.
- One external load owns: artifact validation, rebuild generation creation, rebuild realtime tables, rebuild catch-up consumer creation and `Seek(artifact_checkpoint)`, catch-up polling/apply/commit, per-shard readiness tracking, guarded per-shard cutover, cancellation, and cleanup.
- Start catch-up from the artifact checkpoint stored per topic/partition. Cutover requires the rebuild consumer to have processed through the documented safe positions for all partitions that can produce keys for the shard. Unless the controller confirms a partition-to-shard routing contract, assume every topic/partition can affect every shard.
- Capture the cutover safe position after the artifact is accepted and the rebuild consumer is started. A simple acceptable v1 rule is: for each relevant partition, require committed next offset to be greater than or equal to the high watermark observed after rebuild start; document and test this exact rule. If W05 exposes different progress semantics, align with W05 rather than inventing another watermark type.
- Cutover publishes a new `ShardState` for the shard using the mmap-backed full snapshot plus the rebuild realtime delta for that generation. Do not mutate old generation state; old rows remain valid through existing shared ownership.
- Cancellation should be best-effort before cutover and fail-closed during cutover: no shard should publish unless all its guards pass. If cancellation arrives after some shards already cut over, report `kCancelled` only if no more work will proceed and the partial-cutover state is accurately reflected; otherwise prefer terminal failure with explicit partial state.
- Fail closed on malformed artifacts, checksum failures, hash/shard mismatches, source-progress gaps, consumer seek/poll/apply/commit failures, invalid cross-partition same-key ordering, schema/layout decode failures, and prewarm failures. Existing active shards must continue serving the previous generation.

### Minimal Executable Breakdown
- First add artifact-format parser tests and implementation: valid tiny artifact, bad magic/version, overlapping/out-of-bounds sections, checksum failure, shard/hash mismatch metadata, and per-partition checkpoint decoding.
- Then add the test artifact writer and Bazel wiring so unit and future integration tests can generate tiny real artifacts instead of handwritten mocks.
- Next add mmap snapshot backing tests and implementation: maps a tiny artifact, rejects invalid paths, validates lifetime after view creation, serves rows through `FullSnapshotView`, and marks prewarm complete only after page-touch traversal.
- Next extend `LoadState`/`LoadRequest` as needed and keep the smoke invalid-path assertion passing.
- Then add async-load unit tests with fake consumer/update applier seams: pending/running/failed/succeeded state transitions, cancellation before publish, catch-up starts from artifact checkpoint, cutover waits for loaded+prewarmed+safe progress, partial readiness does not publish unready shards, and schema/layout cutover keeps old generation reads valid until publish.
- Finally add focused integration or smoke coverage only where unit tests cannot prove behavior: end-to-end tiny artifact load through `ForwardIndex::LoadAsync`, read-before-cutover remains old/missing, read-after-cutover sees artifact rows, and invalid-path smoke remains terminal failed.

### Out Of Scope For W06
- W07 compaction and internal full rebase internals.
- Production artifact builder, remote artifact repository, object-store downloader, or non-file URI support beyond explicit fail-closed rejection.
- Public manual publish/cutover API.
- Public pluggable data-source abstraction; use W05 fake consumer seams for tests.
- Live Kafka broker integration tests.
- Optimizing prewarm scheduling, background thread pools, or benchmark-level performance tuning beyond correctness-oriented page touching.

### Focused Test Commands
- `bazel test //tests:artifact_format_test //tests:mmap_snapshot_backing_test //tests:async_load_test`
- `bazel test //tests:kv_index_smoke_test`
- `bazel test //tests:unit_tests //tests:integration_tests //tests:smoke_tests`
- If build wiring changes are non-trivial, also run `bazel build //...`.

### Acceptance Criteria
- Artifacts are fully validated before publish, including header, section directory, checksums, hash metadata, shard count, schema/layout metadata, and per-partition source progress.
- Full snapshot loading is mmap-backed and private/read-only; W06 does not silently copy the full artifact into owned vectors for production reads.
- A shard cannot cut over until its mmap-backed full snapshot has loaded, validated, and completed explicit page-touch prewarm.
- External load progress is observable through `GetLoadState`, including artifact id, shard progress, terminal error, and Kafka/source progress.
- Catch-up starts from artifact checkpoint positions and cutover is guarded by per-partition safe-position checks.
- Invalid artifacts, invalid paths, consumer failures, and guard failures are terminal and fail closed without corrupting the active generation.
- Schema/layout version changes take effect only through shard generation cutover; old generations continue decoding with the old layout until replaced.
- Existing smoke behavior for invalid artifact paths remains intact while adding richer status/progress fields.

## Implementation Log
- 2026-04-30: First coding pass reached a controller-checkpoint `NEEDS_CONTEXT` while still making progress. Artifact-format TDD slice is green: added `src/core/artifact_format.{h,cc}`, `tests/test_support/artifact_writer.{h,cc}`, `tests/unit/artifact_format_test.cc`, and Bazel wiring; `bazel test //tests:artifact_format_test` passed. Mmap TDD slice is in progress: `tests/unit/mmap_snapshot_backing_test.cc` was written and observed RED because `src/core/mmap_snapshot_backing.h` was missing, then `src/core/mmap_snapshot_backing.{h,cc}` plus a snapshot polymorphic seam were added but not yet compiled green. AsyncLoad slice and final task-doc implementation summary remain pending.
- 2026-04-30: Continuation coding pass returned `DONE_WITH_CONCERNS`: mmap slice and AsyncLoad tests/gates passed, adding richer `LoadState`, `src/core/async_load.{h,cc}`, and `ForwardIndex::LoadAsync` integration. Controller review found the concern is a W06 scope gap rather than a mere risk: current AsyncLoad is conservative and synchronous, does not start a real/internal catch-up consumer path, and fails unsafe progress instead of exercising catch-up poll/apply/commit. W06 requires a fix before independent verification.
- 2026-04-30: Continuation pass resumed at the requested mmap checkpoint. `bazel test //tests:mmap_snapshot_backing_test` was RED at compile time because `FullSnapshotView view(nullptr)` became ambiguous after adding the generic `SnapshotBacking` constructor; fixed the test to construct through an explicit `std::shared_ptr<const SnapshotBacking>{}` and observed the mmap test GREEN.
- 2026-04-30: AsyncLoad TDD slice: added `tests/unit/async_load_test.cc` and `//tests:async_load_test`; observed RED because `LoadState` lacked W06 progress fields and `LoadAsync` still used the bootstrap failure implementation. Added richer `LoadState` progress fields, `src/core/async_load.{h,cc}`, Bazel wiring, and `ForwardIndex::LoadAsync` integration. The W06 v1 path now validates local/file artifacts through `MmapSnapshotBacking`, prewarms every shard, exposes source/shard progress, fails closed for invalid paths/hash mismatches/unsafe source progress, and publishes shards only after validation plus prewarm plus safe-position guard. Observed `bazel test //tests:async_load_test` GREEN.
- 2026-04-30: W06 AsyncLoad gap fix TDD: added focused regression coverage in `tests/unit/async_load_test.cc` for non-blocking `ForwardIndex::LoadAsync`, cancellation before cutover, catch-up start from artifact checkpoint positions, catch-up progress to safe high-watermarks, and fail-closed poll/apply/commit failure. Observed RED with `bazel test //tests:async_load_test` because the internal catch-up seam did not exist. Implemented internal-only `AsyncCatchUpRunner`/`AsyncCatchUpRequest` and test factory in `src/core/async_load.{h,cc}`, moved long-running artifact validation/prewarm/catch-up/cutover into owned `ForwardIndex` worker threads with cancellation flags in `include/kv_index/forward_index.h` and `src/core/forward_index.cc`, preserved immediate terminal failure for definitely invalid local artifact paths, and kept production catch-up behind W05 `KafkaUpdateConsumer`/`UpdateCoordinator` concepts. Observed GREEN with `bazel test //tests:async_load_test`.
- 2026-04-30: W06 fix retry 1 RED/GREEN: added regressions for the independent verify findings in `tests/unit/async_load_test.cc`. RED was observed with `bazel test //tests:async_load_test`: the fake runner required post-start `CaptureSafeProgress()` plus one-step `PollApplyCommitOnce()`, while the old implementation only exposed stale all-in-one `PollApplyCommitUntilSafe()`. Implemented the internal-only catch-up seam in `src/core/async_load.{h,cc}` so external AsyncLoad always starts the catch-up runner for non-empty artifact source partitions, captures broker/source progress after `Start()`, and loops poll/apply/commit plus progress recapture until committed next offsets reach the highest observed safe high watermark. The `5/5` artifact plus post-start `8` high-watermark regression now waits in running state until catch-up reaches `8`, and the stale `5/8` then `8/8` regression now continues polling instead of publishing or failing at the stale artifact target.

## Verification Log
- 2026-04-30: `bazel test //tests:mmap_snapshot_backing_test` passed.
- 2026-04-30: `bazel test //tests:artifact_format_test` passed.
- 2026-04-30: `bazel test //tests:async_load_test` passed.
- 2026-04-30: `bazel test //tests:unit_tests //tests:integration_tests` passed.
- 2026-04-30: `bazel test //tests:smoke_tests` passed.
- 2026-04-30: `bazel test //tests:all_tests` passed.
- 2026-04-30: `bazel build //...` passed.
- 2026-04-30: W06 gap-fix verification: `bazel test //tests:async_load_test` passed after implementation.
- 2026-04-30: W06 gap-fix verification: `bazel test //tests:artifact_format_test //tests:mmap_snapshot_backing_test` passed.
- 2026-04-30: W06 gap-fix verification: `bazel test //tests:unit_tests //tests:integration_tests` passed.
- 2026-04-30: W06 gap-fix verification: `bazel test //tests:smoke_tests` passed.
- 2026-04-30: W06 gap-fix verification: `bazel test //tests:all_tests` passed.
- 2026-04-30: W06 gap-fix note: `bazel build //...` was not rerun in this pass because build wiring was not changed materially; the required test suites rebuilt the affected C++ targets.
- 2026-04-30: Independent W06 verify agent reviewed the W06 plan, this task doc, targeted tracked diffs, and untracked W06 files. Commands run: `git status --short` showed W06 implementation files plus pre-existing W05/controller task-doc edits; `git diff -- ...` was inspected for tracked W06 files; untracked W06 source/test files were read directly; `git diff --check` passed with no whitespace errors. Focused and aggregate gates passed: `bazel test //tests:artifact_format_test`, `bazel test //tests:mmap_snapshot_backing_test`, `bazel test //tests:async_load_test`, `bazel test //tests:unit_tests //tests:integration_tests`, `bazel test //tests:smoke_tests`, `bazel test //tests:all_tests`, and `bazel build //...` all completed successfully, mostly from cache.
- 2026-04-30: Blocking finding: external AsyncLoad does not capture the cutover safe high watermark after the rebuild consumer starts. `src/core/async_load.cc:381` builds `safe_progress` only from artifact source progress, `src/core/async_load.cc:383` treats that artifact checkpoint/high-watermark pair as sufficient, and `src/core/async_load.cc:385` starts catch-up only when the artifact already has lag. If an artifact checkpoint equals its captured high watermark, W06 currently skips the catch-up runner entirely and can publish at `src/core/async_load.cc:452` without observing broker progress after rebuild start. This violates the W06 requirement that cutover be guarded by safe positions captured after artifact acceptance/rebuild-consumer startup, and it can publish a stale full snapshot without replaying updates produced after artifact capture.
- 2026-04-30: Blocking finding: when catch-up is started, the production runner can stop at the artifact high watermark instead of the observed high watermark. `src/core/async_load.cc:245` calls `MergeCatchUpProgress` and returns as soon as committed offsets reach `safe_entry.high_watermark`; `MergeCatchUpProgress` only checks `observed->committed_next_offset < safe_entry.high_watermark` at `src/core/async_load.cc:113`, even if `observed->high_watermark` is higher. The outer validation at `src/core/async_load.cc:440` then fail-closes instead of continuing poll/apply/commit to the observed safe high watermark. This does not satisfy the W06 catch-up requirement to poll/apply/commit to safe high-watermarks.
- 2026-04-30: Non-blocking risk: mmap prewarm touches offsets relative to each section (`src/core/mmap_snapshot_backing.cc:230`) rather than every OS page intersecting the section's absolute mapped address range. For unaligned sections that cross a page boundary before `page_size` bytes of section-relative length, this can miss an intersecting page while still marking prewarm ready at `src/core/mmap_snapshot_backing.cc:390`.
- 2026-04-30: Risk assessment: artifact parsing, mmap URI handling, read-only/private mapping, snapshot backing polymorphism, load-state observability, cancellation hooks, and fail-closed paths have useful coverage, and all required commands passed. The cutover safe-position bug is a data-freshness/correctness blocker for the external AsyncLoad contract, so tests passing is not sufficient for W06 acceptance.
- 2026-04-30: Final conclusion: fail
- 2026-04-30: W06 fix retry 1 verification: initial RED `bazel test //tests:async_load_test` failed to build because the old `AsyncCatchUpRunner` seam lacked `CaptureSafeProgress()` and `PollApplyCommitOnce()`, matching the missing post-start safe-position modeling. After implementation, `bazel test //tests:async_load_test` passed.
- 2026-04-30: W06 fix retry 1 verification: `bazel test //tests:artifact_format_test //tests:mmap_snapshot_backing_test` passed.
- 2026-04-30: W06 fix retry 1 verification: `bazel test //tests:unit_tests //tests:integration_tests` passed.
- 2026-04-30: W06 fix retry 1 verification: `bazel test //tests:smoke_tests` passed.
- 2026-04-30: W06 fix retry 1 verification: `bazel test //tests:all_tests` passed.
- 2026-04-30: W06 fix retry 1 verification: `bazel build //...` passed.
- 2026-04-30: W06 fix retry 1 risk note: production catch-up now uses repeated W05 `KafkaUpdateConsumer::Progress()` observations around `UpdateCoordinator::PollApplyCommitOnce()`, but live broker behavior remains unexercised in W06. The prior mmap page-intersection prewarm risk remains unchanged and out of scope for this focused retry.
- 2026-04-30: Independent W06 verify retry 1 reviewed the prior independent fail findings, the fix retry 1 log, the W06 plan section, tracked W06 diffs, and untracked W06 source/test files. Required commands run: `git status --short` showed the existing shared W06/W05/controller edits plus untracked W06 files; `git diff --check` passed; targeted diffs and direct file reads covered `src/core/async_load.{h,cc}`, `src/core/forward_index.cc`, `include/kv_index/forward_index.h`, `include/kv_index/types.h`, `src/core/artifact_format.{h,cc}`, `src/core/mmap_snapshot_backing.{h,cc}`, `src/core/snapshot.{h,cc}`, `tests/unit/async_load_test.cc`, artifact/mmap tests, and Bazel wiring.
- 2026-04-30: Independent W06 verify retry 1 findings: the prior safe-position blockers are resolved. `RunExternalArtifactLoad` now always creates and starts the catch-up runner for non-empty artifact source progress, then captures post-start safe progress through `CaptureSafeProgress()` before cutover. The loop refreshes observed progress after every `PollApplyCommitOnce()` and preserves the maximum observed high watermark, so stale artifact high-watermark targets no longer permit publish or force an early fail-closed result. Regression tests cover artifact `5/5` with post-start observed `5/8`, and stale `5/8` observations that require more than one poll/apply/commit step before cutover.
- 2026-04-30: Independent W06 verify retry 1 production-runner review: `ProductionCatchUpRunner::Start()` seeks the W05 `KafkaUpdateConsumer` to the artifact checkpoint and builds a rebuild `UpdateCoordinator`; `CaptureSafeProgress()` delegates to `KafkaUpdateConsumer::Progress()`; `PollApplyCommitOnce()` checks cancellation before delegating to `UpdateCoordinator::PollApplyCommitOnce()`. Consumer creation/seek/progress/poll/apply/commit failures remain terminal fail-closed paths. The catch-up seam is still internal/test-only under `src/core/async_load.{h,cc}` and no public data-source abstraction was added.
- 2026-04-30: Independent W06 verify retry 1 broader assessment: artifact validation, local/file URI rejection, mmap private read-only loading, explicit prewarm before cutover, richer `LoadState`, cancellation hooks, guarded per-shard publish, old-generation lifetime through shared snapshot ownership, and worker-thread ownership/joining were reviewed with no blocking W06 issues found. No W07 compaction/full-rebase scope creep was observed.
- 2026-04-30: Independent W06 verify retry 1 test results: `bazel test //tests:async_load_test` passed; `bazel test //tests:artifact_format_test //tests:mmap_snapshot_backing_test` passed; `bazel test //tests:unit_tests //tests:integration_tests` passed with 19/19 tests passing; `bazel test //tests:smoke_tests` passed; `bazel test //tests:all_tests` passed with 20/20 tests passing; `bazel build //...` passed.
- 2026-04-30: Independent W06 verify retry 1 risk assessment: live Kafka broker behavior remains unexercised in W06, including exact production `Progress()` behavior immediately after `Seek()`. The previously noted mmap prewarm page-intersection concern remains a non-blocking risk. These do not block W06 retry acceptance because the required internal/production seams and regression behavior now satisfy the W06 contract under available tests.
- 2026-04-30: Independent W06 verify retry 1 final conclusion: pass

## Decisions
- 2026-04-30: W05 completed in commit `1935434`; W06 selected as the next active task.
- 2026-04-30: W06 planning is ready for implementation. The coding agent should keep artifact parsing, mmap backing, and external async-load state in one implementation slice because cutover safety depends on their shared source-progress contract.
- 2026-04-30: Controller authorized the W06 scope gaps identified by planning: root `BUILD.bazel`, `tests/BUILD.bazel`, `include/kv_index/types.h`, and `src/core/snapshot.{h,cc}` may be modified as needed for W06 artifact/mmap/load-state wiring. Do not modify `MODULE.bazel`.
- 2026-04-30: V1 cutover safety must assume every Kafka topic/partition can affect every shard unless a stricter routing contract already exists in W05 APIs. This conservative rule means a shard cutover waits for all artifact source partitions to reach safe positions.
- 2026-04-30: V1 artifact URIs support only plain local paths and `file://` local paths. Reject remote and unknown schemes fail-closed.
- 2026-04-30: V1 checksum algorithm is `fnv1a64` with an explicit checksum algorithm id in the artifact header. It is a deterministic corruption guard, not a cryptographic authenticity guarantee.

## Open Issues
- Resolved in fix retry 1: AsyncLoad now captures cutover safe high-watermarks after artifact acceptance and rebuild consumer startup, including artifacts whose checkpoint equals their artifact-captured high watermark.
- Resolved in fix retry 1: catch-up now continues poll/apply/commit until committed next offsets reach the observed post-start safe high-watermarks instead of stopping at the artifact high watermark or fail-closing on a stale target.
- Remaining risk: production Kafka catch-up is wired behind W05 `KafkaUpdateConsumer`/`UpdateCoordinator` and fail-closes when no Kafka config is available, but it has not been exercised against a live broker in W06.
- Remaining risk: `ForwardIndex` now joins owned load worker threads in the destructor; internal test catch-up runners must not block forever without observing cancellation or being released.

## Next Step
- Completed in commit `f76fb14`; proceed to W07.
