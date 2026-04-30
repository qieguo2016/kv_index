# In-Memory Forward Index Module-Oriented Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan workstream-by-workstream. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the C++20 embedded in-memory forward index described by `docs/superpowers/specs/2026-04-26-in-memory-forward-index-design.zh.md`, including immutable full/compact snapshots, realtime delta updates, Kafka-driven update coordination, shard cutover, schema/layout handling, and fail-closed load/rebase/compaction flows.

**Architecture:** The public SDK remains `kv_index::ForwardIndex`, `kv_index::Row`, and shared type headers. Internally, serving reads pin a `std::shared_ptr<const ShardState>` from a `ShardDirectory`, then read `realtime_delta -> compact_delta -> full_snapshot`; all three layers decode rows through one shared row-materialization and row-decoding contract. Background update paths create new generations, double-write Kafka upserts when needed, build immutable snapshots from sealed inputs, and publish shard states with release/acquire semantics.

**Tech Stack:** C++20, Bazel only, standard library first, Abseil only when the standard library is insufficient or the design explicitly benefits from it, and `librdkafka` behind `KafkaUpdateConsumer` for production Kafka access. Tests use Bazel `cc_test`, `tests/test_support/test_macros.h`, fake Kafka clients, and small owned test snapshots rather than a real broker.

---

## Why This Revision Exists

The previous task list was too fine-grained. It split single modules across many tiny tasks, which made it easy to freeze partial abstractions too early and let later tasks contradict them.

This revision changes the execution unit from "small implementation step" to "module workstream":

- each workstream owns one coherent module boundary;
- interfaces are settled before dependent modules build on them;
- tests are organized around module contracts instead of isolated helper behavior;
- cross-module invariants are called out explicitly so later work cannot silently drift.

Use this document as the authoritative plan instead of the earlier ultra-fine task breakdown.

## Source Context

Design source:

- `docs/superpowers/specs/2026-04-26-in-memory-forward-index-design.zh.md`

Current repository state before implementation:

- `BUILD.bazel` has one public `cc_library(name = "kv_index")` with `src/core/forward_index.cc` and `src/version.cc`.
- `MODULE.bazel` only declares `rules_cc`; there is no Abseil or Kafka dependency yet.
- `include/kv_index/forward_index.h` defines `ForwardIndexOptions`, `ForwardIndex`, `IsPowerOfTwo`, and `StableHash64`.
- `include/kv_index/row.h` defines placeholder `Row`, `FieldAccessor<T>`, and `ListView<T>`.
- `include/kv_index/types.h` defines `ThresholdConfig`, `KafkaConsumerConfig`, `LoadRequest`, `LoadState`, and aliases.
- `src/core/forward_index.cc` validates power-of-two `shard_count`, implements stable shard assignment, returns miss for `Get`/`MGet`, and makes `LoadAsync` fail with a bootstrap message.
- Existing tests only cover defaults, invalid shard count rejection, stable shard assignment, empty miss shape, and placeholder load-state behavior.

Project rules that every worker must keep:

- Bazel is the only build/test entry point. Do not add CMake, Makefile, or alternate test runners.
- Use C++20.
- Prefer standard library equivalents before Abseil.
- First version requires `shard_count` to be a non-zero power of two; default remains `128`.
- Use stable, versioned `StableHash64(primary_key, hash_seed, hash_version)` for shard assignment and artifact metadata.
- Do not expose a business-facing manual publish API. Tests may use internal test peers or internal builders.
- Do not add an index-layer delete operation. Deletes are ordinary upsert rows with business fields such as `is_deleted`.
- Every git commit must include exactly one Codex trailer.

## Cross-Module Contracts

These contracts must be settled before downstream workstreams depend on them.

### 1. Schema And Row Contract

- `RuntimeSchema`, `CompiledRowLayout`, `FieldAccessor<T>`, and `Row` are one module contract and must be implemented together.
- `FieldSpec` must carry enough information to define nullability, deleted state, encoding policy, and default-value behavior for that schema version.
- Accessor mismatch must be observable and must never silently masquerade as "field missing".
- Deleted fields remain part of schema history but do not allocate active row storage and cannot create new accessors.

### 2. Row Materialization Contract

- The system needs one logical "encoded row materialization" shape shared by snapshot building, mmap-backed snapshots, compact snapshots, and realtime appends.
- That contract must represent:
  - fixed row slot bytes;
  - referenced payload bytes for arena-backed strings and lists;
  - dictionary/list-dictionary/element-dictionary data when the field encoding uses them.
- Do not let snapshot code and realtime code invent separate row encodings.

### 3. Immutable Snapshot Contract

- `FullSnapshotView` and `CompactDeltaSnapshot` must both wrap the same `ImmutableRowSnapshotView`.
- Their lookup behavior, row decode behavior, and row lifetime guarantees must be identical.
- Differences between them are metadata and backing ownership only.

### 4. Source Progress Contract

- `source_watermark`, `KafkaCheckpoint`, and cutover safety positions must be modeled as per-partition progress, not a single scalar.
- A shard cutover decision must be based on the partitions that can produce that shard's keys and on a documented safe-position rule.
- Cross-partition ordering for one key should be treated as invalid input unless a stronger upstream ordering contract is explicitly defined.

### 5. Error And Observability Contract

- Internal fail-closed paths use `Status` and `StatusOr<T>`.
- Public `ForwardIndex::Get` and `MGet` may keep `std::optional<Row>` return types, but corruption, accessor mismatch, and loader failures must update observable runtime status.
- A bad row ref, bad artifact section, schema incompatibility, or unsafe cutover must leave the current serving shard unchanged.

## File Structure To Converge On

Public or semi-public headers:

- `include/kv_index/status.h`
- `include/kv_index/types.h`
- `include/kv_index/schema.h`
- `include/kv_index/row.h`
- `include/kv_index/forward_index.h`
- `include/kv_index/kv_index.h`

Internal files:

- `src/core/hash.h`, `src/core/hash.cc`
- `src/core/byte_io.h`
- `src/core/schema.cc`
- `src/core/row_storage.h`, `src/core/row_storage.cc`
- `src/core/frozen_primary_key_index.h`, `src/core/frozen_primary_key_index.cc`
- `src/core/snapshot.h`, `src/core/snapshot.cc`
- `src/core/snapshot_builder.h`, `src/core/snapshot_builder.cc`
- `src/core/realtime_delta.h`, `src/core/realtime_delta.cc`
- `src/core/shard_state.h`, `src/core/shard_state.cc`
- `src/core/shard_directory.h`, `src/core/shard_directory.cc`
- `src/core/artifact_format.h`, `src/core/artifact_format.cc`
- `src/core/mmap_snapshot_backing.h`, `src/core/mmap_snapshot_backing.cc`
- `src/core/kafka_update_consumer.h`, `src/core/kafka_update_consumer.cc`
- `src/core/update_applier.h`, `src/core/update_applier.cc`
- `src/core/update_coordinator.h`, `src/core/update_coordinator.cc`
- `src/core/compaction.h`, `src/core/compaction.cc`
- `src/core/full_rebase.h`, `src/core/full_rebase.cc`
- `src/core/async_load.h`, `src/core/async_load.cc`
- `src/core/test_peer.h`

Primary test files:

- `tests/unit/status_test.cc`
- `tests/unit/schema_test.cc`
- `tests/unit/row_decoder_test.cc`
- `tests/unit/frozen_primary_key_index_test.cc`
- `tests/unit/snapshot_test.cc`
- `tests/unit/realtime_atomic_hash_map_test.cc`
- `tests/unit/realtime_delta_test.cc`
- `tests/unit/shard_state_test.cc`
- `tests/unit/shard_directory_test.cc`
- `tests/unit/artifact_format_test.cc`
- `tests/unit/mmap_snapshot_backing_test.cc`
- `tests/unit/kafka_update_consumer_test.cc`
- `tests/unit/update_applier_test.cc`
- `tests/unit/update_coordinator_test.cc`
- `tests/unit/compaction_test.cc`
- `tests/unit/full_rebase_test.cc`
- `tests/unit/async_load_test.cc`
- `tests/unit/runtime_status_test.cc`
- `tests/unit/lifetime_test.cc`
- `tests/integration/read_precedence_integration_test.cc`
- `tests/integration/async_load_cutover_integration_test.cc`
- `tests/integration/compaction_rebase_integration_test.cc`
- `tests/integration/schema_evolution_integration_test.cc`

## Workstream Dependency Graph

```text
W00 Foundation
  -> W01 Schema And Row Contract
  -> W02 Immutable Snapshot Module
  -> W03 Serving Read Path
  -> W04 Realtime Delta Module
  -> W05 Kafka Update Pipeline
  -> W06 Artifact And AsyncLoad
  -> W07 Compaction And Full Rebase
  -> W08 Observability, Integration, Benchmarks, Docs
```

Parallelism rules:

- `W01` and the early setup part of `W02` may overlap only after the row materialization contract is written down in code comments and tests.
- `W04` and `W05` should not diverge on row encoding or source-position semantics; keep one owner for those interfaces.
- `W06` cannot finalize cutover logic until `W05` defines the per-partition progress model.
- `W07` must reuse the generation-routing and row-materialization contracts from `W04` and `W05`, not fork them.

## Execution Rules

For every workstream below:

- [ ] Read the workstream context and its upstream contracts before editing.
- [ ] Write the contract tests first.
- [ ] Do not freeze a helper API until the whole module contract is represented in tests.
- [ ] Run focused tests after each meaningful sub-milestone.
- [ ] Run the nearest suite named in the workstream before committing.
- [ ] Commit only coherent module-level changes.

Default verification commands:

```bash
bazel test //tests:unit_tests
bazel test //tests:integration_tests
bazel test //tests:all_tests
```

Expected Bazel output includes `PASSED` for each target and `Build completed successfully`.

## Workstreams

### W00: Foundation And Build Layout

**Depends on:** none

**Purpose:** Stabilize the repo baseline and create the build/test structure needed for internal modules and white-box tests.

**Files:**

- Modify: `BUILD.bazel`
- Modify: `tests/BUILD.bazel`
- Create: `include/kv_index/status.h`
- Modify: `include/kv_index/kv_index.h`
- Create: `src/core/hash.h`
- Create: `src/core/hash.cc`
- Modify: `src/core/forward_index.cc`
- Create: `tests/unit/status_test.cc`
- Modify: `tests/unit/forward_index_test.cc`

- [ ] Run `bazel test //tests:all_tests` and confirm the bootstrap baseline.
- [ ] Split Bazel targets into public headers, public SDK, and internal implementation targets without exposing internal headers to SDK users.
- [ ] Add `Status` and `StatusOr<T>` as the common fail-closed primitive.
- [ ] Move stable hash helpers into a reusable internal module while preserving the public `StableHash64` declaration.
- [ ] Keep current public `ForwardIndex` smoke behavior unchanged except for internal refactoring.
- [ ] Run `bazel test //tests:all_tests`.
- [ ] Commit with a build/foundation-focused message.

**Exit criteria:**

- Internal tests can depend on an internal target.
- `Status` and `StatusOr<T>` exist and are covered by tests.
- Stable hash behavior remains deterministic and shard assignment is unchanged.

### W01: Schema, Layout, Row Semantics, And Row Materialization Contract

**Depends on:** W00

**Purpose:** Define one coherent module for runtime schema, compiled layout, field accessor semantics, row decode semantics, and the shared logical encoded-row shape.

**Files:**

- Create: `include/kv_index/schema.h`
- Modify: `include/kv_index/row.h`
- Create: `src/core/byte_io.h`
- Create: `src/core/schema.cc`
- Create: `src/core/row_storage.h`
- Create: `src/core/row_storage.cc`
- Create: `tests/unit/byte_io_test.cc`
- Create: `tests/unit/schema_test.cc`
- Create: `tests/unit/row_decoder_test.cc`
- Modify: `tests/BUILD.bazel`
- Modify: `BUILD.bazel`

- [ ] Add byte-order, alignment, and `ValueRef16` utilities.
- [ ] Implement `RuntimeSchema`, including active/deleted field tracking, encoding policy, nullability, and explicit default-value policy for a schema version.
- [ ] Implement `CompiledRowLayout` and lock down layout fingerprint, presence bitmap semantics, and row-slot alignment rules.
- [ ] Define `FieldAccessor<T>` creation and mismatch behavior together with `Row::Has`, scalar `Get`, string `Get`, and list accessors.
- [ ] Define one shared encoded-row materialization contract that can represent row slot bytes plus arena and dictionary payloads; use that shape in test builders immediately.
- [ ] Implement row decoding for scalar fields, string fields, scalar lists, and dictionary-backed string/list variants using the same module.
- [ ] Run `bazel test //tests:unit_tests`.
- [ ] Commit with a schema/row-module message.

**Exit criteria:**

- `Row`, `FieldAccessor<T>`, schema evolution rules, and row decode semantics are tested as one unit.
- Dictionary-backed and arena-backed field encodings are represented by the same row materialization contract.
- Accessor mismatch is observable and does not silently appear as field absence.

### W02: Immutable Snapshot Module

**Depends on:** W01

**Purpose:** Build the shared immutable storage path used by full snapshots and compact snapshots, including frozen primary-key index, owned snapshot backing, and shared lookup/decode view.

**Files:**

- Create: `src/core/frozen_primary_key_index.h`
- Create: `src/core/frozen_primary_key_index.cc`
- Create: `src/core/snapshot_builder.h`
- Create: `src/core/snapshot_builder.cc`
- Create: `src/core/snapshot.h`
- Create: `src/core/snapshot.cc`
- Create: `tests/unit/frozen_primary_key_index_test.cc`
- Create: `tests/unit/snapshot_test.cc`
- Modify: `tests/BUILD.bazel`
- Modify: `BUILD.bazel`

- [ ] Implement the frozen SwissTable-style primary-key index layout, validation, and lookup behavior.
- [ ] Make `SnapshotBuilder` consume the shared encoded-row materialization contract instead of raw row-slot bytes alone.
- [ ] Implement `OwnedSnapshotBacking` so it can hold frozen index bytes, row arena bytes, payload pools, and dictionary/list-dictionary data.
- [ ] Implement `ImmutableRowSnapshotView` and ensure it is the only lookup/decode path for immutable snapshots.
- [ ] Add `FullSnapshotView` and `CompactDeltaSnapshot` as thin wrappers that differ only in metadata and backing ownership.
- [ ] Run `bazel test //tests:unit_tests`.
- [ ] Commit with an immutable-snapshot-module message.

**Exit criteria:**

- A row encoded once through the shared materialization contract can be served from an owned immutable snapshot.
- Full and compact snapshot wrappers return identical decode behavior for the same backing.
- Row lifetime remains valid after local view objects are destroyed as long as the row pin is held.

### W03: Serving Read Path

**Depends on:** W02

**Purpose:** Connect immutable snapshots into shard-local serving state and then into the public `ForwardIndex` read API.

**Files:**

- Create: `src/core/shard_state.h`
- Create: `src/core/shard_state.cc`
- Create: `src/core/shard_directory.h`
- Create: `src/core/shard_directory.cc`
- Create: `src/core/test_peer.h`
- Modify: `include/kv_index/forward_index.h`
- Modify: `src/core/forward_index.cc`
- Create: `tests/unit/shard_state_test.cc`
- Create: `tests/unit/shard_directory_test.cc`
- Modify: `tests/unit/forward_index_test.cc`
- Modify: `tests/integration/forward_index_integration_test.cc`

- [ ] Implement `ShardState` precedence for `compact_delta -> full_snapshot` first, with a placeholder empty realtime layer.
- [ ] Implement `ShardDirectory` atomic publish/load semantics with `std::atomic<std::shared_ptr<const ShardState>>`.
- [ ] Wire public `ForwardIndex::Get` and `MGet` through `ShardDirectory`.
- [ ] Preserve input order and duplicate-key behavior for `MGet`.
- [ ] Use test peers for installing shard state in white-box tests; do not add any business-facing publish API.
- [ ] Run `bazel test //tests:all_tests`.
- [ ] Commit with a serving-read-path message.

**Exit criteria:**

- Public reads route through pinned shard state.
- `MGet` groups by shard internally but preserves caller-visible order.
- Compact rows override full rows and there is never partial field merging.

### W04: Realtime Delta Module

**Depends on:** W03

**Purpose:** Implement one coherent realtime delta module: source-position ordering, append-only row storage, atomic hash map, realtime table stats, and row lifetime rules.

**Files:**

- Modify: `include/kv_index/types.h`
- Create: `src/core/realtime_delta.h`
- Create: `src/core/realtime_delta.cc`
- Create: `tests/unit/source_position_test.cc`
- Create: `tests/unit/realtime_atomic_hash_map_test.cc`
- Create: `tests/unit/realtime_delta_test.cc`
- Modify: `tests/unit/shard_state_test.cc`

- [ ] Define `SourcePosition` and document one deterministic ordering policy; treat invalid same-key cross-partition ordering as fail-closed input unless a stronger upstream contract is present.
- [ ] Implement append-only realtime row storage using the shared encoded-row materialization contract.
- [ ] Implement `RealtimeAtomicHashMap` slot creation, existing-key update ordering, reserved-slot miss semantics, and capacity exhaustion behavior.
- [ ] Implement `RealtimeDeltaAtomicTable` by combining append-only storage and the atomic hash map.
- [ ] Add threshold helpers and stats for realtime compaction triggers.
- [ ] Extend `ShardState` precedence to `realtime_delta -> compact_delta -> full_snapshot`.
- [ ] Run `bazel test //tests:unit_tests`.
- [ ] Commit with a realtime-delta-module message.

**Exit criteria:**

- Readers can see either the old row or the new row, never a partial row.
- Reserved slots return realtime miss immediately.
- Older source positions never overwrite newer visible rows.

### W05: Kafka Update Pipeline

**Depends on:** W04

**Purpose:** Implement the Kafka-facing update path as one module covering message models, consumer interface, update application, coordinator commit semantics, and multi-generation routing.

**Files:**

- Modify: `include/kv_index/types.h`
- Create: `src/core/kafka_update_consumer.h`
- Create: `src/core/kafka_update_consumer.cc`
- Create: `src/core/update_applier.h`
- Create: `src/core/update_applier.cc`
- Create: `src/core/update_coordinator.h`
- Create: `src/core/update_coordinator.cc`
- Create: `tests/unit/kafka_update_consumer_test.cc`
- Create: `tests/unit/update_applier_test.cc`
- Create: `tests/unit/update_coordinator_test.cc`
- Modify: `tests/BUILD.bazel`

- [ ] Define Kafka partition, checkpoint, and progress models around per-partition positions instead of a single scalar watermark.
- [ ] Add a fake/test consumer seam without introducing a public pluggable data-source abstraction.
- [ ] Implement `UpdateApplier` so it validates payloads against the target generation schema/layout and writes to realtime only after full validation succeeds.
- [ ] Replace the placeholder test-only payload story with one documented production row payload decode boundary; do not leave "production parser later" as plan debt.
- [ ] Implement `UpdateCoordinator` batch apply and commit semantics: commit only after the whole batch is locally published.
- [ ] Implement multi-generation routing for active-generation plus rebuild/rebase/compaction targets without forking row encoding or source-position logic.
- [ ] Add `librdkafka` binding only after fake-consumer tests are green.
- [ ] Run `bazel test //tests:unit_tests`.
- [ ] Commit with a kafka-update-pipeline message.

**Exit criteria:**

- Kafka models, payload decode, apply semantics, and commit semantics are defined in one place.
- Multi-generation routing reuses the same row materialization and source-ordering logic.
- Tests do not require a live broker.

### W06: Artifact Format, Mmap Loading, And External AsyncLoad

**Depends on:** W05

**Purpose:** Implement the external full-artifact path, including artifact format, mmap loading, rebuild generation, per-partition catch-up, and guarded per-shard cutover.

**Files:**

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

- [ ] Define the sharded artifact header, section directory, checksum rules, hash metadata, and per-partition source watermark/checkpoint representation.
- [ ] Add a test artifact writer so integration tests use real tiny artifacts instead of hand-built mocks.
- [ ] Implement read-only private mmap loading and `LoadFullShardFromArtifact`.
- [ ] Implement shard prewarm for mmap-backed full snapshots by touching each page of the frozen index, row arena, string pools, and list pools before cutover; `madvise`-style hints may be auxiliary but are not sufficient as the readiness condition.
- [ ] Implement one external load state machine that owns rebuild generation creation, rebuild catch-up consumer startup, cutover guards, cancellation, and fail-closed behavior.
- [ ] Keep the current invalid-path smoke behavior while growing real load-state fields for artifact id, progress, errors, shard counts, and source progress.
- [ ] Reject shard-count or hash-function mismatches in v1.
- [ ] Guard per-shard cutover on loaded-and-prewarmed shard readiness plus documented per-partition catch-up and safe-position conditions.
- [ ] Add schema-version cutover behavior so old generations decode with old layout until cutover and rebuild generations decode with the new layout.
- [ ] Run `bazel test //tests:unit_tests //tests:integration_tests`.
- [ ] Commit with an async-load-module message.

**Exit criteria:**

- Full artifact loading is mmap-backed and fail-closed.
- A shard is not eligible for external cutover until its mmap-backed full snapshot has completed prewarm.
- External rebuild progress is modeled around per-partition progress and per-shard cutover state.
- Schema-version cutover does not require any public manual publish API.

### W07: Delta Compaction And Internal Full Rebase

**Depends on:** W06

**Purpose:** Implement both internal generation-maintenance workflows as one family of modules that reuse the same routing, row materialization, and lifetime contracts.

**Files:**

- Create: `src/core/compaction.h`
- Create: `src/core/compaction.cc`
- Create: `src/core/full_rebase.h`
- Create: `src/core/full_rebase.cc`
- Create: `tests/unit/compaction_test.cc`
- Create: `tests/unit/full_rebase_test.cc`
- Create: `tests/integration/compaction_rebase_integration_test.cc`

- [ ] Implement realtime boundary capture and sealed-row scanning for compaction.
- [ ] Build new compact snapshots as "sealed realtime overlay old compact" without scanning full snapshot during compaction.
- [ ] Implement compaction cutover by reusing generation routing from the update pipeline.
- [ ] Implement full rebase eligibility checks and conflict handling with external async load.
- [ ] Build rebased full snapshots as "compact overlay full" using the same immutable snapshot path as owned full snapshots.
- [ ] Implement rebase cutover and verify that updates published during rebase remain visible through rebase realtime.
- [ ] Run `bazel test //tests:unit_tests //tests:integration_tests`.
- [ ] Commit with a compaction-rebase message.

**Exit criteria:**

- Compaction and rebase both reuse existing generation-routing and row-materialization contracts.
- Old generations remain alive while rows from them are still pinned.
- External async load retains priority over internal full rebase.

### W08: Observability, Lifetime Verification, Integration, Benchmarks, And Docs

**Depends on:** W07

**Purpose:** Close the loop on runtime status, fail-closed paths, lifetime guarantees, end-to-end integration, benchmarks, and user-facing docs.

**Files:**

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

- [ ] Add runtime status structs for shard generation, schema version, artifact id, delta stats, accessor mismatch count, and last error.
- [ ] Harden cancellation, checksum failure, schema failure, and cutover-failure paths so serving shards stay unchanged on error.
- [ ] Verify lifetime pinning for old full, old compact, and old realtime rows after cutover.
- [ ] Add end-to-end integration coverage for read precedence, cross-shard `MGet`, async load windows, schema evolution, compaction, and rebase.
- [ ] Add focused non-blocking benchmarks for `Get`, `MGet`, snapshot decode, and realtime update cost.
- [ ] Update `README.md` and optional examples only after the implementation is actually present.
- [ ] Run `bazel test //tests:all_tests` and `bazel build //...`.
- [ ] Commit with an observability/docs/finalization message.

**Exit criteria:**

- Runtime status is sufficient to debug accessor mismatch, load failure, and cutover progress.
- Generation lifetime is proven by tests, not only by design intent.
- Public docs no longer describe implemented functionality as missing.

## Acceptance Checklist

- [ ] `ForwardIndexOptions` defaults match design: `shard_count = 128`, stable hash version remains explicit.
- [ ] `ForwardIndex` rejects non-power-of-two shard counts.
- [ ] `Get` reads one pinned shard state and returns a complete row or miss.
- [ ] `MGet` preserves input order across shards and duplicate keys.
- [ ] `Row` pins backing memory for string/list views and for immutable/realtime lifetime safety.
- [ ] Accessor mismatch is observable and not silently represented as field absence.
- [ ] Full and compact snapshots share `ImmutableRowSnapshotView` lookup/decode.
- [ ] Realtime delta lookup is lock-free on the read path and can return miss for reserved slots.
- [ ] Older `SourcePosition` never overwrites newer row data.
- [ ] Compact delta overlays realtime-sealed rows over old compact rows.
- [ ] Full rebase overlays compact rows over full rows without schema changes.
- [ ] External `AsyncLoad` can switch schema/layout by shard.
- [ ] Cutover is per-shard, fail-closed, and guarded by full-shard prewarm plus per-partition Kafka progress and safe-position rules.
- [ ] Kafka offsets commit only after all local publishes in the batch succeed.
- [ ] Tests do not require a live Kafka broker.
- [ ] `librdkafka` is encapsulated in `KafkaUpdateConsumer`.
- [ ] No CMake or Makefile is introduced.
- [ ] `bazel test //tests:all_tests` passes.
- [ ] `bazel build //...` passes.
