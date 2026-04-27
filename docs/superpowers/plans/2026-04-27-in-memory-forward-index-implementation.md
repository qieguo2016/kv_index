# In-Memory Forward Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the C++20 embedded in-memory forward index described in `docs/superpowers/specs/2026-04-26-in-memory-forward-index-design.md`, with sharded single-key snapshot reads, runtime schema/layout support, mmap/owned immutable snapshots, realtime whole-row Kafka upserts, delta compaction, internal full rebase, and external async full load orchestration.

**Architecture:** Implement the system in layers: schema and row encoding first, immutable snapshot reads second, shard directory and public read API third, then realtime delta publication and generation switching. Kafka consumer concerns live in a dedicated `KafkaUpdateConsumer` module that owns poll/seek/lag/commit behavior but contains no schema, generation, realtime delta, or cutover logic. Background workflows share one generation-switch coordinator so delta compaction, internal full rebase, and external async load keep the read path fixed as `realtime_delta -> compact_delta -> full_snapshot`.

**Tech Stack:** C++20, CMake, GoogleTest, Abseil, standard atomics, POSIX mmap for external artifact backing, Kafka consumer adapter boundary with fake Kafka client tests.

---

## Source Spec

- `docs/superpowers/specs/2026-04-26-in-memory-forward-index-design.md`
- Chinese mirror: `docs/superpowers/specs/2026-04-26-in-memory-forward-index-design.zh.md`

The spec files are the behavior authority. This plan intentionally creates implementation files from scratch because the repository currently contains only design documentation.

## Parallelization Model

The tasks are designed for multi-agent execution after the first foundation tasks land. Agents should claim whole tasks, not individual files inside a task. Do not edit files owned by another active task unless that task is already merged.

Recommended waves:

```text
Wave 0:
  Task 1

Wave 1:
  Task 2

Wave 2:
  Task 3
  Task 4

Wave 3:
  Task 5

Wave 4:
  Task 6

Wave 5:
  Task 7

Wave 6:
  Task 8

Wave 7:
  Task 9

Wave 8:
  Task 10

Wave 9:
  Task 11

Wave 10:
  Task 12

Wave 11:
  Task 13

Wave 12:
  Task 14

Wave 13:
  Task 15
```

Dependency graph:

```text
1 bootstrap -> 2 schema/layout

2 schema/layout -> 3 row storage/decoder
2 schema/layout -> 4 frozen index
2 schema/layout -> 14 observability/status

3 row storage/decoder + 4 frozen index -> 5 immutable snapshot

5 immutable snapshot -> 6 shard directory/read API
5 immutable snapshot -> 11 artifact format/mmap loader

6 shard directory/read API -> 7 realtime delta
7 realtime delta -> 8 source-position ordering
8 source-position ordering -> 9 delta compaction
9 delta compaction -> 10 full rebase

8 source-position ordering -> 12 kafka consumer module

11 artifact format/mmap loader + 8 source-position ordering + 12 kafka consumer module -> 13 async load coordinator

feature area under test -> 15 integration/performance tests

Task 5 depends on both Task 3 and Task 4.
Task 15 depends on the feature area being tested. It can grow incrementally after each wave.
```

## File Map

Create these production directories:

- `include/kv_index/`: public API headers only.
- `src/schema/`: runtime schema, field metadata, compiled row layout, field accessors.
- `src/row/`: row slot encoding, row arenas, payload pools, row decoder, `Row` implementation.
- `src/snapshot/`: frozen primary-key index, immutable snapshot view, mmap/owned backing, snapshot builder.
- `src/realtime/`: fixed-capacity realtime atomic hash map, realtime row storage, source-position ordered upserts.
- `src/core/`: shard state, shard directory, public `ForwardIndex`, generation switch primitives.
- `src/kafka/`: Kafka-specific consumer types, fakeable Kafka client boundary, `KafkaUpdateConsumer` poll/seek/lag/commit module.
- `src/update/`: delta compaction, internal full rebase, external async load coordinator.
- `src/artifact/`: artifact binary format, section directory, checksums, mmap loader.
- `src/observability/`: counters, load state, per-shard status, error reporting.
- `tests/`: unit and integration tests following the same module split.

Create these support files:

- `CMakeLists.txt`
- `cmake/Dependencies.cmake`
- `cmake/Sanitizers.cmake`
- `.clang-format`
- `.gitignore`
- `README.md`

## Global Rules For All Tasks

- Use TDD for each behavior change: write the focused failing test first, run it, implement minimal code, run it again.
- Keep commits small. Each task should produce one or more commits with one Codex trailer:

```text
Co-Authored-By: Codex <noreply@openai.com>
```

- Do not use `absl::Hash` for shard assignment or persisted indexes. Use the versioned stable hash helper introduced in Task 2.
- Public query APIs must not expose `ShardState`.
- Delta rows are complete row versions. Never merge fields from full/compact/realtime layers on read.
- V1 only supports Kafka as the live update source. Do not introduce a generic data-source plugin interface.
- Keep Kafka consumer mechanics in `src/kafka/`; it may poll, seek, report lag/progress, and commit offsets, but it must not decode schema rows, choose generations, write realtime delta, or perform shard cutover.
- All generation publication uses release-store; readers use acquire-load.
- All loader and update workflow failures fail closed and leave current serving shards active.

---

## Task 1: Build And Test Bootstrap

**Dependencies:** none

**Agent ownership:** repository build/test scaffolding only.

**Files:**

- Create: `CMakeLists.txt`
- Create: `cmake/Dependencies.cmake`
- Create: `cmake/Sanitizers.cmake`
- Create: `.clang-format`
- Create: `.gitignore`
- Create: `README.md`
- Create: `include/kv_index/version.h`
- Create: `src/version.cc`
- Create: `tests/version_test.cc`

**Deliverable:** A compiling C++20 project with GoogleTest and a placeholder library target named `kv_index`.

- [ ] Step 1: Write `tests/version_test.cc` with a test that includes `kv_index/version.h` and expects `kv_index::VersionString()` to be non-empty.
- [ ] Step 2: Run `cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug` and expect configuration to fail until targets exist.
- [ ] Step 3: Add CMake project, dependency wiring for Abseil and GoogleTest, `kv_index` library target, and `kv_index_tests` target.
- [ ] Step 4: Implement `kv_index::VersionString()` in `include/kv_index/version.h` and `src/version.cc`.
- [ ] Step 5: Run `cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug && cmake --build build && ctest --test-dir build --output-on-failure`.
- [ ] Step 6: Commit with message `chore: bootstrap cxx build`.

**Acceptance criteria:**

- `ctest --test-dir build --output-on-failure` passes.
- README documents the build/test command.

---

## Task 2: Runtime Schema, Layout, And Stable Hash

**Dependencies:** Task 1

**Agent ownership:** schema/layout/hash files.

**Files:**

- Create: `include/kv_index/types.h`
- Create: `include/kv_index/schema.h`
- Create: `src/schema/runtime_schema.h`
- Create: `src/schema/runtime_schema.cc`
- Create: `src/schema/compiled_layout.h`
- Create: `src/schema/compiled_layout.cc`
- Create: `src/schema/field_accessor.h`
- Create: `src/schema/stable_hash.h`
- Create: `src/schema/stable_hash.cc`
- Create: `tests/schema/runtime_schema_test.cc`
- Create: `tests/schema/compiled_layout_test.cc`
- Create: `tests/schema/stable_hash_test.cc`

**Deliverable:** Runtime schema validation, compiled fixed row layout, typed field accessors, and stable shard hash.

- [ ] Step 1: Test that adding fields creates stable `FieldId` entries and deleting a field marks it deleted without removing metadata.
- [ ] Step 2: Test that reusing a deleted field ID or changing an existing field type is rejected.
- [ ] Step 3: Test that `CompiledRowLayout` produces 8-byte-aligned slot sizes, presence bitmap offsets, fixed-area offsets, and 16-byte ref-area offsets.
- [ ] Step 4: Test that `StableHash64(primary_key, seed, version)` is deterministic and `ShardFor` rejects non-power-of-two shard counts.
- [ ] Step 5: Implement `FieldId`, `SchemaVersion`, `PhysicalType`, `FieldStatus`, `FieldSpec`, `RuntimeSchema`, `CompiledRowLayout`, `FieldAccessor<T>`, and `StableHash64`.
- [ ] Step 6: Run `ctest --test-dir build --output-on-failure -R "schema|stable_hash"`.
- [ ] Step 7: Commit with message `feat: add runtime schema and layout`.

**Acceptance criteria:**

- Layout rejects unsupported type changes.
- Accessors carry `schema_version`, `layout_fingerprint`, field ID, physical type, list flag, and offsets.
- `ShardFor` uses `hash & (shard_count - 1)`.

---

## Task 3: Row Encoding, Payload Pools, And Row Decoder

**Dependencies:** Task 2

**Agent ownership:** row storage and decoder files.

**Files:**

- Create: `include/kv_index/row.h`
- Create: `src/row/value_ref.h`
- Create: `src/row/row_arena.h`
- Create: `src/row/row_arena.cc`
- Create: `src/row/payload_pool.h`
- Create: `src/row/payload_pool.cc`
- Create: `src/row/row_encoder.h`
- Create: `src/row/row_encoder.cc`
- Create: `src/row/row_decoder.h`
- Create: `src/row/row_decoder.cc`
- Create: `tests/row/row_encoding_test.cc`
- Create: `tests/row/field_accessor_test.cc`

**Deliverable:** Rows can be encoded into fixed-size slots and decoded through field ID or compatible field accessor.

- [ ] Step 1: Test scalar encode/decode for `int8`, `int32`, `int64`, `uint64`, `bool`, and `string`.
- [ ] Step 2: Test list encode/decode for all V1 list types, including empty present list versus missing field.
- [ ] Step 3: Test that deleted/missing fields report absent without reading storage.
- [ ] Step 4: Test that accessor layout mismatch fails observably and does not read by stale offset.
- [ ] Step 5: Implement `ValueRef16`, append-only row arena, append-only string/list payload pool, encoder, decoder, `Row`, and list/string view types.
- [ ] Step 6: Run `ctest --test-dir build --output-on-failure -R "row|field_accessor"`.
- [ ] Step 7: Commit with message `feat: add row encoding and decoding`.

**Acceptance criteria:**

- Scalar reads copy values out.
- String/list views are backed by a pin owned by `Row`.
- No field read falls back to name lookup after accessor mismatch.

---

## Task 4: Frozen Primary-Key Index

**Dependencies:** Task 2

**Agent ownership:** frozen index files only.

**Files:**

- Create: `src/snapshot/frozen_primary_key_index.h`
- Create: `src/snapshot/frozen_primary_key_index.cc`
- Create: `src/snapshot/frozen_primary_key_index_builder.h`
- Create: `src/snapshot/frozen_primary_key_index_builder.cc`
- Create: `tests/snapshot/frozen_primary_key_index_test.cc`

**Deliverable:** Immutable SwissTable-style primary-key index view and owned builder.

- [ ] Step 1: Test exact-key lookup returns row offsets for existing keys and miss for absent keys.
- [ ] Step 2: Test duplicate keys are rejected during build.
- [ ] Step 3: Test serialized owned bytes can be reopened as `FrozenPrimaryKeyIndexView`.
- [ ] Step 4: Test hash seed/version mismatch validation fails.
- [ ] Step 5: Implement header validation, control bytes, key array, row-offset array, builder, and lookup.
- [ ] Step 6: Run `ctest --test-dir build --output-on-failure -R frozen_primary_key_index`.
- [ ] Step 7: Commit with message `feat: add frozen primary key index`.

**Acceptance criteria:**

- Capacity is rounded to a power of two from `row_count / 0.875`.
- No tombstones exist in immutable snapshots.
- Lookup does not depend on `absl::flat_hash_map` at serving time.

---

## Task 5: Immutable Snapshot View And Owned Snapshot Builder

**Dependencies:** Task 3, Task 4

**Agent ownership:** immutable snapshot view and owned backing.

**Files:**

- Create: `src/snapshot/snapshot_backing.h`
- Create: `src/snapshot/immutable_row_snapshot_view.h`
- Create: `src/snapshot/immutable_row_snapshot_view.cc`
- Create: `src/snapshot/snapshot_builder.h`
- Create: `src/snapshot/snapshot_builder.cc`
- Create: `src/snapshot/full_snapshot_view.h`
- Create: `src/snapshot/compact_delta_snapshot.h`
- Create: `tests/snapshot/immutable_row_snapshot_view_test.cc`
- Create: `tests/snapshot/snapshot_builder_test.cc`

**Deliverable:** Full and compact snapshots share one immutable lookup/decode implementation.

- [ ] Step 1: Test lookup/decode from an owned full snapshot.
- [ ] Step 2: Test lookup/decode from an owned compact delta snapshot uses identical decode logic.
- [ ] Step 3: Test compact snapshot row overrides full row when both are queried through higher-level layer selection later.
- [ ] Step 4: Implement `OwnedSnapshotBacking`, `SnapshotBackingRef`, `ImmutableRowSnapshotView`, `SnapshotBuilder`, `FullSnapshotView`, and `CompactDeltaSnapshot`.
- [ ] Step 5: Run `ctest --test-dir build --output-on-failure -R "immutable_row_snapshot|snapshot_builder"`.
- [ ] Step 6: Commit with message `feat: add immutable row snapshots`.

**Acceptance criteria:**

- Snapshot views do not own build-time temporary maps.
- `Row` pins backing memory for its lifetime.
- Full and compact snapshots expose the same read interface.

---

## Task 6: Shard Directory And Public Read API

**Dependencies:** Task 5

**Agent ownership:** core read API files.

**Files:**

- Create: `include/kv_index/forward_index.h`
- Create: `src/core/shard_state.h`
- Create: `src/core/shard_state.cc`
- Create: `src/core/shard_directory.h`
- Create: `src/core/shard_directory.cc`
- Create: `src/core/forward_index.cc`
- Create: `tests/core/forward_index_read_test.cc`
- Create: `tests/core/shard_directory_test.cc`

**Deliverable:** `ForwardIndex::Get` and `MGet` work against published immutable shard states.

- [ ] Step 1: Test `Get` returns full rows from full shard snapshots.
- [ ] Step 2: Test `MGet` returns results in input-key order across multiple shards.
- [ ] Step 3: Test per-shard publication switches one shard without affecting another shard.
- [ ] Step 4: Implement `ShardState`, `ShardDirectory`, release-store publication, acquire-load reader pinning, `ForwardIndex::Get`, and `ForwardIndex::MGet`.
- [ ] Step 5: Run `ctest --test-dir build --output-on-failure -R "forward_index_read|shard_directory"`.
- [ ] Step 6: Commit with message `feat: add sharded read api`.

**Acceptance criteria:**

- Public API does not expose `ShardState`.
- `MGet` preserves input order.
- Read path order is ready for `realtime_delta -> compact_delta -> full_snapshot`, even if realtime is initially empty.

---

## Task 7: Realtime Atomic Delta Table

**Dependencies:** Task 3, Task 6

**Agent ownership:** realtime table files and integration hooks in `ShardState`.

**Files:**

- Create: `src/realtime/source_position.h`
- Create: `src/realtime/row_ref.h`
- Create: `src/realtime/realtime_atomic_hash_map.h`
- Create: `src/realtime/realtime_atomic_hash_map.cc`
- Create: `src/realtime/realtime_delta_atomic_table.h`
- Create: `src/realtime/realtime_delta_atomic_table.cc`
- Modify: `src/core/shard_state.h`
- Modify: `src/core/shard_state.cc`
- Modify: `src/core/forward_index.cc`
- Create: `tests/realtime/realtime_atomic_hash_map_test.cc`
- Create: `tests/realtime/realtime_delta_atomic_table_test.cc`
- Create: `tests/core/forward_index_delta_test.cc`

**Deliverable:** Whole-row realtime upserts are visible immediately after local publication.

- [ ] Step 1: Test insert and lookup in fixed-capacity `RealtimeAtomicHashMap`.
- [ ] Step 2: Test concurrent reader does not observe a reserved/uninitialized slot.
- [ ] Step 3: Test realtime delta hit returns a complete row and does not merge with full snapshot fields.
- [ ] Step 4: Test realtime delta overrides compact and full layers.
- [ ] Step 5: Implement `RowRef`, `SourcePosition`, atomic slot state, append-only realtime row storage, and realtime table publication.
- [ ] Step 6: Wire `ShardState::Get` to check realtime before compact before full.
- [ ] Step 7: Run `ctest --test-dir build --output-on-failure -R "realtime|forward_index_delta"`.
- [ ] Step 8: Commit with message `feat: add realtime delta table`.

**Acceptance criteria:**

- Slot creation uses CAS.
- `latest_row` publication is release-store or CAS with release semantics.
- Reads acquire-load the latest row pointer.
- Capacity exhaustion fails closed.

---

## Task 8: Source-Position Ordering And Delta Publication Errors

**Dependencies:** Task 7

**Agent ownership:** source-position comparisons and publication status.

**Files:**

- Modify: `src/realtime/source_position.h`
- Modify: `src/realtime/realtime_atomic_hash_map.cc`
- Modify: `src/realtime/realtime_delta_atomic_table.cc`
- Create: `src/realtime/delta_publish_result.h`
- Create: `tests/realtime/source_position_test.cc`
- Create: `tests/realtime/delta_publish_ordering_test.cc`

**Deliverable:** Older replayed rows cannot overwrite newer rows inside one generation.

- [ ] Step 1: Test partition/offset ordering.
- [ ] Step 2: Test business version ordering if configured.
- [ ] Step 3: Test event-time plus tie-breaker ordering if configured.
- [ ] Step 4: Test older update returns a rejected/stale publication result and leaves latest row unchanged.
- [ ] Step 5: Implement comparable `SourcePosition` variants and integrate compare-before-CAS update logic.
- [ ] Step 6: Run `ctest --test-dir build --output-on-failure -R "source_position|delta_publish_ordering"`.
- [ ] Step 7: Commit with message `feat: enforce delta source ordering`.

**Acceptance criteria:**

- Invalid source-position metadata fails closed.
- Same-key concurrent writers cannot publish an older row over a newer row.

---

## Task 9: Delta Compaction Workflow

**Dependencies:** Task 5, Task 8

**Agent ownership:** compaction workflow and generation switch primitive.

**Files:**

- Create: `src/core/generation_switch.h`
- Create: `src/core/generation_switch.cc`
- Create: `src/update/delta_compaction.h`
- Create: `src/update/delta_compaction.cc`
- Modify: `src/core/shard_state.h`
- Modify: `src/core/shard_directory.h`
- Create: `tests/update/delta_compaction_test.cc`
- Create: `tests/core/generation_switch_test.cc`

**Deliverable:** Realtime delta can be sealed into a compact snapshot without changing lookup results.

- [ ] Step 1: Test generation switch publishes a new shard and old shard remains alive while a row pins it.
- [ ] Step 2: Test compaction scans sealed realtime rows newest-first and keeps only latest row per key.
- [ ] Step 3: Test old compact snapshot rows are included only when not overridden by sealed realtime.
- [ ] Step 4: Test upserts during the double-write window remain visible after cutover.
- [ ] Step 5: Implement compaction boundary capture, compact generation realtime table, double-write hook, `SnapshotBuilder` integration, catch-up check, and shard publication.
- [ ] Step 6: Run `ctest --test-dir build --output-on-failure -R "delta_compaction|generation_switch"`.
- [ ] Step 7: Commit with message `feat: add delta compaction workflow`.

**Acceptance criteria:**

- Read path remains `realtime_delta -> compact_delta -> full_snapshot`.
- Compaction failure leaves the original shard active.
- Old realtime and compact generations are retired only after reader pins drain.

---

## Task 10: Internal Full Rebase Workflow

**Dependencies:** Task 9

**Agent ownership:** rebase workflow only.

**Files:**

- Create: `src/update/full_rebase.h`
- Create: `src/update/full_rebase.cc`
- Create: `tests/update/full_rebase_test.cc`
- Modify: `src/core/generation_switch.h`
- Modify: `src/core/generation_switch.cc`

**Deliverable:** A shard can merge compact delta into a new full snapshot and reset compact delta.

- [ ] Step 1: Test rebase scans compact before full so compact rows override full rows.
- [ ] Step 2: Test rebase realtime delta remains highest-priority after cutover.
- [ ] Step 3: Test rebase requires realtime-before-switch data to be sealed into compact delta first.
- [ ] Step 4: Test external async load priority can cancel or delay rebase for the same shard.
- [ ] Step 5: Implement per-shard rebase state, immutable input pinning, owned full snapshot build, catch-up validation, and generation switch.
- [ ] Step 6: Run `ctest --test-dir build --output-on-failure -R full_rebase`.
- [ ] Step 7: Commit with message `feat: add internal full rebase`.

**Acceptance criteria:**

- Rebase does not change schema/layout.
- Only one rebase can run for a shard at a time.
- Failure leaves the original shard active.

---

## Task 11: Artifact Format And Mmap Loader

**Dependencies:** Task 5

**Agent ownership:** artifact format, mmap backing, validation.

**Files:**

- Create: `src/artifact/artifact_format.h`
- Create: `src/artifact/artifact_format.cc`
- Create: `src/artifact/checksum.h`
- Create: `src/artifact/checksum.cc`
- Create: `src/artifact/mmap_snapshot_backing.h`
- Create: `src/artifact/mmap_snapshot_backing.cc`
- Create: `src/artifact/artifact_loader.h`
- Create: `src/artifact/artifact_loader.cc`
- Create: `tests/artifact/artifact_loader_test.cc`
- Create: `tests/artifact/mmap_snapshot_backing_test.cc`

**Deliverable:** External full shard artifacts load as read-only mmap-backed immutable snapshots.

- [ ] Step 1: Test valid artifact header, metadata, section directory, schema, frozen index, row arena, and pools load into `ImmutableRowSnapshotView`.
- [ ] Step 2: Test checksum mismatch fails closed.
- [ ] Step 3: Test unsupported format version fails closed.
- [ ] Step 4: Test shard/hash/schema metadata mismatch fails closed.
- [ ] Step 5: Test non-little-endian marker mismatch fails closed.
- [ ] Step 6: Implement artifact structs, section validation, checksum verification, read-only private mmap backing, and loader.
- [ ] Step 7: Run `ctest --test-dir build --output-on-failure -R "artifact|mmap"`.
- [ ] Step 8: Commit with message `feat: add mmap artifact loader`.

**Acceptance criteria:**

- External full artifacts are not heap-loaded.
- Mmap sections remain valid while returned rows pin the snapshot backing.
- Loader errors do not mutate serving shards.

---

## Task 12: Kafka Consumer Module And Commit Boundary

**Dependencies:** Task 8

**Agent ownership:** Kafka consumer module and the SDK coordinator boundary that commits offsets after publication. Do not add generic source abstractions.

**Files:**

- Create: `src/kafka/kafka_types.h`
- Create: `src/kafka/kafka_client.h`
- Create: `src/kafka/fake_kafka_client.h`
- Create: `src/kafka/kafka_update_consumer.h`
- Create: `src/kafka/kafka_update_consumer.cc`
- Create: `src/update/update_applier.h`
- Create: `src/update/update_coordinator.h`
- Create: `src/update/update_coordinator.cc`
- Create: `tests/kafka/kafka_update_consumer_test.cc`
- Create: `tests/update/update_coordinator_test.cc`

**Deliverable:** Kafka poll/seek/lag/commit behavior is isolated from index logic, and the SDK update coordinator commits offsets only after local publication succeeds.

- [ ] Step 1: Test `KafkaUpdateConsumer::PollBatch` converts fake Kafka records into `KafkaUpsertMessage` with primary key, raw payload, topic/partition/offset, and metadata.
- [ ] Step 2: Test `KafkaUpdateConsumer::Seek` seeks each requested topic partition to the configured offset.
- [ ] Step 3: Test `KafkaUpdateConsumer::GetProgress` exposes per-partition consumed offset, committed offset, high watermark, and lag.
- [ ] Step 4: Test `KafkaUpdateConsumer::Commit` commits the provided per-partition offsets and does not decode row payloads.
- [ ] Step 5: Test `UpdateCoordinator` publishes every message in a batch through an injected `UpdateApplier` before committing the corresponding `KafkaCheckpoint`.
- [ ] Step 6: Test `UpdateCoordinator` does not commit offsets when any message publication fails.
- [ ] Step 7: Implement Kafka-specific types: `KafkaPartition`, `KafkaPosition`, `KafkaUpsertMessage`, `KafkaCheckpoint`, `KafkaProgress`, and fakeable `KafkaClient`.
- [ ] Step 8: Implement `KafkaUpdateConsumer` as a thin Kafka adapter over `KafkaClient`.
- [ ] Step 9: Implement `UpdateApplier` as an SDK-side interface and `UpdateCoordinator` as the poll/apply/commit loop.
- [ ] Step 10: Run `ctest --test-dir build --output-on-failure -R "kafka_update_consumer|update_coordinator"`.
- [ ] Step 11: Commit with message `feat: add kafka update consumer`.

**Acceptance criteria:**

- No file in `src/kafka/` includes schema, row encoder/decoder, shard state, realtime delta, or cutover headers.
- The only source supported by this module is Kafka; there is no generic `SourceReader`, `UpsertSourceReader`, or non-Kafka plugin interface.
- Offsets are committed only after the SDK reports local publication success.
- Tests use `FakeKafkaClient` or recorded `KafkaUpsertMessage` batches; they do not require a live Kafka cluster.

---

## Task 13: External AsyncLoad Coordinator

**Dependencies:** Task 8, Task 11, Task 12

**Agent ownership:** async load APIs and coordinator.

**Files:**

- Modify: `include/kv_index/forward_index.h`
- Create: `include/kv_index/load.h`
- Create: `src/update/async_load_coordinator.h`
- Create: `src/update/async_load_coordinator.cc`
- Create: `src/update/rebuild_catchup.h`
- Create: `src/update/rebuild_catchup.cc`
- Modify: `src/core/forward_index.cc`
- Modify: `src/update/update_coordinator.h`
- Modify: `src/update/update_coordinator.cc`
- Create: `tests/update/async_load_coordinator_test.cc`
- Create: `tests/core/forward_index_load_test.cc`

**Deliverable:** `ForwardIndex::LoadAsync`, `GetLoadState`, and `CancelLoad` orchestrate per-shard full artifact cutover using two independent `KafkaUpdateConsumer` streams.

- [ ] Step 1: Test `LoadAsync` creates one rebuild generation and rejects a second concurrent external rebuild.
- [ ] Step 2: Test per-shard cutover waits for every relevant Kafka partition lag to be below threshold and replay to pass the safe source position.
- [ ] Step 3: Test a shard not yet cut over still serves old full plus live realtime updates.
- [ ] Step 4: Test rebuild realtime delta accumulated before cutover is visible after shard switch.
- [ ] Step 5: Test schema-version switch: old generation ignores added/deleted-field changes while new generation encodes with new layout.
- [ ] Step 6: Test rebuild delta hard limits abort the load and preserve current serving shards.
- [ ] Step 7: Implement load state machine, cutover batch limit, Kafka lag gate, rebuild catch-up wiring, cancellation, and status reporting.
- [ ] Step 8: Run `ctest --test-dir build --output-on-failure -R "async_load|forward_index_load"`.
- [ ] Step 9: Commit with message `feat: add external async load coordinator`.

**Acceptance criteria:**

- Cutover defaults to one shard at a time.
- Config can allow two shards only when loaded batch bytes are within limit.
- Live Apply and Rebuild Catch-up are modeled as independent `KafkaUpdateConsumer` streams.
- Kafka consumer modules still do not contain schema decode, generation selection, realtime delta writes, or shard cutover.
- Failure or cancellation leaves active shards serving.

---

## Task 14: Observability, Thresholds, And Operational State

**Dependencies:** Task 2, Task 7; integrate incrementally with Tasks 9-13 when available.

**Agent ownership:** observability/status files and non-invasive metric hooks.

**Files:**

- Create: `include/kv_index/status.h`
- Create: `src/observability/counters.h`
- Create: `src/observability/counters.cc`
- Create: `src/observability/index_status.h`
- Create: `src/observability/index_status.cc`
- Create: `src/observability/thresholds.h`
- Create: `src/observability/thresholds.cc`
- Modify: `src/core/forward_index.cc`
- Modify: `src/realtime/realtime_delta_atomic_table.cc`
- Modify: `src/update/delta_compaction.cc`
- Modify: `src/update/full_rebase.cc`
- Modify: `src/update/async_load_coordinator.cc`
- Modify: `src/kafka/kafka_update_consumer.cc`
- Create: `tests/observability/index_status_test.cc`
- Create: `tests/observability/thresholds_test.cc`

**Deliverable:** Operational state exposes active artifact, per-shard generation, schema version, delta sizes, Kafka lag/load progress, and last error.

- [ ] Step 1: Test accessor mismatch increments `field_accessor_mismatch_total`.
- [ ] Step 2: Test default realtime compaction thresholds: load factor `0.60`, unique keys `5%`, row arena `64 MiB`, payload pool `64 MiB`.
- [ ] Step 3: Test full rebase thresholds: compact bytes `20%`, compact bytes `256 MiB`, compact unique keys `20%`.
- [ ] Step 4: Test rebuild delta limits: warning `5%`, global hard `10%`, per-shard hard `20%`.
- [ ] Step 5: Test Kafka lag and committed offsets are visible in status snapshots.
- [ ] Step 6: Implement lightweight in-process counters, status snapshots, threshold configs, Kafka progress hooks, and last-error propagation.
- [ ] Step 7: Run `ctest --test-dir build --output-on-failure -R "observability|thresholds"`.
- [ ] Step 8: Commit with message `feat: add index observability state`.

**Acceptance criteria:**

- Status reads do not block hot query path.
- Errors are observable and not silently converted to misses.
- Threshold defaults match the design spec.
- Kafka lag appears as Kafka lag, not as a generic source abstraction.

---

## Task 15: Integration, Concurrency, And Performance Tests

**Dependencies:** Incremental; final pass depends on Tasks 1-14.

**Agent ownership:** test-only files and benchmark support.

**Files:**

- Create: `tests/integration/single_key_snapshot_test.cc`
- Create: `tests/integration/schema_evolution_test.cc`
- Create: `tests/integration/update_workflows_test.cc`
- Create: `tests/integration/concurrent_read_write_test.cc`
- Create: `tests/perf/get_latency_benchmark.cc`
- Create: `tests/perf/mget_latency_benchmark.cc`
- Create: `tests/perf/realtime_delta_benchmark.cc`
- Modify: `CMakeLists.txt`

**Deliverable:** Cross-module tests cover the consistency and workflow guarantees from the design.

- [ ] Step 1: Test a lookup observes one complete row version and never a partially encoded row under concurrent upserts.
- [ ] Step 2: Test compact delta overrides full and realtime overrides compact.
- [ ] Step 3: Test business delete marker rows are returned as normal rows.
- [ ] Step 4: Test schema add/delete across async load generations.
- [ ] Step 5: Test delta compaction keeps lookup results unchanged.
- [ ] Step 6: Test full rebase keeps lookup results unchanged except compact is folded into full.
- [ ] Step 7: Test external async load shard-by-shard cutover with mixed old/new shards.
- [ ] Step 8: Test Kafka live apply and rebuild catch-up use independent `KafkaUpdateConsumer` instances and commit offsets only after local publication.
- [ ] Step 9: Add optional benchmarks for `Get`, `MGet`, immutable snapshot lookup/decode, realtime publication, compaction, and rebase.
- [ ] Step 10: Run `ctest --test-dir build --output-on-failure`.
- [ ] Step 11: Commit with message `test: add forward index integration coverage`.

**Acceptance criteria:**

- Full test suite passes.
- Benchmarks are optional in normal `ctest` unless explicitly enabled.
- Tests document remaining known limitations: no global multi-key snapshot isolation and no field-scan optimization.

---

## Merge And Coordination Notes

- Task 1 should merge before any other task starts implementation.
- Task 2 should merge before row, snapshot, realtime, or update workflow work starts.
- Task 3 and Task 4 can run in parallel after Task 2 because their write sets are disjoint.
- Task 5 starts only after Tasks 3 and 4 merge.
- Task 6 can start after Task 5 and should merge before realtime integration.
- Task 7 and Task 8 should stay sequential because Task 8 modifies publication semantics introduced by Task 7.
- Task 9 and Task 10 should stay sequential because rebase reuses generation switching and compaction sealing.
- Task 11 can run in parallel with Tasks 7-10 after Task 5.
- Task 12 can start after Task 8; it must not depend on artifact loading or shard cutover internals.
- Task 13 needs Task 8, Task 11, and Task 12 because async load combines replay ordering, artifact loading, and Kafka consumer progress.
- Task 14 can start early with schema/realtime metrics and then integrate with workflow and Kafka files as they land.
- Task 15 can add tests incrementally per wave, but the final integration pass should run after Task 14.

## Final Verification

Before declaring the implementation complete:

- [ ] Run `cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug`.
- [ ] Run `cmake --build build`.
- [ ] Run `ctest --test-dir build --output-on-failure`.
- [ ] Run sanitizer builds if enabled: `cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DKV_INDEX_ENABLE_ASAN=ON && cmake --build build-asan && ctest --test-dir build-asan --output-on-failure`.
- [ ] Review `docs/superpowers/specs/2026-04-26-in-memory-forward-index-design.md` and confirm every Testing Strategy bullet is either covered or explicitly deferred in documentation.
