# In-Memory Forward Index Design

## Goal

Build an embedded C++20 library that serves low-latency in-memory forward lookups:

```text
uint64_t primary_key -> structured value
```

The component is optimized for high-concurrency read-heavy workloads. Kafka upserts must become visible to subsequent reads immediately after the active generation consumer for the target shard publishes them locally. Reads must still observe a complete row version and never a partially applied value.

The value schema is fixed per schema version but can be hot-loaded at runtime. Schema evolution only supports adding and deleting fields. Existing field types cannot change, and field IDs are never reused.

## Non-Goals

- The SDK does not deliver full artifacts to online machines.
- The SDK does not expose a manual business-facing publish API.
- The SDK does not provide index-level delete operations. Deletes are represented as normal upserted fields, such as `is_deleted`.
- The first version does not provide global multi-key snapshot isolation across all shards. The core semantic is single-key lookup consistency.
- The first version does not optimize for field-scan workloads. The common path is point lookup followed by reading all fields.

## Online API Sketch

```cpp
class ForwardIndex {
 public:
  std::optional<Row> Get(uint64_t primary_key) const;
  std::vector<std::optional<Row>> MGet(absl::Span<const uint64_t> primary_keys) const;

  LoadId LoadAsync(const LoadRequest& request);
  LoadState GetLoadState(LoadId id) const;
  bool CancelLoad(LoadId id);
};

class ShardState {
 public:
  uint32_t ShardId() const;
  uint64_t Generation() const;
  const RuntimeSchema& Schema() const;
  std::optional<Row> Get(uint64_t primary_key) const;
};

class Row {
 public:
  bool Has(FieldId field_id) const;

  template <typename T>
  std::optional<T> Get(FieldId field_id) const;

  template <typename T>
  std::optional<T> Get(FieldAccessor<T> accessor) const;

  template <typename T>
  ListView<T> GetList(FieldId field_id) const;
};
```

`ShardState` is an internal type; public query APIs do not expose `CurrentShard`. `Row` keeps the pinned shard state alive so references into row arenas and dictionary pools remain valid. `MGet` returns results in the same order as the input keys and groups keys by shard internally to avoid repeatedly acquiring the same shard pointer.

## Consistency Model

The index provides single-key snapshot semantics:

- A lookup observes one shard state.
- Within that shard, it sees one complete row version.
- A lookup never observes a partially encoded row.
- Different shards may switch to a new full artifact at different times during a full update.

This intentionally does not guarantee that a multi-key request across shards observes one global full version. That stronger guarantee would require a global manifest and would increase memory pressure during full updates.

## Core Architecture

The index is sharded by primary key. Each shard can be loaded, compacted, and switched independently.

```text
ForwardIndex
  -> ShardDirectory
       -> std::atomic<std::shared_ptr<const ShardState>>[N]

ShardState published container
  -> FullSnapshotView
  -> CompactDeltaSnapshot
  -> RealtimeDeltaAtomicTable mutable, read-optimized
  -> schema/layout handles
```

`ShardDirectory` is an array of independently published shard pointers. In C++20, each pointer is represented as `std::atomic<std::shared_ptr<const ShardState>>`. Publishing a shard uses release-store; readers use acquire-load to pin the shard state before lookup.

The read path only touches one shard:

```text
Get(primary_key)
  -> shard_id = ShardFor(primary_key)
  -> acquire current ShardState for shard_id
  -> realtime_delta.Get(primary_key)
  -> compact_delta.Get(primary_key)
  -> full_snapshot.Get(primary_key)
  -> return Row
```

Each Kafka upsert is a complete row. A delta hit returns a full value, so the read path never merges fields from base and delta.

## Runtime Schema

Schema is a runtime object and is included in every full artifact. Each shard state binds to one schema version and compiled row layout.

Fields are identified by stable `FieldId`, not only by name:

```text
FieldId -> name, type, repeated/list flag, nullable/default, encoding, status
```

Schema evolution rules:

- Adding a field creates a new field ID.
- Deleting a field marks the field tombstoned.
- Field IDs are never reused.
- Field type changes are rejected.
- A row can only be interpreted with the compiled layout for its schema version.

Clients should resolve field names to `FieldId` and `FieldAccessor<T>` outside the hot path. Accessors include schema-version checks and fail fast when used with an incompatible shard state.

## Realtime Incremental Updates

Kafka messages are whole-record upserts:

```text
primary_key -> complete structured row
```

There is no index-level delete operation. A delete is represented by normal fields in the upserted row.

The serving path uses a read-optimized mutable delta table:

```text
RealtimeDeltaAtomicTable
  -> fixed-capacity open-addressing hash table
  -> slot: atomic key state + primary_key + atomic RowRef*
  -> append-only row arena
```

Upsert publication:

```text
1. Decode and validate the Kafka row.
2. Encode the complete row into an append-only row arena.
3. Find the primary-key slot in the shard's realtime table, creating the slot with CAS when absent.
4. Release-store the new RowRef* into that slot.
5. Commit Kafka offset only after local publication succeeds.
```

Subsequent reads can see the new row immediately after step 4. Old row memory is not reclaimed inline. Replaced rows stay in the append-only arena until the realtime table generation is compacted and reader epochs prove no query can still reference the old row.

The realtime table avoids application-level read locks. Slot creation uses CAS on the slot key state. Reads use open-addressing probe plus atomic pointer load in the common path.

## Online Full Update

Full update is executed shard by shard to avoid holding two complete full datasets in memory.

```text
ActiveGeneration
  -> active ShardState[0..N-1]

RebuildGeneration
  -> artifact_id
  -> full_watermark W
  -> NewShard[0..N-1]
       -> new_full: unloaded / loaded
       -> RealtimeDeltaAtomicTable
       -> replay_progress
```

Two logical update streams are required:

```text
Live Apply
  -> consumes current Kafka stream
  -> writes immediately to ActiveGeneration shards
  -> remains the authoritative visible stream for shards that still point to ActiveGeneration

Rebuild Catch-up
  -> consumes from full_watermark W
  -> writes the same row format into RebuildGeneration shards
  -> becomes the authoritative visible stream for a shard after that shard switches to RebuildGeneration
```

The two streams are implemented as two independent Kafka consumers. During rebuild, both consumers keep running globally. Live Apply continues writing ActiveGeneration for every shard until the entire rebuild finishes. Rebuild Catch-up writes RebuildGeneration for every shard from watermark `W` until all shards have switched. After a shard switches, Rebuild Catch-up is the active generation consumer for that shard, while Live Apply's writes to the old ActiveGeneration shard are no longer serving-visible.

Per-shard cutover:

```text
1. Load new_full for shard_i from the sharded artifact.
2. Ensure RebuildGeneration shard_i has replayed through the latest source position published by Live Apply for shard_i.
3. Attach new_full_i to RebuildGeneration shard_i.
4. Atomically replace active_shards[i] with RebuildGeneration shard_i.
5. Keep both Kafka consumers running globally.
6. Release the old shard when readers drain.
```

At the moment shard_i switches:

```text
before: old_full_i + old_realtime_delta_i
after:  new_full_i + rebuild_realtime_delta_i
```

Shards that have not switched yet continue serving:

```text
old_full + old_realtime_delta
```

Their RebuildGeneration realtime deltas continue accumulating in the background until their own cutover.

After a shard switches, queries for that shard read RebuildGeneration. Live Apply continues consuming and writing ActiveGeneration globally until the rebuild completes, but those writes are no longer serving-visible for switched shards. Rebuild Catch-up is the serving-visible stream for switched shards and must stay caught up according to the configured lag threshold.

## Kafka Ordering Requirements

Kafka must be keyed by `primary_key` so that updates for the same key are ordered within one partition. Each message also carries a comparable source position.

Delta writes should preserve a comparable source position, such as:

```text
partition + offset
business_version
event_time + tie_breaker
```

This prevents an older replayed update from overwriting a newer update inside the same generation.

## Delta Compaction

Realtime delta grows with live updates. Each shard should compact independently:

```text
RealtimeDeltaAtomicTable
  -> scan latest RowRef per key
  -> build CompactDeltaSnapshot keyed by primary_key
  -> create a fresh empty RealtimeDeltaAtomicTable
  -> atomically publish new ShardState
```

`CompactDeltaSnapshot` stores complete updated row slots keyed by primary key for records changed since the full snapshot. `FullSnapshotView` remains read-only.

The read path remains fixed:

```text
realtime_delta -> compact_delta -> full_snapshot
```

If a shard's compact delta grows beyond configured thresholds, the component alerts and prioritizes that shard during the next full rebuild.

## Concurrency Model

- Query threads read one immutable shard state plus its read-optimized realtime delta.
- The realtime delta uses CAS slot creation and atomic row pointer publication for immediate visibility.
- Full rebuild loads and switches one shard at a time.
- Old shard states remain alive while any query holds a shared reference.
- Only one full rebuild generation should be active at a time.
- Per-shard cutover and rebuild catch-up must be coordinated by one update coordinator.

## Memory Model

Peak full-update memory is bounded by shard batch size rather than full dataset size:

```text
active full dataset
+ currently loaded new full shard batch
+ active realtime deltas
+ rebuild generation realtime deltas since watermark
+ compact deltas
```

To keep memory predictable:

- Use mmap-backed full shard artifacts where possible.
- Limit full cutover batch size.
- Track rebuild generation realtime delta size per shard.
- Abort rebuild if rebuild delta exceeds configured limits.
- Prefer frozen indexes for full shards to avoid rebuilding full-size heap hash maps.

## Error Handling

Load, replay, compaction, and shard cutover must fail closed. Current serving shards remain active if any step fails.

Failure examples:

- Artifact checksum mismatch.
- Unsupported format version.
- Schema type incompatibility.
- Corrupt row arena section.
- Corrupt dictionary section.
- Rebuild Catch-up cannot reach the safe cutover position.
- Delta row fails schema validation.
- Kafka ordering metadata is invalid.

Operational state should expose active artifact ID, per-shard generation, rebuild progress, Kafka lag, delta sizes, schema version, and last error.

## Full Artifact and Sharding

Offline full build produces a sharded artifact:

```text
IndexArtifact
  -> Header
  -> Metadata
  -> RuntimeSchema
  -> ShardDirectory
       -> ShardArtifact[0]
       -> ShardArtifact[1]
       -> ...
       -> ShardArtifact[N-1]
```

Each shard artifact contains:

```text
ShardArtifact
  -> shard_id
  -> row_count
  -> FrozenPrimaryKeyIndex
  -> RowArena
  -> StringPools
  -> ListPools
  -> Checksums
```

Header and metadata include:

```text
magic
format_version
artifact_id
schema_version
shard_count
build_time
source_watermark
section_directory
checksum
```

`source_watermark` is the preferred starting point for Kafka replay. `build_time + safety_buffer` is only a fallback when the upstream source cannot provide a stronger watermark.

The full snapshot should be mmap-friendly. Row arenas, string pools, list pools, and frozen primary-key indexes should be readable directly from the artifact where possible. This prevents full updates from requiring two complete heap-resident copies of the dataset.

## Primary Key Index

For full snapshots, the artifact stores an mmap-friendly frozen hash table:

```text
FrozenPrimaryKeyIndex: primary_key -> row_offset
```

`absl::flat_hash_map<uint64_t, RowOffset>` remains useful for compact and realtime delta snapshots, where heap overhead is bounded by recent updates instead of the whole dataset.

## Row Layout and Value Encoding

The primary storage is row-based because the dominant access pattern is fetching one record and reading all fields.

```text
FixedSizeRowSlot
  -> presence_bitmap
  -> fixed_area
  -> ref_area
```

All rows for the same compiled schema version use the same slot size. This makes row addressing simple and cache-friendly:

```text
row_address = row_base + row_id * row_slot_size
```

Scalar fields are stored inline in `fixed_area`. Strings and lists are not stored as variable-length inline payloads. They are represented by fixed-width references in `ref_area` and resolved through external pools and arenas.

String and list fields use field-level encoding policies:

- `inline_ref`: store a fixed-width small-value reference in the row.
- `dict`: store a dictionary ID in the row and the value in a typed pool.
- `arena`: store an offset and length into a variable-length arena.
- `list_dict`: deduplicate the whole list value.
- `element_dict`: deduplicate repeated list elements, especially repeated strings.

Encoding policy is configured per field. Offline builder may also choose defaults from Parquet statistics, but explicit configuration wins. Schema additions and deletions create a new compiled layout and may change `row_slot_size`; old shard states continue using their original layout.

## Testing Strategy

Core tests:

- `Get` returns full rows from full shard snapshots.
- `MGet` returns results in input-key order across multiple shards.
- Realtime delta update is visible immediately after local publication.
- Delta hit returns a whole row and never merges partial fields from full.
- Compact delta overrides full snapshot.
- Realtime delta overrides compact delta.
- Per-shard cutover switches one shard without affecting other shards.
- A shard not yet cut over still serves old full plus live realtime updates.
- RebuildGeneration realtime delta accumulated before cutover is visible after shard switch.
- Older source positions cannot overwrite newer source positions inside one generation.
- Deleted business rows are returned as normal rows with delete marker fields.
- Schema add field returns defaults for old snapshots and values for new snapshots.
- Schema delete field tombstones field ID and never reuses it.
- Type changes for existing field IDs are rejected.
- Dictionary string/list fields decode correctly.
- Artifact checksum and format validation fail closed.
- Delta compaction keeps lookup results unchanged.

Performance tests:

- Single-key `Get` latency under concurrent readers.
- Batched `MGet` latency for mixed-shard and same-shard key sets.
- Full-row decode latency for common schemas.
- Realtime delta `Get` latency and update publication cost.
- Memory use with mmap full shards, dictionary pools, and realtime deltas.
- Per-shard load and cutover time.
- Delta compaction CPU cost.

## Open Decisions

- Exact binary encoding for `FixedSizeRowSlot` alignment and endianness.
- Initial set of scalar and list element types.
- Shard count and shard assignment function.
- Full shard primary-key index uses an mmap-friendly frozen hash table.
- Default thresholds for realtime delta compaction, rebuild delta limit, and shard cutover batch size.
- Field accessors fail fast across incompatible schema versions.
- Full artifacts require mmap support in v1.
