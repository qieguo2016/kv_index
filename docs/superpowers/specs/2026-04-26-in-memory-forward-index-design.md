# In-Memory Forward Index Design

## Goal

Build an embedded C++20 library that serves low-latency in-memory forward lookups:

```text
uint64_t primary_key -> structured value
```

The component is optimized for high-concurrency read-heavy workloads. Kafka upserts must become visible to subsequent reads immediately after the SDK consumes and publishes them locally. Reads must still observe a complete row: a lookup sees either the previous full row or the new full row, never a partially applied value.

The value schema is fixed per schema version but can be hot-loaded at runtime. Schema evolution only supports adding and deleting fields. Existing field types cannot change, and field IDs are never reused.

## Non-Goals

- The SDK does not deliver full artifacts to online machines.
- The SDK does not expose a manual business-facing publish API.
- The SDK does not provide index-level delete operations. Deletes are represented as normal upserted fields, such as `is_deleted`.
- The first version does not provide global multi-key snapshot isolation across all shards. The core semantic is single-key lookup consistency.
- The first version does not optimize for field-scan workloads. The common path is point lookup followed by reading most or all fields.

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

`ShardDirectory` is an array of independently published shard pointers. In C++20, each pointer can be represented as `std::atomic<std::shared_ptr<const ShardState>>`, or hidden behind a small holder class. Publishing a shard uses release-store; readers use acquire-load to pin the shard state before lookup.

The read path only touches one shard:

```text
Find(primary_key)
  -> shard_id = ShardFor(primary_key)
  -> acquire current ShardState for shard_id
  -> realtime_delta.Find(primary_key)
  -> compact_delta.Find(primary_key)
  -> full_snapshot.Find(primary_key)
  -> return ValueView
```

Each Kafka upsert is a complete row. A delta hit returns a full value, so the read path never merges fields from base and delta.

## Consistency Model

The index provides single-key snapshot semantics:

- A lookup observes one shard state.
- Within that shard, it sees either the old complete row or the new complete row.
- A lookup never observes a partially encoded row.
- Different shards may switch to a new full artifact at different times during a full update.

This intentionally does not guarantee that a multi-key request across shards observes one global full version. That stronger guarantee would require a global manifest and would increase memory pressure during full updates.

## Realtime Incremental Updates

Kafka messages are whole-record upserts:

```text
primary_key -> complete structured row
```

There is no index-level delete operation. A delete is represented by normal fields in the upserted row.

The serving path uses a read-optimized mutable delta table:

```text
RealtimeDeltaAtomicTable
  -> sharded hash table
  -> slot: primary_key -> atomic RowRef*
  -> append-only row arena
```

Upsert publication:

```text
1. Decode and validate the Kafka row.
2. Encode the complete row into an append-only row arena.
3. Find or create the primary-key slot in the shard's realtime table.
4. Release-store the new RowRef* into that slot.
5. Commit Kafka offset only after local publication succeeds.
```

Subsequent reads can see the new row immediately after step 4. Old row memory is not reclaimed inline. Replaced rows stay in the append-only arena until the realtime table is rotated or compacted and reader epochs prove no query can still reference the old row.

The realtime table should avoid application-level read locks. Writes may use shard-local locks or CAS during slot creation, but reads should be a hash lookup plus atomic pointer load in the common path.

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
  -> PrimaryKeyEntries or frozen hash index
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

For full snapshots, the artifact should store either:

- a sorted `primary_key -> row_offset` array, or
- an mmap-friendly frozen hash table.

The first implementation may support both modes:

```text
low-memory mode: binary search over sorted mmap array
low-latency mode: frozen hash table view
```

`absl::flat_hash_map<uint64_t, RowOffset>` remains useful for compact or realtime delta snapshots, where heap overhead is bounded by recent updates rather than the whole dataset.

## Row Layout and Value Encoding

The primary storage is row-based because the dominant access pattern is fetching one record and reading most or all fields.

```text
RowRecord
  -> row_size
  -> presence_bitmap
  -> fixed_area
  -> var_or_ref_area
```

Scalar fields are stored inline in `fixed_area`. Strings and lists use field-level encoding policies:

- `inline`: store short values directly in the row.
- `dict`: store a dictionary ID in the row and the value in a string or list pool.
- `arena`: store an offset and length into a variable-length arena.
- `list_dict`: deduplicate the whole list value.
- `element_dict`: deduplicate repeated list elements, especially repeated strings.

Encoding policy is configured per field. Offline builder may also choose defaults from Parquet statistics, but explicit configuration wins.

## Runtime Schema

Schema is a runtime object and is included in every full artifact. Each shard state binds to one schema version and compiled row layout.

Fields are identified by stable `FieldId`, not only by name:

```text
FieldId -> name, type, repeated/list flag, nullable/default, encoding, status
```

Schema evolution rules:

- Adding a field creates a new field ID.
- Deleting a field marks the field deprecated or tombstoned.
- Field IDs are never reused.
- Field type changes are rejected.
- A row can only be interpreted with the compiled layout for its schema version.

Clients should resolve field names to `FieldId` or `FieldAccessor<T>` outside the hot path. Accessors include schema-version checks and can re-resolve or fail safely when used with a newer shard state.

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
       -> rebuild_delta_since_W
       -> replay_progress
```

Two logical update streams are required:

```text
Live Apply
  -> consumes current Kafka stream
  -> writes immediately to active serving shards
  -> preserves consume-then-readable semantics

Rebuild Catch-up
  -> consumes from full_watermark W
  -> distributes upserts into NewShard[shard_id].rebuild_delta_since_W
  -> prepares shard cutover data before each shard switches
```

The two streams may be implemented as two consumers, or as one catch-up consumer plus a live consumer that starts dual-writing after catch-up. The correctness requirement is that active serving shards keep receiving live upserts while rebuild shards accumulate all upserts after `W`.

Per-shard cutover:

```text
1. Load new_full for shard_i from the sharded artifact.
2. Ensure rebuild_delta_since_W for shard_i has caught up to a safe position.
3. Build NewShardState_i = new_full_i + rebuild_delta_i + empty realtime table.
4. Record a per-shard cutover position C.
5. Ensure rebuild_delta_i contains all shard_i messages up to C.
6. Briefly pause live apply for shard_i, or write live messages > C into a per-shard handoff buffer.
7. Atomically replace active_shards[i] with NewShardState_i.
8. Apply buffered messages > C into the new realtime table.
9. Route subsequent live upserts for shard_i to the new active shard.
10. Release the old shard when readers drain.
```

At the moment shard_i switches:

```text
before: old_full_i + old_realtime_delta_i
after:  new_full_i + rebuild_delta_since_W_i + new realtime_delta_i
```

Shards that have not switched yet continue serving:

```text
old_full + old_realtime_delta
```

Their rebuild deltas continue accumulating in the background until their own cutover.

## Kafka Ordering Requirements

Kafka should be keyed by `primary_key` so that updates for the same key are ordered within one partition. If this cannot be guaranteed, each message must carry a monotonic business version or source position.

Delta writes should preserve a comparable source position, such as:

```text
partition + offset
business_version
event_time + tie_breaker
```

This prevents an older replayed update from overwriting a newer live update when live apply and rebuild catch-up overlap.

## Delta Compaction

Realtime delta grows with live updates. Each shard should compact independently:

```text
RealtimeDeltaAtomicTable
  -> scan latest RowRef per key
  -> build CompactDeltaSnapshot
  -> create a fresh empty RealtimeDeltaAtomicTable
  -> atomically publish new ShardState
```

The read path remains fixed:

```text
realtime_delta -> compact_delta -> full_snapshot
```

If a shard's compact delta grows beyond configured thresholds, the component should alert or prioritize that shard during the next full rebuild.

## Online API Sketch

```cpp
class ForwardIndex {
 public:
  std::optional<ValueView> Find(uint64_t primary_key) const;
  std::shared_ptr<const ShardState> CurrentShard(uint64_t primary_key) const;

  LoadId LoadAsync(const LoadRequest& request);
  LoadState GetLoadState(LoadId id) const;
  bool CancelLoad(LoadId id);
};

class ShardState {
 public:
  uint32_t ShardId() const;
  uint64_t Generation() const;
  const RuntimeSchema& Schema() const;
  std::optional<ValueView> Find(uint64_t primary_key) const;
};

class ValueView {
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

`ValueView` must keep the pinned shard state alive so references into row arenas and dictionary pools remain valid.

## Concurrency Model

- Query threads read one immutable shard state plus its read-optimized realtime delta.
- The realtime delta uses atomic row pointer publication for immediate visibility.
- Full rebuild loads and switches one shard or one small shard batch at a time.
- Old shard states remain alive while any query holds a shared reference.
- Only one full rebuild generation should be active at a time.
- Per-shard cutover, live apply routing, and rebuild catch-up must be coordinated by one update coordinator.

## Memory Model

Peak full-update memory is bounded by shard batch size rather than full dataset size:

```text
active full dataset
+ currently loaded new full shard batch
+ active realtime deltas
+ rebuild delta since watermark
+ compact deltas
```

To keep memory predictable:

- Use mmap-backed full shard artifacts where possible.
- Limit full cutover batch size.
- Track rebuild delta size per shard.
- Abort or throttle rebuild if rebuild delta exceeds configured limits.
- Prefer frozen indexes for full shards to avoid rebuilding full-size heap hash maps.

## Error Handling

Load, replay, compaction, or shard cutover must fail closed. Current serving shards remain active if any step fails.

Failure examples:

- Artifact checksum mismatch.
- Unsupported format version.
- Schema type incompatibility.
- Corrupt row arena or dictionary section.
- Rebuild catch-up cannot reach the safe cutover position.
- Delta row fails schema validation.
- Kafka ordering metadata is missing or inconsistent.

Operational state should expose active artifact ID, per-shard generation, rebuild progress, Kafka lag, delta sizes, schema version, and last error.

## Testing Strategy

Core tests:

- Lookup returns full rows from full shard snapshots.
- Realtime delta update is visible immediately after local publication.
- Delta hit returns a whole row and never merges partial fields from full.
- Compact delta overrides full snapshot.
- Realtime delta overrides compact delta.
- Per-shard cutover switches one shard without affecting other shards.
- A shard not yet cut over still serves old full plus live realtime updates.
- Rebuild delta accumulated before cutover is visible after shard switch.
- Older replayed updates cannot overwrite newer live updates.
- Deleted business rows are returned as normal rows with delete marker fields.
- Schema add field returns defaults for old snapshots and values for new snapshots.
- Schema delete field tombstones field ID and never reuses it.
- Type changes for existing field IDs are rejected.
- Dictionary string/list fields decode correctly.
- Artifact checksum and format validation fail closed.
- Delta compaction keeps lookup results unchanged.

Performance tests:

- Single-key lookup latency under concurrent readers.
- Full-row decode latency for common schemas.
- Realtime delta lookup latency and update publication cost.
- Memory use with mmap full shards, dictionary pools, and realtime deltas.
- Per-shard load and cutover time.
- Delta compaction CPU cost.

## Open Decisions

- Exact binary encoding for `RowRecord` alignment and endianness.
- Initial set of scalar and list element types.
- Shard count and shard assignment function.
- Whether full shard primary-key index starts with sorted array only or frozen hash table.
- Default thresholds for realtime delta compaction, rebuild delta limit, and shard cutover batch size.
- Whether field accessors auto-re-resolve across schema versions or fail fast.
- Whether mmap is mandatory for full artifacts in v1 or configurable.
