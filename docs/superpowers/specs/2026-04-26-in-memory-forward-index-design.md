# In-Memory Forward Index Design

## Goal

Build an embedded C++17 library that serves low-latency in-memory forward lookups:

```text
uint64_t primary_key -> structured value
```

The component is optimized for high-concurrency read-heavy workloads. Reads must observe strict snapshot semantics: each query sees either one complete old version or one complete new version, never a partially updated state.

The value schema is fixed per schema version but can be hot-loaded at runtime. Schema evolution only supports adding and deleting fields. Existing field types cannot change, and field IDs are never reused.

## Non-Goals

- The SDK does not deliver full artifacts to online machines.
- The SDK does not expose a manual business-facing publish API.
- The SDK does not provide index-level delete operations. Deletes are represented as normal upserted fields, such as `is_deleted`.
- The first version does not optimize for field-scan workloads. The common path is point lookup followed by reading most or all fields.

## High-Level Architecture

```text
ForwardIndex
  -> C++17 atomic_load/store protected shared_ptr<const SnapshotManifest>

SnapshotManifest immutable
  -> FullSnapshot immutable
  -> MajorDeltaSnapshot immutable
  -> MinorDeltaSnapshot immutable

Background only
  -> MutableDeltaBuffer
```

`ForwardIndex` owns the current immutable `SnapshotManifest`. Query threads pin the manifest once, then perform lookup against the immutable minor delta, major delta, and full snapshot. In C++17 this should be implemented with `std::atomic_load` and `std::atomic_store` free functions on a `std::shared_ptr`, or hidden behind a small holder class. The design should not rely on `std::atomic<std::shared_ptr<T>>`, which is a C++20 facility.

Kafka upserts are written only to `MutableDeltaBuffer`, which is never read by query threads. Periodically, the mutable buffer is frozen and merged into a new immutable minor delta. The index then publishes a new manifest with one atomic pointer swap.

## Read Path

```text
Find(primary_key)
  -> acquire current SnapshotManifest
  -> minor_delta.Find(primary_key)
  -> major_delta.Find(primary_key)
  -> full_snapshot.Find(primary_key)
  -> return ValueView
```

The read path takes no application-level mutex from the query thread's perspective because all data structures reachable from a manifest are immutable. The only synchronization on the read path is atomically acquiring the current shared manifest pointer and incrementing the shared pointer reference count.

Because Kafka messages contain whole-record upserts, a delta hit returns a complete row. The read path never merges fields from base and delta.

`ValueView` must keep the pinned manifest or snapshot handle alive so references into row arenas and dictionary pools remain valid for the full lifetime of the view.

## Snapshot Semantics

Each query observes exactly one `SnapshotManifest`.

```text
T0: current manifest = M1
T1: background freezes Kafka buffer into MinorDelta'
T2: background builds M2 = Full + Major + MinorDelta'
T3: atomic current manifest = M2
```

Queries that pinned `M1` continue reading `M1`. Queries starting after `T3` read `M2`. No query observes a partially built delta or a mix of manifests.

## Storage Layout

The primary storage is row-based because the dominant access pattern is fetching one record and reading most or all fields.

```text
Snapshot
  -> PrimaryKeyIndex
  -> RowArena
  -> RuntimeSchema
  -> CompiledRowLayout
  -> StringPools
  -> ListPools
```

Each row is a compact variable-length record:

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

## Primary Key Index

The artifact stores a stable sorted array of:

```text
primary_key -> row_offset
```

At runtime, the default lookup index is:

```cpp
absl::flat_hash_map<uint64_t, RowOffset>
```

This keeps the first implementation simple and fast. Loading is asynchronous, so rebuilding the hash map from artifact entries is acceptable. A low-memory mode can use binary search over the sorted array. A future version can add an mmap-friendly frozen hash table without changing the query API.

## Runtime Schema

Schema is a runtime object and is included in every full artifact. Each snapshot binds to one schema version and compiled row layout.

Fields are identified by stable `FieldId`, not only by name:

```text
FieldId -> name, type, repeated/list flag, nullable/default, encoding, status
```

Schema evolution rules:

- Adding a field creates a new field ID.
- Deleting a field marks the field deprecated or tombstoned.
- Field IDs are never reused.
- Field type changes are rejected.
- A snapshot can only be interpreted with its own compiled layout.

Clients should resolve field names to `FieldId` or `FieldAccessor<T>` outside the hot path. Accessors include schema-version checks and can re-resolve or fail safely when used with a newer manifest.

## Binary Artifact

Offline build output is a binary artifact optimized for online loading:

```text
IndexArtifact
  -> Header
  -> Metadata
  -> RuntimeSchema
  -> CompiledRowLayout
  -> PrimaryKeyEntries
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
row_count
build_time
source_watermark
section_directory
checksum
```

`source_watermark` is the preferred starting point for Kafka replay. `build_time + safety_buffer` is only a fallback when the upstream source cannot provide a stronger watermark.

## Offline Builder

The offline builder is a separate Bazel target from the online runtime library. It can depend on Arrow/Parquet without pulling heavy dependencies into the query runtime.

```text
Parquet + schema/config
  -> validate schema compatibility
  -> encode rows
  -> build dictionaries and list pools
  -> write binary artifact
  -> write metadata and checksums
```

The builder owns full artifact construction only. Data delivery to online machines is outside the SDK.

## Online Loading

The online runtime exposes asynchronous loading:

```cpp
LoadId LoadAsync(const LoadRequest& request);
LoadState GetLoadState(LoadId id) const;
bool CancelLoad(LoadId id);
std::shared_ptr<const SnapshotManifest> CurrentManifest() const;
```

Full load state machine:

```text
Pending
LoadingArtifact
BuildingRuntimeIndex
ReplayingKafka
CatchingUp
SwitchingBase
Succeeded / Failed / Cancelled
```

The load job:

1. Parses artifact metadata and schema.
2. Validates schema compatibility.
3. Loads row arenas and pools.
4. Builds the runtime primary key hash index.
5. Replays Kafka upserts from `source_watermark - safety_buffer`.
6. Waits until lag is below the configured threshold.
7. Builds a new manifest and atomically switches the current manifest.

No business-facing `PublishSnapshot()` API is exposed. Publishing is an internal step of the load and delta-update state machines.

## Incremental Updates

Kafka messages are whole-record upserts:

```text
primary_key -> complete structured row
```

There is no index-level delete operation. A delete is represented by normal fields in the upserted row.

Incremental state machine:

```text
KafkaConsumer
  -> MutableDeltaBuffer
  -> freeze every N seconds or M records
  -> merge into MinorDeltaSnapshot
  -> atomic publish new SnapshotManifest
```

When `MinorDeltaSnapshot` exceeds configured thresholds, a background compaction merges it into `MajorDeltaSnapshot`:

```text
MajorDelta + MinorDelta
  -> NewMajorDelta
  -> EmptyMinorDelta
  -> atomic publish new SnapshotManifest
```

Read path layer count remains fixed:

```text
minor -> major -> full
```

If `MajorDeltaSnapshot` grows beyond a configured ratio of the full snapshot, the component should emit an alert or request a new full artifact. The SDK should not silently allow unbounded delta growth.

## C++ API Sketch

```cpp
class ForwardIndex {
 public:
  std::shared_ptr<const SnapshotManifest> CurrentManifest() const;
  std::optional<ValueView> Find(uint64_t primary_key) const;

  LoadId LoadAsync(const LoadRequest& request);
  LoadState GetLoadState(LoadId id) const;
  bool CancelLoad(LoadId id);
};

class SnapshotManifest {
 public:
  uint64_t Version() const;
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

Recommended request usage:

```cpp
auto manifest = index.CurrentManifest();
auto value = manifest->Find(primary_key);
```

This pins one manifest for the whole request and avoids acquiring the current manifest repeatedly.

## Concurrency Model

- Query threads read immutable manifests only.
- Kafka consumer and loader mutate only background-owned state.
- Publishing a new version is a single atomic manifest pointer swap.
- Old manifests remain alive while any query holds a shared reference.
- Only one full load job should be active at a time.
- Delta freeze and compaction jobs must serialize their manifest updates through one coordinator.

## Error Handling

Load or delta publication must fail closed. The current serving manifest remains active if any step fails.

Failure examples:

- Artifact checksum mismatch.
- Unsupported format version.
- Schema type incompatibility.
- Corrupt row arena or dictionary section.
- Kafka replay cannot catch up within configured limits.
- Delta row fails schema validation.

Operational state should expose the last successful manifest version, active load job state, Kafka lag, delta sizes, schema version, and last error.

## Testing Strategy

Core tests:

- Lookup returns full rows from full snapshot.
- Minor delta overrides full snapshot for the same primary key.
- Major delta overrides full snapshot.
- Minor delta overrides major delta.
- Whole-row upsert never merges partial fields from base.
- Atomic manifest switch preserves old-manifest readers.
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
- Memory use with dictionary and arena encoding.
- Load time and hash-index build time.
- Delta freeze and compaction CPU cost.

## Open Decisions

- Exact binary encoding for `RowRecord` alignment and endianness.
- Initial set of scalar and list element types.
- Default thresholds for mutable buffer freeze, minor compaction, and major-delta alerting.
- Whether field accessors auto-re-resolve across schema versions or fail fast.
- Whether the first implementation supports mmap-backed row arenas or always copies sections into owned memory.
