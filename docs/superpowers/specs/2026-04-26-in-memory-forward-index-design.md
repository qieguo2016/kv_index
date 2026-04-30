# In-Memory Forward Index Design

## Goal

Build an embedded C++20 library that serves low-latency in-memory forward lookups:

```text
uint64_t primary_key -> structured value
```

The component is optimized for high-concurrency read-heavy workloads. After a Kafka upsert is locally published by the target shard's active generation update stream, later reads can observe the new row. Reads concurrent with publication may return the old row, and if a read observes a realtime slot being created it may directly return miss. Reads must still observe a complete row version and never a partially applied value.

The value schema is fixed per schema version but can be hot-loaded at runtime. Schema evolution only supports adding and deleting fields. Existing field types cannot change, and field IDs are never reused.

## Non-Goals

- The SDK does not deliver full artifacts to online machines.
- The SDK does not expose a manual business-facing publish API.
- The SDK does not provide index-level delete operations. Deletes are represented as normal upserted fields, such as `is_deleted`.
- The first version does not provide global multi-key snapshot isolation across all shards. The core semantic is single-key complete-row reads, not linearizable realtime delta lookups.
- The first version does not optimize for field-scan workloads. The common path is point lookup followed by reading all fields.

## Technology Stack

- Build and compilation: use Bazel as the only build entry point. Core libraries, tests, benchmarks, and examples are managed through `BUILD.bazel` / Bazel modules, without a parallel CMake or Makefile build.
- Language standard: C++20. The design relies on C++20 `std::atomic<std::shared_ptr<T>>`, concepts-friendly API constraints, and clearer memory-model expression.
- Base library: Abseil (`absl`) is allowed, but default to equivalent C++ standard library components first, such as `std::optional`, `std::string_view`, standard containers, `std::chrono`, `std::atomic`, and standard synchronization primitives. Use Abseil components only when the standard library does not satisfy the requirement, when there is a clear performance or memory-semantics benefit, or when the design explicitly requires them, such as SwissTable-related capabilities, `absl::Cord`, or a project-wide status/error model.
- Kafka client: use `librdkafka` as the only Kafka integration. `KafkaUpdateConsumer` encapsulates `librdkafka` poll, seek, lag, and offset commit semantics; tests isolate real brokers through a fake adapter or recorded batches.

## Online API Sketch

```cpp
struct ForwardIndexOptions {
  uint32_t shard_count = 128;
  ThresholdConfig thresholds;
  KafkaConsumerConfig kafka_consumer;
};

class ForwardIndex {
 public:
  explicit ForwardIndex(const ForwardIndexOptions& options);

  std::optional<Row> Get(uint64_t primary_key) const;
  std::vector<std::optional<Row>> MGet(const std::vector<uint64_t>& primary_keys) const;

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
  std::vector<std::optional<Row>> MGet(const std::vector<uint64_t>& primary_keys) const;
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

`ForwardIndexOptions` is the SDK initialization entry point. It centralizes shard count, delta compaction / full rebase / rebuild cutover thresholds, and Kafka consumer parameters. After construction, `ForwardIndex` owns these options and uses them to create internal `KafkaUpdateConsumer`s and background update coordinators.

`ShardState` is an internal type; public query APIs do not expose `CurrentShard`. `Row` keeps the pinned shard state alive so references into row arenas and dictionary pools remain valid. Scalar field reads always copy values out. String/list fields may return either owning copies or pinned views, depending on the API. A view reference must not outlive the `Row` that holds the backing pin. `ForwardIndex::MGet` returns results in the same order as the input keys, groups keys by shard internally, and calls `ShardState::MGet` for each shard to avoid repeatedly acquiring the same shard pointer. `ShardState::MGet` only handles keys that already belong to that shard and preserves the order of the shard-local keys it receives.

## Consistency Model

The index provides single-key complete-row read semantics, but the realtime delta read path does not aim for linearizability:

- A lookup observes one shard state.
- Within that shard, it sees one complete row version.
- A lookup never observes a partially encoded row.
- If a lookup races with realtime slot creation and observes `reserved`, the realtime delta layer may directly return miss without waiting or retrying; the upper layer may still continue to compact/full lookup.
- If a lookup races with a realtime row pointer update for an existing key, it may see the old complete row version.
- Different shards may switch to a new full artifact at different times during a full update.

This intentionally does not guarantee that a multi-key request across shards observes one global full version. That stronger guarantee would require a global manifest and would increase memory pressure during full updates.

## Core Architecture

The index is sharded by primary key. Each shard can be loaded, compacted, and switched independently.

The v1 default shard count is `128`. Under the read-heavy workload with about 100 GiB of full data and about 100M rows, each shard averages about 781K rows and about 800 MiB of full data. This granularity avoids overly fragmented shard metadata, artifact sections, status metrics, and cutover scheduling, while still keeping mmap load, cutover, and compaction peak memory controllable per shard.

Shard assignment uses a stable, versioned 64-bit hash and does not use `absl::Hash`, whose behavior may vary across processes or Abseil versions:

```text
hash = StableHash64(primary_key, hash_seed, hash_version)
shard_id = hash & (shard_count - 1)
```

V1 requires `shard_count` to be a power of two. `hash_seed` and `hash_version` are written into artifact metadata so offline build, online serving, and rebuild catch-up all use the same sharding function.

```text
ForwardIndex
  -> ShardDirectory
       -> std::atomic<std::shared_ptr<const ShardState>>[N]

ShardState published container
  -> FullSnapshotView
       -> ImmutableRowSnapshotView mmap-backed or rebase-owned
  -> CompactDeltaSnapshot
       -> ImmutableRowSnapshotView owned heap-backed
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

Each upsert is a complete row. A delta hit returns a full value, so the read path never merges fields from base and delta.

`FullSnapshotView` and `CompactDeltaSnapshot` are thin wrappers whose read side delegates to `ImmutableRowSnapshotView`. They share primary-key lookup, row decoding, string/list pool resolution, and schema/layout validation logic. They differ only in backing memory and additional metadata. Full snapshots published by external `AsyncLoad` use mmap-backed artifacts; full snapshots generated by internal `Full Rebase` may use owned backing.

All shards use the same published container structure. Delta compaction, internal `Full Rebase`, and external `AsyncLoad` also use the same generation switch model:

```text
1. Create NewGeneration / NewShardState.
2. Kafka upserts from the switch start point are written to both the current serving generation and the new generation.
3. The background builder only scans inputs that are already immutable before the switch start point.
4. Confirm that the new generation realtime delta has caught up with the current serving generation.
5. Atomically switch active_shards[i] so reads start using the new generation.
6. Retire the old generation after readers drain.
```

The read path therefore always remains:

```text
realtime_delta -> compact_delta -> full_snapshot
```

Only the immutable inputs scanned by the background builder differ between update types.

## Runtime Schema

Schema is a runtime object and is included in every full artifact. Each shard state binds to one schema version and compiled row layout.

Fields are identified by stable `FieldId`, not only by name:

```text
FieldId -> name, type, repeated/list flag, nullable/default, encoding, status
```

Schema evolution rules:

- Adding a field creates a new field ID.
- Deleting a field marks the field deleted.
- Field IDs are never reused.
- Field type changes are rejected.
- A row can only be interpreted with the compiled layout for its schema version.

When external `AsyncLoad` switches schema/layout, the old active generation still decodes Kafka rows with the old schema: newly added fields are ignored, and deleted fields are still handled by the old layout until that shard switches away. The new rebuild generation encodes Kafka rows with the new schema; deleted fields follow the new layout and are no longer written into new row slots. Each shard's rows must always be interpreted with the schema/layout bound to their own generation.

Clients should resolve field names to `FieldId` and `FieldAccessor<T>` outside the hot path. Accessors include schema-version checks and fail fast when used with an incompatible shard state.

V1 supports these scalar types:

```text
int8
int32
int64
uint64
bool
string
```

V1 supports these list types:

```text
list<int8>
list<int32>
list<int64>
list<uint64>
list<bool>
list<string>
```

`presence_bitmap` distinguishes missing/null from present. A present list field with `element_count = 0` represents an empty list; a not-present field represents missing/null/default.

`FieldAccessor<T>` carries:

```text
schema_version
layout_fingerprint
field_id
physical_type
is_list
nullable/default policy
field_offset/ref_offset
```

`Row::Get(accessor)` must compare `schema_version` and `layout_fingerprint` first. If they are incompatible, it must not read by offset and must not fall back to field-name lookup. V1 behavior:

- Debug/test builds use `CHECK` or assert fail-fast.
- Release builds return an accessor mismatch error and increment `field_accessor_mismatch_total`.
- If the public API remains `std::optional<T>`, the implementation must still record the last error or expose observable status so accessor mismatch is not silently disguised as a missing field.

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

V1 artifacts and owned compact snapshots use little-endian encoding. V1 only supports little-endian hosts; loaders fail closed on non-little-endian hosts. Every row slot starts at an 8-byte aligned address, and `row_slot_size` is rounded up to 8 bytes.

Scalar fields are stored inline in `fixed_area`. Strings and lists are not stored as variable-length inline payloads. They are represented by fixed-width references in `ref_area` and resolved through external pools and arenas.

- `presence_bitmap` uses the field's position in the compiled layout; 1 bit means the field is present. Bitmap bytes are rounded up and then padded to an 8-byte boundary.
- Scalar fields in `fixed_area` use natural alignment for their physical type, with maximum 8-byte alignment. `int8` and `bool` occupy 1 byte; `int32` occupies 4 bytes; `int64` and `uint64` occupy 8 bytes. No bit-packing is used beyond the presence bitmap.
- `string` and `list<T>` in `ref_area` use a 16-byte fixed-width reference and do not inline variable-length payload in the row slot.

```text
ValueRef16
  -> uint64_t offset
  -> uint32_t byte_length
  -> uint32_t element_count_or_flags
```

For `string`, `byte_length` is the number of bytes. V1 stores UTF-8 bytes but does not perform Unicode normalization in the index layer. For `list<T>`, payload is contiguous, `byte_length` is payload byte count, and `element_count_or_flags` is the number of elements. The field's pool/encoding is determined by the compiled layout and is not repeated in every reference.

String and list fields use field-level encoding policies:

- `inline_ref`: store a fixed-width small-value reference in the row.
- `dict`: store a dictionary ID in the row and the value in a typed pool.
- `arena`: store an offset and length into a variable-length arena.
- `list_dict`: deduplicate the whole list value.
- `element_dict`: deduplicate repeated list elements, especially repeated strings.

Encoding policy is configured per field. Offline builder may also choose defaults from Parquet statistics, but explicit configuration wins. Schema additions and deletions create a new compiled layout and may change `row_slot_size`; old shard states continue using their original layout.

## Unified Row Storage Abstraction

Full, compact, and realtime layers all use a row-store abstraction that separates index from data:

```text
PrimaryKeyIndex
  -> primary_key -> RowLocator

RowStorageView
  -> RowLocator -> FixedSizeRowSlot
  -> ValueRef16 -> string/list payload pools

RowDecoder
  -> RuntimeSchema + CompiledRowLayout + FixedSizeRowSlot + payload pools
  -> Row
```

The layers differ only in whether the index is mutable and how the backing memory is owned:

```text
FullSnapshotView
  -> FrozenPrimaryKeyIndexView
  -> ImmutableRowSnapshotView

CompactDeltaSnapshot
  -> FrozenPrimaryKeyIndexView
  -> ImmutableRowSnapshotView

RealtimeDeltaAtomicTable
  -> RealtimeAtomicHashMap
  -> RealtimeRowStorageView
```

The serving-visible value in `RealtimeAtomicHashMap` is an atomic `RowRef*`, but semantically `RowRef` is still a `RowLocator`: it locates a fully encoded fixed row slot in the append-only row arena and resolves append-only string/list payload pools through `ValueRef16` values stored in that slot. After any layer hits, the read path uses the same schema/layout validation, row decoding, and field accessor logic. The only difference is that full/compact locators are frozen `row_offset` values, while realtime locators are atomically published `RowRef*` values.

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

V1 requires mmap for external full artifacts:

- External full artifacts must be loaded through read-only mmap; full dataset heap loading is not provided.
- The mmap is private/read-only; row arena, string pools, list pools, and frozen primary-key index are all read directly from artifact sections.
- The loader must validate header, format version, section directory, schema compatibility, and checksums. On failure it fails closed and leaves the current serving shard unchanged.
- To avoid the first serving request for a shard paying major or minor page-fault cost, cutover must require a full-shard prewarm step. At minimum, prewarm covers the frozen primary-key index, row arena, string pools, and list pools by touching each page so the pages are faulted into cache/page tables before the shard becomes serving-visible.
- `madvise(..., MADV_WILLNEED)` or similar readahead hints may be used as an optimization, but they are not sufficient as the readiness condition for cutover. V1 should treat successful userspace page-touch prewarm as the completion criterion. `mlock` is not required by default.
- Internal `Full Rebase` and compact delta snapshots may use owned heap backing, but after sealing they must expose the same `ImmutableRowSnapshotView` as mmap full snapshots.

## Primary Key Index

Full snapshots and compact delta snapshots both use a frozen primary-key index view in the serving read path:

```text
FrozenPrimaryKeyIndexView: primary_key -> row_offset
```

The full snapshot index is read from the mmap artifact; the compact delta snapshot index is generated by compaction and owned by heap backing. Both expose the same lookup API to the read path. `absl::flat_hash_map<uint64_t, RowOffset>` remains useful as a temporary build-time index, but a serving-visible compact snapshot should be sealed into `FrozenPrimaryKeyIndexView` to reuse the full snapshot read path. Serving-visible realtime delta uses `RealtimeAtomicHashMap`.

`FrozenPrimaryKeyIndexView` uses the same binary layout for mmap-backed full snapshots and owned-backed compact snapshots:

```text
FrozenPrimaryKeyIndex
  -> Header
       magic
       format_version
       row_count
       capacity
       hash_seed
       hash_version
       group_width
       control_offset
       key_offset
       row_offset_offset
  -> control_bytes[capacity + group_width]
  -> keys[capacity] uint64_t
  -> row_offsets[capacity] uint64_t
```

- The index uses SwissTable-style frozen open addressing.
- `capacity` is rounded up to the next power of two from `row_count / 0.875`.
- `control_bytes` store H2 metadata and empty sentinels; there are no tombstones because snapshots are immutable.
- `keys` stores primary keys, and `row_offsets` stores byte offsets into `RowArenaView`.
- mmap-backed snapshots point directly into artifact sections; owned-backed snapshots hold heap bytes in the same layout. The read path depends only on the view and does not care which backing type owns the memory.

## ImmutableRowSnapshotView

`ImmutableRowSnapshotView` is the shared read-side structure for full snapshots and compact delta snapshots:

```text
ImmutableRowSnapshotView
  -> FrozenPrimaryKeyIndexView
  -> RowArenaView
  -> StringPoolView
  -> ListPoolView
  -> RuntimeSchema
  -> CompiledRowLayout
  -> SnapshotBackingRef
```

It only provides immutable lookup and row decoding; it does not build, compact, or free memory:

```text
Get(primary_key)
  -> FrozenPrimaryKeyIndexView.Lookup(primary_key)
  -> RowArenaView.RowAt(row_offset)
  -> construct Row with schema/layout and backing pin
```

`SnapshotBackingRef` guarantees that row arena, string pools, list pools, and index memory remain valid for the lifetime of a `Row`. There are two backing implementations:

```text
MmapSnapshotBacking
  -> artifact fd / mapping
  -> section directory
  -> checksums and artifact metadata

OwnedSnapshotBacking
  -> owned frozen index bytes
  -> owned row arena
  -> owned string/list payload pools
  -> compact generation metadata
```

`FullSnapshotView` is a thin wrapper over `ImmutableRowSnapshotView + artifact/rebase metadata/source watermark`. Full snapshots from external `AsyncLoad` use `MmapSnapshotBacking`; full snapshots from internal `Full Rebase` may use `OwnedSnapshotBacking`. `CompactDeltaSnapshot` is a thin wrapper over `ImmutableRowSnapshotView + compact generation metadata/build stats`. This keeps full and compact binary encoding aligned and lets lookup, row decoding, schema evolution, and dictionary/list decoding tests share the same cases.

`SnapshotBuilder` may use `absl::flat_hash_map`, vector sort, or other temporary structures to collect key-to-row-offset mappings during build. Once sealed into `OwnedSnapshotBacking`, the serving read path only sees the frozen index view.

## RealtimeAtomicHashMap

`RealtimeAtomicHashMap` is the primary-key index for `RealtimeDeltaAtomicTable`. It targets the read-heavy, write-light online delta layer. Its goal is not to replace a general-purpose hash map, but to provide a stable read path without application-level read locks under fixed-capacity, no-erase, generation-level reclamation constraints.

The implementation should be based on the mature Abseil SwissTable / `absl::flat_hash_map` design:

- Prefer reusing Abseil public APIs, hash policy, hash mixing, and equality semantics.
- For control-byte grouped probing, H1/H2 hash splitting, probe sequence, and load-factor thresholds, prefer stable reusable Abseil mechanisms.
- If a required capability exists only in Abseil internal APIs, do not depend directly on unstable internal symbols as a long-term ABI; vendor/fork the necessary code or rewrite it following the SwissTable design, while preserving source and license notes.
- Do not use an `absl::flat_hash_map` object itself for serving realtime delta, because it does not expose slot CAS, occupancy/initialization states, or safe concurrent read/write behavior without external locks.

Recommended layout:

```text
RealtimeAtomicHashMap
  -> capacity: fixed, power-of-two preferred
  -> atomic control bytes: SwissTable-style group metadata
  -> slots[]

Slot
  -> atomic<uint8_t> state: empty / reserved / occupied
  -> uint64_t primary_key
  -> atomic<RowRef*> latest_row
```

`control bytes` quickly skip non-matching groups and reduce probes; `state` protects key and row pointer publication during concurrent slot creation. Control bytes must be implemented with atomic reads/writes or an equivalent data-race-free mechanism; the implementation cannot directly reuse the non-atomic control byte storage inside `absl::flat_hash_map`. Slots are never erased within one realtime generation. Business deletes are still represented as normal upserts with delete marker fields.

`SourcePosition` lives in `RowRef`. When updating an existing key, the writer publishes a new `RowRef*` with `latest_row.compare_exchange`, using the current `RowRef` source position to decide whether overwrite is allowed. This prevents an older replayed row from overwriting a newer row pointer.

Read path:

```text
1. Hash primary_key and derive H1/H2.
2. Scan control bytes using the SwissTable probe sequence.
3. For H2-matching slots, read state.
4. If state is reserved, directly return miss.
5. If state is occupied, compare primary_key.
6. If the key matches, acquire-load latest_row and return RowRef.
7. If an empty group terminates the probe, return miss.
```

The read path does not hold a shard-level read lock and does not read slots under initialization. If it observes `reserved`, a writer is creating the slot; `RealtimeAtomicHashMap` lookup directly returns miss without skipping, waiting, retrying, or reading key or row pointer. The upper layer can treat this as a realtime delta miss and continue to compact/full lookup. This may cause a query racing with slot creation to miss the soon-to-be-published realtime row, but it keeps the read path as short as possible and stabilizes tail latency.

Write path:

```text
1. Decode row, validate schema, write row arena, and write payload pools outside the hash map.
2. Probe for primary_key.
3. If an occupied matching slot is found, read current latest_row.
4. Compare new and old RowRef source positions; publish latest_row with compare_exchange if the new version is newer.
5. If an empty slot is found, CAS state: empty -> reserved.
6. The winning writer first writes the control byte from empty to this key's H2 so the probe chain remains non-empty; concurrent readers that hit this reserved slot return miss.
7. Initialize primary_key and latest_row.
8. Finally release-store state: reserved -> occupied.
9. Writers that lose CAS retry probing or reread the slot.
```

Writes must generate an immutable `RowRef` before publishing it to the map. Once a reader acquire-loads `RowRef*`, it can read a complete row version. If the same key is updated concurrently or replayed out of order, the write path must reject older `SourcePosition`s.

`RealtimeAtomicHashMap` targets complete-row visibility, not linearizable queries. Reads during slot creation may return miss; reads during an existing-key update may acquire-load the old `RowRef*` and return the old row. The write path only needs to guarantee that it never publishes a partially initialized slot or a partially encoded row.

Capacity policy:

- The hash map has fixed capacity and does not rehash in the serving hot path.
- Each shard reserves for expected delta key count; default load factor should not exceed 50%-70%.
- When approaching capacity thresholds, trigger delta compaction, create `CompactGeneration`, build a new `CompactDeltaSnapshot` and a fresh `RealtimeAtomicHashMap`, then publish the new `ShardState` after it catches up.
- On capacity exhaustion, fail closed: reject further new-key delta publication for that shard, alert, and trigger compaction/rebuild. Do not perform blocking expansion in the read/write hot path.

Memory and lifecycle:

- `RowRef` points into append-only row arena and append-only string/list payload pools.
- Replaced old rows are not freed in the write path.
- Old realtime generations are released as a whole after compaction or shard cutover and reader epoch drain.
- Control bytes and slots should be cache-line aligned to avoid false sharing between hot `latest_row` pointers and frequently written metadata.

## Kafka Ordering Requirements

Kafka must be keyed by `primary_key` so that updates for the same key are ordered within one partition. Each message also carries a comparable source position.

Delta writes should preserve a comparable source position, such as:

```text
partition + offset
business_version
event_time + tie_breaker
```

This prevents an older replayed update from overwriting a newer update inside the same generation.

## Kafka Consumer Module

V1 only supports Kafka and does not introduce a pluggable data-source abstraction. Kafka consumer concerns are collected in a dedicated module so poll, seek, lag, and offset commit logic do not leak across index update flows. This module contains no index logic: it does not parse schemas, choose generations, write realtime deltas, or perform shard cutover.

In implementation, `KafkaUpdateConsumer` directly wraps `librdkafka` and maps `KafkaConsumerConfig` to `librdkafka` consumer properties. The index layer only depends on the C++20/Abseil-style interface exposed by `KafkaUpdateConsumer`; business logic must not directly propagate `librdkafka` handles, callbacks, or error codes.

Layering:

```text
ForwardIndex SDK
  -> UpdateCoordinator
       -> KafkaUpdateConsumer
       -> UpdateApplier
       -> ShardCutoverCoordinator
```

`KafkaUpdateConsumer` creates and owns Kafka consumers, seeks by partition/offset, polls batches, exposes partition lag/progress, and commits offsets after the SDK confirms local publication succeeded. `UpdateCoordinator` calls the consumer to fetch batches, then passes messages to `UpdateApplier`; `UpdateApplier` decodes rows with the target generation schema/layout and writes realtime deltas. This lets the same Kafka message still be encoded with the old active generation layout and the new rebuild generation layout during an external `AsyncLoad` double-write window.

Suggested interface:

```cpp
struct KafkaPartition {
  std::string topic;
  int32_t partition;
};

struct KafkaPosition {
  KafkaPartition partition;
  uint64_t offset;
};

struct KafkaUpsertMessage {
  uint64_t primary_key;
  KafkaPosition position;
  absl::Cord raw_payload;
  KafkaMessageMetadata metadata;
};

struct KafkaCheckpoint {
  absl::flat_hash_map<KafkaPartition, KafkaPosition> positions;
};

class KafkaUpdateConsumer {
 public:
  StatusOr<std::vector<KafkaUpsertMessage>> PollBatch(PollOptions options);
  Status Commit(const KafkaCheckpoint& checkpoint);
  Status Seek(const KafkaCheckpoint& checkpoint);
  StatusOr<KafkaProgress> GetProgress() const;
};
```

`UpdateCoordinator` loops on `PollBatch`, passes messages to `UpdateApplier` for the target generation set, and commits the corresponding Kafka offsets only after every message in the batch has been locally published. On failure it fails closed and does not advance offsets. Tests can use a fake Kafka client or recorded `KafkaUpsertMessage` batches to verify coordinator/consumer interaction without introducing a non-Kafka data-source interface.

## Realtime Incremental Updates

Kafka messages are whole-record upserts:

```text
primary_key -> complete structured row
```

There is no index-level delete operation. A delete is represented by normal fields in the upserted row.

The serving path uses a read-optimized mutable delta table:

```text
RealtimeDeltaAtomicTable
  -> fixed-capacity RealtimeAtomicHashMap
  -> slot: atomic key state + primary_key + atomic RowRef*
  -> append-only row arena
  -> append-only string/list payload pools
```

Upsert publication:

```text
1. Decode and validate the Kafka row with the target generation schema/layout.
2. Encode the complete row into an immutable RowRef: write the fixed row slot into the row arena,
   write string/list payloads into this realtime generation's append-only payload pools,
   and store only fixed-width references in the row slot.
3. Find the primary-key slot in the shard's realtime table, creating the slot with CAS when absent.
4. Release-store the new RowRef* into that slot.
5. Commit Kafka offsets only after local publication succeeds.
```

After step 4, subsequent reads can see the new row. Reads concurrent with step 4 may see the old row; if realtime delta lookup observes the new slot still in `reserved`, it can directly return realtime miss and continue to compact/full lookup. Old row memory is not reclaimed inline. Replaced rows stay in the append-only arena until the realtime table generation is compacted and reader epochs prove no query can still reference the old row.

The realtime table avoids application-level read locks. Slot creation uses CAS on the slot key state. Reads use open-addressing probe plus atomic pointer load in the common path. See `RealtimeAtomicHashMap` for the hash table design.

## Delta Compaction

Realtime delta grows with live updates. Each shard should compact independently:

```text
ActiveGeneration
  -> full_snapshot + compact_delta + realtime_delta

CompactGeneration
  -> same full_snapshot
  -> rebuilt compact_delta
  -> fresh realtime_delta
```

Delta compaction also uses the double-write, cutover, and old-generation retirement flow:

```text
1. Record the current offset of the active realtime append-only buffer as the compact boundary.
2. Create CompactGeneration with a fresh empty RealtimeDeltaAtomicTable.
3. From after the boundary onward, write upserts to both ActiveGeneration and CompactGeneration.
4. Scan the active realtime append-only buffer backward from the boundary to the beginning; keep only the first latest RowRef seen for each key.
5. Continue scanning the old CompactDeltaSnapshot; only add keys not already covered by the realtime scan.
6. SnapshotBuilder builds an owned ImmutableRowSnapshot and publishes it as the new CompactDeltaSnapshot.
7. Confirm CompactGeneration realtime delta has caught up with ActiveGeneration.
8. Atomically replace active_shards[i] with CompactGeneration.
9. Release the old realtime generation and old compact snapshot after readers drain.
```

`CompactDeltaSnapshot` stores complete updated row slots keyed by primary key for records changed since the full snapshot. The new compact snapshot is equivalent to `sealed realtime delta + old compact snapshot`; on key conflicts, the sealed realtime row overrides the old compact row. It uses the same `ImmutableRowSnapshotView` read interface as `FullSnapshotView`; `FullSnapshotView` backing may come from an mmap artifact or an owned snapshot generated by internal rebase, while `CompactDeltaSnapshot` backing comes from an owned heap snapshot produced by compaction.

The read path remains fixed:

```text
realtime_delta -> compact_delta -> full_snapshot
```

If a shard's compact delta grows beyond configured thresholds, the component triggers internal `Full Rebase` and merges compact delta into that shard's `FullSnapshotView`.

Default realtime delta compaction triggers are evaluated per shard; any one condition triggers compaction:

- `RealtimeAtomicHashMap` load factor >= 0.60.
- realtime delta unique keys >= 5% of shard row count.
- realtime delta row arena bytes >= 64 MiB.
- realtime delta append-only payload pool bytes >= 64 MiB.

With the default 128 shards, this is about 39K unique updated keys per shard, usually far below the full shard size, keeping realtime probes short and compaction cost bounded. Once `CompactDeltaSnapshot` grows to a higher threshold, internal `Full Rebase` merges it into `FullSnapshotView`.

## External AsyncLoad Full Update

`FullSnapshotView` has two update triggers:

- External `AsyncLoad`: an offline system has built a new sharded full artifact and calls `ForwardIndex::LoadAsync`. This path may switch the artifact, schema version, and compiled layout.
- Internal `Full Rebase`: when a shard's `CompactDeltaSnapshot` becomes too large, the component internally merges that compact snapshot into the shard's `FullSnapshotView`. This path does not depend on an external artifact, does not change schema/layout, and only reduces compact-layer serving pressure.

Both paths execute shard by shard to avoid holding two complete full datasets in memory.

This section describes the external `AsyncLoad` path: after the offline pipeline produces a new sharded full artifact, the SDK integrator calls `ForwardIndex::LoadAsync` to trigger the full update.

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
  -> writes RebuildGeneration shards using the schema/layout bound to RebuildGeneration
  -> becomes the authoritative visible stream for a shard after that shard switches to RebuildGeneration
```

The two streams are implemented as two independent `KafkaUpdateConsumer`s. During rebuild, both consumers keep running globally. Live Apply continues writing ActiveGeneration for every shard until the entire rebuild finishes. Rebuild Catch-up writes RebuildGeneration for every shard from watermark `W` until all shards have switched. After a shard switches, Rebuild Catch-up is the active generation update stream for that shard, while Live Apply's writes to the old ActiveGeneration shard are no longer serving-visible.

If the new full artifact carries a new schema version, Live Apply still encodes rows with the old active generation schema, while Rebuild Catch-up encodes rows with the new rebuild generation schema. The same Kafka upsert may therefore enter two row stores with different layouts during the double-write window, but each generation still interprets rows only with its own compiled layout.

Per-shard cutover:

```text
1. Load new_full for shard_i from the sharded artifact.
2. Prewarm `new_full_i` by touching each page of the frozen primary-key index, row arena, string pools, and list pools so the shard does not rely on disk-backed page faults on its first serving request.
3. Ensure every Kafka partition relevant to RebuildGeneration shard_i is below the configured lag threshold and has replayed through the safe source position published by Live Apply for shard_i.
4. Attach new_full_i to RebuildGeneration shard_i.
5. Atomically replace active_shards[i] with RebuildGeneration shard_i.
6. Keep both `KafkaUpdateConsumer`s running globally.
7. Release the old shard when readers drain.
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

After a shard switches, queries for that shard read RebuildGeneration. Live Apply continues consuming and writing ActiveGeneration globally until the rebuild completes, but those writes are no longer serving-visible for switched shards. Rebuild Catch-up is the serving-visible stream for switched shards and must stay caught up according to the configured per-partition lag threshold. The cutover coordinator tracks lag by Kafka partition; only when every partition relevant to that shard is below the threshold and the new full shard has completed prewarm can rebuild catch-up be considered ready for cutover.

Rebuild generation delta limits:

- warning: global rebuild delta bytes >= 5% of active full dataset, default about 5 GiB.
- hard limit: global rebuild delta bytes >= 10% of active full dataset, default about 10 GiB; abort rebuild.
- per-shard hard limit: rebuild delta bytes >= 20% of full shard bytes, default about 160 MiB; abort rebuild or prioritize that shard.

Default shard cutover batch:

- Cut over `1` shard at a time by default.
- Allow configuration up to `2` shards, while requiring currently loaded new full shard batch bytes <= 2 GiB.
- If available memory falls below the configured watermark, automatically reduce batch size to `1`.

## Internal Full Rebase

Internal `Full Rebase` controls long-term compact delta growth. It processes one shard and merges that shard's current sealed `CompactDeltaSnapshot` into `FullSnapshotView`. It still follows the generation switch model: during rebase, Kafka upserts are written to both the old serving generation and the rebase generation; the background builder only scans immutable compact/full inputs; reads switch after the rebase generation catches up.

```text
before: full_snapshot + compact_delta + realtime_delta
after:  rebased_full_snapshot + empty compact_delta + rebase_realtime_delta
```

Rebase flow:

```text
1. Pin the current serving ShardState.
2. Create a rebase generation with a fresh empty RealtimeDeltaAtomicTable and an empty CompactDeltaSnapshot.
3. From the switch start point onward, write Kafka upserts to both the current serving generation and the rebase generation.
4. Ensure realtime data before the switch start point has entered the sealed CompactDeltaSnapshot through delta compaction; otherwise run delta compaction first.
5. Scan CompactDeltaSnapshot first, then FullSnapshotView; compact rows override full rows on key conflicts.
6. Write the merged complete shard as a new owned/mmap-capable full snapshot backing.
7. Confirm the rebase generation realtime delta has caught up with the current serving generation.
8. Atomically replace active_shards[i] with the rebased ShardState.
9. Release old full, old compact, and old realtime generation after readers drain.
```

`Full Rebase` does not consume an external full watermark and does not switch schema version. During rebase, new Kafka upserts write into the rebase generation realtime delta, which remains the highest-priority overlay after cutover. After publication, the read path remains:

```text
realtime_delta -> compact_delta -> full_snapshot
```

Internal trigger thresholds are evaluated per shard, and any one condition triggers rebase:

- compact snapshot bytes >= 20% of full shard bytes.
- compact snapshot bytes >= 256 MiB.
- compact snapshot unique keys >= 20% of shard row count.

With the default 128 shards, each full shard is about 800 MiB; `Full Rebase` usually triggers when compact snapshot size is about 160 MiB or unique keys reach about 156K. Only one rebase is allowed for a shard at a time. If external `AsyncLoad` conflicts with internal `Full Rebase`, external `AsyncLoad` takes priority and internal rebase is canceled or delayed.

## Concurrency Model

- Query threads read one immutable shard state plus its read-optimized realtime delta.
- The realtime delta uses CAS slot creation and atomic row pointer publication to publish only complete rows; concurrent reads may return miss during slot creation or return the old row during row pointer updates.
- External full rebuild and internal full rebase load and switch one shard at a time by default.
- Old shard states remain alive while any query holds a shared reference.
- Only one active external full rebuild generation should exist at a time.
- The same shard may have only one internal full rebase at a time, and it must not run concurrently with that shard's external cutover.
- Per-shard cutover and rebuild catch-up must be coordinated by one update coordinator.

## Memory Model

Peak full-update memory is bounded by shard batch size rather than full dataset size:

```text
active full dataset
+ currently loaded new full shard batch
+ active realtime deltas
+ compaction/rebase generation realtime deltas during double-write windows
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

## Testing Strategy

Core tests:

- `Get` returns full rows from full shard snapshots.
- `Get` returns full rows from compact delta snapshots through the same `ImmutableRowSnapshotView`.
- `MGet` returns results in input-key order across multiple shards.
- Realtime delta updates are visible after local publication completes; reads concurrent with publication may see the old row, and reads during slot creation may return realtime miss.
- `UpdateCoordinator` commits offsets only after every Kafka upsert in the batch has been locally published.
- `KafkaUpdateConsumer` can be tested with a fake Kafka client or recorded message batches for poll, seek, lag, and commit behavior.
- Delta hit returns a whole row and never merges partial fields from full.
- Compact delta overrides full snapshot.
- Realtime delta overrides compact delta.
- Per-shard cutover switches one shard without affecting other shards.
- A shard not yet cut over still serves old full plus live realtime updates.
- RebuildGeneration realtime delta accumulated before cutover is visible after shard switch.
- External rebuild cutover is allowed only after every relevant Kafka partition lag is below threshold and replay has passed the safe source position.
- External `AsyncLoad` can switch full artifact, schema version, and rebuild realtime delta by shard.
- During external `AsyncLoad` double-write, the old generation ignores added/deleted-field changes while the new generation encodes with the new schema.
- Internal `Full Rebase` can merge compact delta into full snapshot while preserving rebase realtime delta as the highest-priority layer.
- Internal `Full Rebase` scans only immutable compact/full inputs and preserves new upserts during the switch window through rebase realtime delta.
- If external `AsyncLoad` conflicts with internal `Full Rebase`, external `AsyncLoad` takes priority and internal rebase is canceled or delayed.
- Older source positions cannot overwrite newer source positions inside one generation.
- Delta compaction builds a new compact snapshot equivalent to sealed realtime delta overriding the old compact snapshot.
- New upserts during the delta compaction double-write window remain visible after cutover.
- Deleted business rows are returned as normal rows with delete marker fields.
- Schema add field returns defaults for old snapshots and values for new snapshots.
- Schema delete marks the field ID deleted and never reuses it.
- Type changes for existing field IDs are rejected.
- Dictionary string/list fields decode correctly.
- String/list pinned views remain valid for the lifetime of their `Row`.
- mmap-backed full snapshot, rebase-owned full snapshot, and owned compact snapshot have identical row decode behavior.
- Artifact checksum and format validation fail closed.
- Delta compaction keeps lookup results unchanged.

Performance tests:

- Single-key `Get` latency under concurrent readers.
- Batched `MGet` latency for mixed-shard and same-shard key sets.
- Full-row decode latency for common schemas.
- `ImmutableRowSnapshotView` lookup/decode latency with mmap-backed and owned-backed snapshots.
- Realtime delta `Get` latency and update publication cost.
- Memory use with mmap full shards, dictionary pools, and realtime deltas.
- Per-shard load and cutover time.
- Delta compaction CPU cost.
- Internal `Full Rebase` single-shard build time, peak memory, and publish latency.
