# kv_index

`kv_index` is a C++20 embedded in-memory forward-index library for high
concurrency, read-heavy point lookup workloads. It keeps immutable full and
compact snapshots plus realtime deltas in memory, and exposes pinned `Row`
objects so decoded string/list payloads remain valid while callers hold them.

## Build And Test

Bazel is the only supported build entry point.

```bash
bazel test //tests:all_tests
bazel build //...
```

Focused suites are available for the initialized test layers:

```bash
bazel test //tests:unit_tests
bazel test //tests:smoke_tests
bazel test //tests:integration_tests
```

Benchmark binaries are build-only helpers and are not part of the default test
suites:

```bash
bazel build //tests:get_benchmark //tests:mget_benchmark \
  //tests:realtime_delta_benchmark //tests:snapshot_decode_benchmark
```

## Implemented V1 Surface

- `ForwardIndexOptions` defaults to `shard_count = 128` and `hash_version = 1`.
  Shard count must be a non-zero power of two.
- `ForwardIndex::Get` and `ForwardIndex::MGet` read pinned shard generations.
  `MGet` preserves input order, duplicates, and misses across shards.
- Reads apply whole-row layer precedence: realtime delta, then compact delta,
  then full snapshot. There is no field-level merge between layers.
- Local/file artifact loading is available through `LoadAsync`,
  `GetLoadState`, and `CancelLoad`. Load state reports shard load, prewarm,
  cutover progress, source progress, terminal state, and last error.
- Runtime debugging is available through `GetRuntimeStatus()`, including shard
  generation, schema/layout fingerprint, artifact id for mmap full snapshots,
  layer presence, row counts, realtime delta stats, load states, and last error.
- Realtime publication, delta compaction, and internal full rebase are covered
  by in-repo broker-free tests. Kafka access remains encapsulated behind
  `KafkaUpdateConsumer`; automated tests use fake seams rather than a live
  broker.

## Limits

- Artifacts are loaded from local paths or `file://` URIs. Remote downloaders
  and production artifact builders are outside the v1 library surface.
- Runtime status is a synchronous debug snapshot, not a metrics exporter.
- Accessor mismatch remains observable through existing accessor errors. The
  aggregate `accessor_mismatch_count` status field is currently always zero
  because counting mismatches would require adding shared mutable state to
  pinned `Row` ownership.
- Async load cutover publishes shards sequentially. A late publish failure can
  leave earlier shards on the new generation and later shards on the old one;
  load state reports the explicit `cutover_shard_count` and last error.
