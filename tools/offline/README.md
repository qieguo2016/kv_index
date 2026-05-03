# kv_index Offline Artifact Builder

This directory is a separate Bazel module. Build it from this directory so its
offline-only dependencies stay out of the online `kv_index` module graph.

```bash
cd tools/offline
bazel build //:kv_index_build_artifact
```

The intended command shape is:

```bash
kv_index_build_artifact \
  --schema schema.yaml \
  --input hive_table_parquet_dir \
  --output full_artifact_dir \
  --artifact_id build-20260503 \
  --omit_source_progress
```

The input may be one Parquet file or a directory tree containing Hive-style
Parquet partitions. The output is a directory containing:

```text
schema.yaml
shard_00000.kvi
shard_00001.kvi
...
```

`schema.yaml` carries the updated schema version plus the final shard/hash
configuration. By default the tool writes `schema_version + 1`; pass
`--schema_version N` to choose an explicit version. `--shard_count`,
`--hash_seed`, and `--hash_version` override values in the schema file when
present.

Apache Arrow/Parquet is intentionally wired only in this offline Bazel module.
The checked-in `third_party/apache_arrow` wrapper points at the Homebrew
`apache-arrow` keg under `/opt/homebrew/opt/apache-arrow`.
