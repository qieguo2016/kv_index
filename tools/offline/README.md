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
  --input data.parquet \
  --output full.kvi \
  --artifact_id build-20260503 \
  --shard_count 128 \
  --hash_seed 0 \
  --hash_version 1
```

Current status: the module boundary and CLI validation are in place. The
schema parser, Parquet reader, and row encoder should be added here, with their
YAML/Parquet dependencies declared only in this module's `MODULE.bazel`.
