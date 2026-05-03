# Offline Artifact Builder Design

## Goal

Add an offline command that builds a full kv_index artifact from a schema file
and a Parquet input while keeping the online `kv_index` Bazel module free of
offline-only dependencies.

## Dependency Boundary

`kv_index` remains the online module. Its public serving target `//:kv_index`
continues to depend only on the current online dependency set. Other projects
that import `@kv_index//:kv_index` must not resolve Arrow, Parquet, YAML, or
Python package dependencies.

The offline command lives under `tools/offline/` with its own `MODULE.bazel`.
That directory is a separate Bazel module and is built by running Bazel from
inside `tools/offline`. The offline module depends on the online module through:

```python
bazel_dep(name = "kv_index", version = "0.1.0")
local_path_override(module_name = "kv_index", path = "../..")
```

The override is root-module-only, so consumers of the online module do not
inherit this local development wiring. Offline-only dependencies are declared
only in `tools/offline/MODULE.bazel`.

## Online Builder Surface

The online module exposes a small artifact-builder target that has no
Parquet/YAML dependency. This target owns serialization of the kv_index artifact
format and accepts already compiled layouts plus already encoded rows:

- artifact id, shard count, hash seed, hash version
- compiled row layout
- optional source progress
- per-shard primary keys and `model::EncodedRow` values

This moves the existing artifact-writing logic out of `tests/test_support` into
production code so tests and offline tools use the same writer.

`//:kv_index` does not depend on the artifact-builder target. The target is
available for offline tooling as `@kv_index//:kv_index_artifact_builder`.

## Offline Command

The offline module owns file-format adapters:

- schema-file parser: maps `schema.yaml` to `RuntimeSchema`,
  `CompiledRowLayout`, and primary-key metadata.
- Parquet reader: reads batches from the Parquet file, validates columns against
  the schema config, and converts each row to field values.
- row encoder: converts typed values into `model::EncodedRow` for the compiled
  layout.
- shard router: uses `StableHash64(primary_key, hash_seed, hash_version) %
  shard_count`.
- command entry point: validates CLI options and writes the full artifact.

The initial CLI shape is:

```bash
kv_index_build_artifact \
  --schema schema.yaml \
  --input data.parquet \
  --output full.kvi \
  --artifact_id build-20260503 \
  --shard_count 128 \
  --hash_seed 0 \
  --hash_version 1 \
  [--omit_source_progress]
```

## Compatibility Rules

- Missing required fields fail closed.
- Nullable absent fields remain physically absent in the encoded row.
- Field encodings follow `RuntimeSchema::AddField` validation.
- Artifact metadata uses the same hash seed/version as online serving.
- Duplicate primary keys in the same full artifact fail closed.
- The writer produces artifacts accepted by `ParseArtifact` and
  `MmapSnapshotBacking::LoadShard`.

## Testing

Online tests cover artifact writer compatibility without offline dependencies.
Offline module tests cover CLI parsing and module wiring without requiring the
online module to resolve offline dependencies.

End-to-end Parquet tests should live only in `tools/offline` once the chosen
Parquet dependency is available in that module.
