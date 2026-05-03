# Offline Artifact Builder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an isolated offline artifact-builder module while preserving the online `//:kv_index` dependency graph.

**Architecture:** Move artifact serialization into a dependency-light online builder target that is not linked into `//:kv_index`. Add `tools/offline` as a separate Bazel module that depends on `@kv_index//:kv_index_artifact_builder` and owns future Parquet/YAML dependencies.

**Tech Stack:** C++20, Bazel/Bzlmod, existing kv_index schema/row/snapshot/artifact internals, future offline-only Parquet/YAML dependencies isolated under `tools/offline`.

---

### Task 1: Production Artifact Writer

**Files:**
- Create: `src/artifact/artifact_writer.h`
- Create: `src/artifact/artifact_writer.cc`
- Modify: `tests/test_support/artifact_writer.h`
- Modify: `tests/test_support/artifact_writer.cc`
- Modify: `BUILD.bazel`
- Modify: `tests/BUILD.bazel`
- Test: `tests/unit/artifact_format_test.cc`
- Test: `tests/unit/mmap_snapshot_backing_test.cc`

- [ ] Write a failing build/test by changing `tests/test_support/artifact_writer.cc` to call a non-existent production `kv_index::artifact::WriteArtifact`.
- [ ] Run `bazel test //tests:artifact_format_test` and confirm it fails because the production writer does not exist.
- [ ] Add production writer structs mirroring the current test artifact spec with non-test names.
- [ ] Move serialization logic from test support into `src/artifact/artifact_writer.cc`.
- [ ] Keep test support as a thin adapter from `TestArtifactSpec` to `artifact::ArtifactBuildSpec`.
- [ ] Add `//:kv_index_artifact_builder` target that depends on existing online internals but is not a dependency of `//:kv_index`.
- [ ] Run `bazel test //tests:artifact_format_test //tests:mmap_snapshot_backing_test`.

### Task 2: Offline Module Boundary

**Files:**
- Create: `tools/offline/MODULE.bazel`
- Create: `tools/offline/BUILD.bazel`
- Create: `tools/offline/src/kv_index_build_artifact.cc`
- Create: `tools/offline/README.md`

- [ ] Add an offline module with `module(name = "kv_index_offline")`.
- [ ] Add `bazel_dep(name = "kv_index", version = "0.1.0")` and `local_path_override(module_name = "kv_index", path = "../..")`.
- [ ] Add a minimal `cc_binary` that links `@kv_index//:kv_index_artifact_builder`.
- [ ] Implement CLI help and option validation, returning non-zero for missing required options.
- [ ] Run `cd tools/offline && bazel build //:kv_index_build_artifact`.

### Task 3: Schema And Parquet Adapter Follow-Up

**Files:**
- Future: `tools/offline/src/schema_config.{h,cc}`
- Future: `tools/offline/src/parquet_reader.{h,cc}`
- Future: `tools/offline/src/row_encoder.{h,cc}`

- [ ] Add YAML dependency only to `tools/offline/MODULE.bazel`.
- [ ] Add Parquet dependency only to `tools/offline/MODULE.bazel`.
- [ ] Implement schema parsing and row encoding in the offline module.
- [ ] Add a Parquet end-to-end test that builds an artifact and verifies it through online artifact parsing/loading.

### Verification

- [ ] Run `bazel test //tests:unit_tests` from the online module root.
- [ ] Run `bazel build //...` from the online module root and confirm offline code is not included.
- [ ] Run `cd tools/offline && bazel build //:kv_index_build_artifact`.
- [ ] Confirm root `MODULE.bazel` contains no offline-only dependencies.
