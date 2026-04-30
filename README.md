# kv_index

`kv_index` is a C++20 embedded in-memory forward-index library scaffolded for
the design in `docs/superpowers/specs/2026-04-26-in-memory-forward-index-design.md`.

## Build And Test

Bazel is the only supported build entry point.

```bash
bazel test //tests:all_tests
```

Focused suites are available for the initialized test layers:

```bash
bazel test //tests:unit_tests
bazel test //tests:smoke_tests
bazel test //tests:integration_tests
```

The current implementation is a bootstrap: it provides the public `kv_index`
headers, deterministic shard assignment, empty read behavior, and load-state
plumbing. Snapshot loading, realtime delta publication, Kafka consumption, and
generation cutover are intentionally left for subsequent implementation phases.
