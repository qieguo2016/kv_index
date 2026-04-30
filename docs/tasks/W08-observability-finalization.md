# W08 Observability, Lifetime Verification, Integration, Benchmarks, And Docs

## Metadata
- Status: pending
- Owner Role: controller
- Depends on: W07
- Retry Count: 0
- Last Updated: 2026-04-30

## Scope
Close the loop on runtime status, fail-closed paths, lifetime guarantees, end-to-end integration, benchmarks, and user-facing docs.

Files in scope:
- Modify: `include/kv_index/types.h`
- Modify: `include/kv_index/forward_index.h`
- Modify: `src/core/forward_index.cc`
- Modify: `src/core/shard_state.h`
- Modify: `src/core/realtime_delta.h`
- Create: `tests/unit/runtime_status_test.cc`
- Create: `tests/unit/lifetime_test.cc`
- Create: `tests/integration/read_precedence_integration_test.cc`
- Create: `tests/integration/async_load_cutover_integration_test.cc`
- Create: `tests/integration/schema_evolution_integration_test.cc`
- Create: `tests/benchmark/get_benchmark.cc`
- Create: `tests/benchmark/mget_benchmark.cc`
- Create: `tests/benchmark/realtime_delta_benchmark.cc`
- Create: `tests/benchmark/snapshot_decode_benchmark.cc`
- Modify: `README.md`
- Optionally create: `examples/basic_lookup.cc`

Required implementation items:
- Add runtime status structs for shard generation, schema version, artifact id, delta stats, accessor mismatch count, and last error.
- Harden cancellation, checksum failure, schema failure, and cutover-failure paths so serving shards stay unchanged on error.
- Verify lifetime pinning for old full, old compact, and old realtime rows after cutover.
- Add end-to-end integration coverage for read precedence, cross-shard `MGet`, async load windows, schema evolution, compaction, and rebase.
- Add focused non-blocking benchmarks for `Get`, `MGet`, snapshot decode, and realtime update cost.
- Update `README.md` and optional examples only after the implementation is actually present.
- Run `bazel test //tests:all_tests` and `bazel build //...`.
- Commit with an observability/docs/finalization message only after verify passes and controller requests it.

Exit criteria:
- Runtime status is sufficient to debug accessor mismatch, load failure, and cutover progress.
- Generation lifetime is proven by tests, not only by design intent.
- Public docs no longer describe implemented functionality as missing.

## Plan Notes
- Pending.

## Implementation Log
- Pending.

## Verification Log
- Pending.

## Decisions
- Pending W07 completion.

## Open Issues
- None yet.

## Next Step
- Wait for W07.
