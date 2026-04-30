# W06 Artifact Format, Mmap Loading, And External AsyncLoad

## Metadata
- Status: pending
- Owner Role: controller
- Depends on: W05
- Retry Count: 0
- Last Updated: 2026-04-30

## Scope
Implement the external full-artifact path, including artifact format, mmap loading, rebuild generation, per-partition catch-up, and guarded per-shard cutover.

Files in scope:
- Create: `src/core/artifact_format.h`
- Create: `src/core/artifact_format.cc`
- Create: `src/core/mmap_snapshot_backing.h`
- Create: `src/core/mmap_snapshot_backing.cc`
- Create: `src/core/async_load.h`
- Create: `src/core/async_load.cc`
- Create: `tests/test_support/artifact_writer.h`
- Create: `tests/test_support/artifact_writer.cc`
- Create: `tests/unit/artifact_format_test.cc`
- Create: `tests/unit/mmap_snapshot_backing_test.cc`
- Create: `tests/unit/async_load_test.cc`
- Modify: `src/core/forward_index.cc`
- Modify: `include/kv_index/forward_index.h`

Required implementation items:
- Define the sharded artifact header, section directory, checksum rules, hash metadata, and per-partition source watermark/checkpoint representation.
- Add a test artifact writer so integration tests use real tiny artifacts instead of hand-built mocks.
- Implement read-only private mmap loading and `LoadFullShardFromArtifact`.
- Implement shard prewarm for mmap-backed full snapshots by touching each page of the frozen index, row arena, string pools, and list pools before cutover.
- Implement one external load state machine that owns rebuild generation creation, rebuild catch-up consumer startup, cutover guards, cancellation, and fail-closed behavior.
- Keep the current invalid-path smoke behavior while growing real load-state fields for artifact id, progress, errors, shard counts, and source progress.
- Reject shard-count or hash-function mismatches in v1.
- Guard per-shard cutover on loaded-and-prewarmed shard readiness plus documented per-partition catch-up and safe-position conditions.
- Add schema-version cutover behavior so old generations decode with old layout until cutover and rebuild generations decode with the new layout.
- Run `bazel test //tests:unit_tests //tests:integration_tests`.
- Commit with an async-load-module message only after verify passes and controller requests it.

Exit criteria:
- Full artifact loading is mmap-backed and fail-closed.
- A shard is not eligible for external cutover until its mmap-backed full snapshot has completed prewarm.
- External rebuild progress is modeled around per-partition progress and per-shard cutover state.
- Schema-version cutover does not require any public manual publish API.

## Plan Notes
- Pending.

## Implementation Log
- Pending.

## Verification Log
- Pending.

## Decisions
- Pending W05 completion.

## Open Issues
- None yet.

## Next Step
- Wait for W05.
