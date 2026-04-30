# W02 Immutable Snapshot Module

## Metadata
- Status: completed
- Owner Role: controller
- Depends on: W01
- Retry Count: 0
- Last Updated: 2026-04-30

## Scope
Build the shared immutable storage path used by full snapshots and compact snapshots, including frozen primary-key index, owned snapshot backing, and shared lookup/decode view.

Files in scope:
- Create: `src/core/frozen_primary_key_index.h`
- Create: `src/core/frozen_primary_key_index.cc`
- Create: `src/core/snapshot_builder.h`
- Create: `src/core/snapshot_builder.cc`
- Create: `src/core/snapshot.h`
- Create: `src/core/snapshot.cc`
- Create: `tests/unit/frozen_primary_key_index_test.cc`
- Create: `tests/unit/snapshot_test.cc`
- Modify: `tests/BUILD.bazel`
- Modify: `BUILD.bazel`

Required implementation items:
- Implement the frozen SwissTable-style primary-key index layout, validation, and lookup behavior.
- Make `SnapshotBuilder` consume the shared encoded-row materialization contract instead of raw row-slot bytes alone.
- Implement `OwnedSnapshotBacking` so it can hold frozen index bytes, row arena bytes, payload pools, and dictionary/list-dictionary data.
- Implement `ImmutableRowSnapshotView` as the only lookup/decode path for immutable snapshots.
- Add `FullSnapshotView` and `CompactDeltaSnapshot` as thin wrappers differing only in metadata and backing ownership.
- Run `bazel test //tests:unit_tests`.
- Commit with an immutable-snapshot-module message only after verify passes and controller requests it.

Exit criteria:
- A row encoded once through the shared materialization contract can be served from an owned immutable snapshot.
- Full and compact snapshot wrappers return identical decode behavior for the same backing.
- Row lifetime remains valid after local view objects are destroyed as long as the row pin is held.

## Plan Notes
Planning conclusion:
- The original implementation plan is specific enough to define W02 scope, required files, and exit criteria. W02 is too broad to implement safely as one undifferentiated coding pass, so keep W02 as the single active task and execute the serial sub-stages below.

Minimal executable breakdown for coding agent:
1. Baseline and context guard:
   - Inspect W01 APIs in `include/kv_index/schema.h`, `include/kv_index/row.h`, `src/core/row_storage.h`, `src/core/row_storage.cc`, `src/core/hash.h`, and the current Bazel/test patterns.
   - If feasible before behavior-changing edits, run `bazel test //tests:unit_tests` and record any pre-existing failure in `Implementation Log`.
   - Add W02 test targets to `tests/BUILD.bazel` early enough to support test-first work, then keep all new snapshot code behind the restricted `//:kv_index_internal` target.
2. Frozen primary-key index:
   - Create `src/core/frozen_primary_key_index.h` and `src/core/frozen_primary_key_index.cc`.
   - Define an internal frozen index byte layout matching the design: header with magic, format version, row count, capacity, hash seed, hash version, group width, control/key/row-offset section offsets; followed by `control_bytes[capacity + group_width]`, `keys[capacity]`, and `row_offsets[capacity]`.
   - Use `kv_index::core::StableHash64(primary_key, hash_seed, hash_version)` for all placement and lookup. Do not use `std::hash`, `absl::Hash`, pointer values, or process-dependent hashing.
   - Build a SwissTable-style immutable open-addressed table with an empty sentinel control byte, H2 metadata, no tombstones, power-of-two capacity, and a max load factor of 0.875. Keep `group_width` explicit in the header, with 16 as the first-version default unless tests reveal a simpler constant is needed.
   - Provide a validated `FrozenPrimaryKeyIndexView` over bytes and a builder helper that returns owned bytes. Validation must reject bad magic/version, malformed offsets, non-power-of-two capacity, capacity too small for row count, truncated sections, invalid group width, and duplicate primary keys during build.
   - Cover empty, single-row, multi-row, collision/probe, miss, duplicate-key, seed/version metadata, and malformed-layout behavior in `tests/unit/frozen_primary_key_index_test.cc`.
3. Snapshot builder and owned backing:
   - Create `src/core/snapshot_builder.h` and `src/core/snapshot_builder.cc`.
   - Make `SnapshotBuilder` consume W01's shared `kv_index::internal::EncodedRow` materialization contract, not ad hoc raw row-slot bytes. The builder should validate `schema_version`, `layout_fingerprint`, and minimum `row_slot_size()` against the supplied `CompiledRowLayout`.
   - Store immutable rows in `OwnedSnapshotBacking` using the W01 materialization shape: contiguous fixed row-slot bytes keyed by byte `row_offset`, plus per-row arena bytes, string dictionaries, scalar-list dictionaries, string-list dictionaries, and string element dictionaries. Keep this standard-library-only unless there is a concrete measured reason to add Abseil.
   - Reject duplicate primary keys in `SnapshotBuilder`; compaction/rebase stages must de-duplicate before calling W02 builders.
   - Seal builds the frozen index from primary key to row-slot byte offset and returns an owned backing/view object without exposing temporary build maps to serving code.
4. Shared immutable snapshot view:
   - Create `src/core/snapshot.h` and `src/core/snapshot.cc`.
   - Implement `ImmutableRowSnapshotView` as the only immutable snapshot lookup/decode path. It should use `FrozenPrimaryKeyIndexView::Lookup(primary_key)`, validate the returned row offset against the owned backing, reconstruct or pin an `EncodedRow` backed by the snapshot data, and call `kv_index::internal::MaterializeRow`.
   - Prefer an internal API like `StatusOr<std::optional<Row>> Get(std::uint64_t primary_key) const`, so misses are distinct from corruption or layout errors. Keep public `ForwardIndex` integration for W03.
   - Preserve row lifetime after local snapshot/view objects are destroyed by ensuring returned `Row` objects hold a `std::shared_ptr` pin to the materialized row data or backing-owned equivalent.
   - Add `FullSnapshotView` and `CompactDeltaSnapshot` as thin wrappers over the same `ImmutableRowSnapshotView`; wrappers may carry minimal metadata, but lookup/decode must delegate to the shared view.
5. Bazel wiring and handoff:
   - Add the new snapshot/index source and header files to `//:kv_index_internal` in `BUILD.bazel`.
   - Add `frozen_primary_key_index_test` and `snapshot_test` to `tests/BUILD.bazel`, and include both in `unit_tests` and `all_tests`.
   - Run focused tests as each stage lands, then run `bazel test //tests:unit_tests`.
   - Do not commit unless the controller later requests it after verify passes.

Implementation boundaries:
- In scope for coding: only the files listed in W02 scope, plus this task document's `Implementation Log` if the coding agent records progress.
- Out of scope: public SDK read-path integration, shard directory, realtime delta, Kafka/update coordination, mmap artifact loading, artifact checksum/prewarm, delta compaction, full rebase orchestration, business-facing manual publish APIs, and index-layer delete operations.
- Do not modify W01 schema/row APIs unless W02 cannot satisfy the row-lifetime contract without that change. If such a blocker appears, stop and record it for controller/human decision instead of expanding scope silently.
- Keep snapshot types internal under `src/core`; do not expose snapshot implementation headers through public SDK headers.
- Use C++20 and the C++ standard library first. Abseil is allowed only for a clearly justified temporary build-time data structure, not serving-visible immutable lookup.
- Preserve stable, versioned hash metadata for index layout and future artifact compatibility: `StableHash64(primary_key, hash_seed, hash_version)` is the only shard/index hash primitive.

Dependency assumptions:
- W01 commit `7d6216d6` is present and provides `RuntimeSchema`, `CompiledRowLayout`, `FieldAccessor<T>`, `Row`, `kv_index::internal::EncodedRow`, row writer helpers, and `kv_index::internal::MaterializeRow`.
- `EncodedRow` currently owns row-slot bytes, arena bytes, and dictionary/list-dictionary payload containers; W02 can use that as the source materialization contract without changing W01 files.
- The first W02 implementation may reconstruct a pinned `EncodedRow` per successful snapshot lookup if needed for lifetime safety; zero-copy mmap-backed row views can be left to W06 unless W02 can achieve them without changing W01 APIs.
- The first version is little-endian and internal-only; external artifact loading, host-endian fail-closed checks, and mmap section validation belong to W06.
- Existing `tests/test_support/test_macros.h` is sufficient for W02 tests; keep test helpers local to the new unit test files where practical.

Risks:
- Frozen-index probing has several edge cases: empty tables, control-byte mirroring, H2 collisions, capacity/load calculations, and offset validation can all create false misses if not covered with focused tests.
- Current `Row` pins `std::shared_ptr<const EncodedRow>` rather than a generic `SnapshotBackingRef`, so the coding agent must be deliberate about row lifetime and should not return views into destroyed local buffers.
- Dictionary and list-dictionary payloads can drift from W01 decode semantics if the builder serializes only row slots; snapshot tests must include arena strings plus dictionary-backed strings/lists.
- Duplicate-key policy affects later compaction/rebase semantics. W02 should reject duplicates and rely on later stages to present already-deduplicated complete rows.
- Overbuilding artifact/mmap concerns in W02 could blur task boundaries with W06. Keep owned heap backing and shared view semantics first.

Concrete acceptance criteria:
- `FrozenPrimaryKeyIndexView` validates the frozen byte layout before serving and performs deterministic lookup using header `hash_seed` and `hash_version`.
- `SnapshotBuilder` accepts complete `EncodedRow` values, rejects mismatched schema/layout metadata and duplicate keys, and seals into an `OwnedSnapshotBacking`.
- `OwnedSnapshotBacking` owns frozen index bytes, fixed row-slot arena bytes, per-row payload arena bytes, and all dictionary/list-dictionary payload containers needed to decode rows.
- `ImmutableRowSnapshotView` is the only lookup/decode implementation used by immutable snapshots and returns explicit errors for malformed backing instead of treating corruption as a miss.
- `FullSnapshotView` and `CompactDeltaSnapshot` delegate to the same immutable view and return identical decoded rows for the same backing.
- A row returned from a snapshot remains valid after temporary builder/view objects are destroyed as long as the returned `Row` is held.
- `bazel test //tests:unit_tests` passes before handing off to verify.

Prioritized focused tests:
- Optional baseline guard: `bazel test //tests:unit_tests`.
- Frozen index layout and lookup: `bazel test //tests:frozen_primary_key_index_test`.
- Owned snapshot build/decode/lifetime: `bazel test //tests:snapshot_test`.
- Task gate before handoff: `bazel test //tests:unit_tests`.

## Implementation Log
- 2026-04-30 coding agent:
  - Context/baseline guard: inspected W01 schema/row/materialization APIs and current Bazel test wiring. Ran `bazel test //tests:unit_tests` before behavior-changing edits; result: PASS, 5/5 tests passed from cache.
  - Frozen index TDD red: added `tests/unit/frozen_primary_key_index_test.cc` and `//tests:frozen_primary_key_index_test`; ran `bazel test //tests:frozen_primary_key_index_test`; expected failure observed: compile error for missing `src/core/frozen_primary_key_index.h`.
  - Frozen index green: implemented frozen byte layout, stable-hash builder, validation, duplicate-key rejection, and lookup in `src/core/frozen_primary_key_index.{h,cc}`; reran `bazel test //tests:frozen_primary_key_index_test`; result: PASS.
  - Snapshot TDD red: added `tests/unit/snapshot_test.cc` and `//tests:snapshot_test`; ran `bazel test //tests:snapshot_test`; expected failure observed: compile error for missing `src/core/snapshot.h`.
  - Snapshot green: implemented `OwnedSnapshotBacking`, `SnapshotBuilder`, `ImmutableRowSnapshotView`, `FullSnapshotView`, and `CompactDeltaSnapshot` in `src/core/snapshot*.{h,cc}`; reran `bazel test //tests:snapshot_test`; result: PASS.
  - Post-green cleanup: changed `ImmutableRowSnapshotView` to retain the validated frozen-index view/status instead of revalidating the whole index per lookup, and made frozen-index load validation avoid overflow in malformed headers. Reran focused tests after cleanup; result: PASS.
  - W02 gate runs after implementation: `bazel test //tests:frozen_primary_key_index_test` PASS; `bazel test //tests:snapshot_test` PASS; `bazel test //tests:unit_tests` PASS, 7/7 tests; `bazel test //tests:all_tests` PASS, 9/9 tests; `bazel test --nocache_test_results //tests:all_tests` PASS, 9/9 tests executed.
  - Changed files: `BUILD.bazel`; `tests/BUILD.bazel`; `src/core/frozen_primary_key_index.h`; `src/core/frozen_primary_key_index.cc`; `src/core/snapshot.h`; `src/core/snapshot.cc`; `src/core/snapshot_builder.h`; `src/core/snapshot_builder.cc`; `tests/unit/frozen_primary_key_index_test.cc`; `tests/unit/snapshot_test.cc`; `docs/tasks/W02-immutable-snapshot.md`.
  - Implementation notes: W02 uses only the C++ standard library plus existing project primitives. Immutable snapshot lookup reconstructs an owned `EncodedRow` per hit and calls W01 `MaterializeRow`, preserving returned `Row` lifetime after temporary builder/view/backing objects are destroyed. External mmap/zero-copy artifact loading remains deferred to W06 by design.

## Verification Log
- 2026-04-30 verify agent:
  - Final decision: pass.
  - Diff/scope inspection:
    - `git rev-parse HEAD`: confirmed the W01 baseline is `7d6216d6e93fb68ba5a42a397a5223f4406df381`.
    - `git status --short`: tracked edits are `BUILD.bazel`, `tests/BUILD.bazel`, `docs/tasks/W02-immutable-snapshot.md`, and `docs/tasks/controller.md`; untracked W02 additions are `src/core/frozen_primary_key_index.{h,cc}`, `src/core/snapshot.{h,cc}`, `src/core/snapshot_builder.{h,cc}`, `tests/unit/frozen_primary_key_index_test.cc`, and `tests/unit/snapshot_test.cc`.
    - `git diff --name-status`: tracked edits are limited to `BUILD.bazel`, `tests/BUILD.bazel`, `docs/tasks/W02-immutable-snapshot.md`, and controller status edits in `docs/tasks/controller.md`.
    - `git ls-files --others --exclude-standard`: explicitly listed the expected untracked W02 source and test files.
    - Scope check passed: implementation changes are within W02 files, with only controller status edits outside W02; no W01 public APIs, public `ForwardIndex` read path, artifact/mmap loading, shard directory, realtime, or Kafka scope were changed.
  - Implementation review:
    - Frozen primary-key index layout includes magic, format version, header size, row count, capacity, hash seed, hash version, group width, control/key/row-offset offsets, total size, `control_bytes[capacity + group_width]`, `keys[capacity]`, and `row_offsets[capacity]`.
    - Frozen-index validation rejects bad magic/version/header size, invalid group width, non-power-of-two or too-small capacity, malformed offsets, truncated sections, non-mirrored control bytes, invalid control bytes, duplicate keys in malformed bytes, H2/key mismatch, unreachable slots, and row-count mismatch. Build rejects duplicate primary keys.
    - Lookup uses `kv_index::core::StableHash64(primary_key, hash_seed, hash_version)` from the validated header metadata and returns `StatusOr<std::optional<uint64_t>>`, keeping ordinary misses distinct from malformed index/backing errors.
    - `SnapshotBuilder` consumes W01 `kv_index::internal::EncodedRow`, validates schema version, layout fingerprint, and minimum row-slot size, rejects duplicate primary keys, and does not introduce an alternate row decode contract.
    - `OwnedSnapshotBacking` owns frozen index bytes, contiguous fixed row-slot bytes, per-row arena payload bytes, and all W01 dictionary/list-dictionary/element-dictionary payload containers needed by `MaterializeRow`.
    - `ImmutableRowSnapshotView` is the shared full/compact lookup and decode path. It validates the index once, propagates malformed backing errors separately from misses, reconstructs a pinned `EncodedRow` per hit, and delegates decode to W01 `MaterializeRow`.
    - `FullSnapshotView` and `CompactDeltaSnapshot` are thin wrappers over the same `ImmutableRowSnapshotView` path and tests confirm identical hit/miss behavior for the same backing.
    - Row lifetime is safe for W02: returned `Row` owns a materialized `EncodedRow`, so it remains valid after builder, backing, and local view objects are destroyed.
  - Tests and commands:
    - `bazel test //tests:frozen_primary_key_index_test`: passed from cache, 1/1 test passing.
    - `bazel test //tests:snapshot_test`: passed from cache, 1/1 test passing.
    - `bazel test //tests:unit_tests`: passed from cache, 7/7 test targets passing.
    - `bazel test //tests:all_tests`: passed from cache, 9/9 test targets passing.
    - `bazel test //tests:all_tests --nocache_test_results`: passed, executed 9/9 test targets.
    - `git diff --check`: passed.
  - Review notes:
    - No blocking W02 plan-compliance issues or regressions found.
    - Non-blocking hardening opportunity: `SnapshotBuilder::AddRow` currently uses linear duplicate detection, which is acceptable for W02 correctness but should become a set-backed or seal-time check before large full-snapshot builds.
    - Non-blocking test hardening opportunity: frozen-index tests cover probe collisions and broad malformed layout behavior, but future tests could explicitly corrupt bad version, section offsets, invalid group width, non-power-of-two capacity, and control-byte mirror bytes.

## Decisions
- 2026-04-30: W01 completed in commit `7d6216d6`; W02 selected as the next active task.
- 2026-04-30: Plan agent concluded the original W02 plan is specific enough for scope, but W02 should execute as serial sub-stages inside the single active task. Duplicate primary keys should be rejected by W02 builders; later compaction/rebase stages are responsible for presenting already-deduplicated rows.
- 2026-04-30: Verify agent returned `pass` after focused tests, `unit_tests`, `all_tests`, and uncached `all_tests`. Controller marked W02 completed and authorized the W02 commit.

## Open Issues
- None.

## Open Risks
- No blocking W02 implementation risks are known.
- The current owned snapshot view intentionally copies row-slot/payload data into a materialized `EncodedRow` per successful lookup for lifetime safety; mmap-backed zero-copy serving remains out of scope for W02 and belongs to W06.
- `SnapshotBuilder` duplicate-key detection is currently linear per `AddRow`; this is not a correctness blocker for W02, but should be revisited before large full-snapshot build paths depend on it.
- Frozen-index malformed-layout tests cover representative corruption, but additional explicit header/offset/mirror corruption cases would further harden regression coverage.

## Next Step
- W02 completed. Create the W02 atomic commit, then controller may select W03.
