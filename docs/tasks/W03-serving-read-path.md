# W03 Serving Read Path

## Metadata
- Status: completed
- Owner Role: controller
- Depends on: W02
- Retry Count: 0
- Last Updated: 2026-04-30

## Scope
Connect immutable snapshots into shard-local serving state and then into the public `ForwardIndex` read API.

Files in scope:
- Create: `src/core/shard_state.h`
- Create: `src/core/shard_state.cc`
- Create: `src/core/shard_directory.h`
- Create: `src/core/shard_directory.cc`
- Create: `src/core/test_peer.h`
- Modify: `include/kv_index/forward_index.h`
- Modify: `src/core/forward_index.cc`
- Create: `tests/unit/shard_state_test.cc`
- Create: `tests/unit/shard_directory_test.cc`
- Modify: `tests/unit/forward_index_test.cc`
- Modify: `tests/integration/forward_index_integration_test.cc`
- Modify: `BUILD.bazel`
- Modify: `tests/BUILD.bazel`

Required implementation items:
- Implement `ShardState` precedence for `compact_delta -> full_snapshot` first, with a placeholder empty realtime layer.
- Implement `ShardDirectory` atomic publish/load semantics with `std::atomic<std::shared_ptr<const ShardState>>`.
- Wire public `ForwardIndex::Get` and `MGet` through `ShardDirectory`.
- Preserve input order and duplicate-key behavior for `MGet`.
- Use test peers for installing shard state in white-box tests; do not add any business-facing publish API.
- Run `bazel test //tests:all_tests`.
- Commit with a serving-read-path message only after verify passes and controller requests it.

Exit criteria:
- Public reads route through pinned shard state.
- `MGet` groups by shard internally but preserves caller-visible order.
- Compact rows override full rows and there is never partial field merging.

## Plan Notes
Planning conclusion:
- The original implementation plan is specific enough to define W03 scope, required files, and exit criteria. W03 is too broad to implement safely as one undifferentiated coding pass, so keep W03 as the single active task and execute the serial sub-stages below.
- Current code context: `ForwardIndex::ShardFor` already uses the stable versioned hash, but public `Get` and `MGet` are still bootstrap misses. W02 commit `b2f248d` provides `SnapshotBuilder`, `OwnedSnapshotBacking`, `FullSnapshotView`, and `CompactDeltaSnapshot`; both snapshot wrappers currently expose `StatusOr<std::optional<Row>> Get(std::uint64_t) const`.
- BUILD-file scope decision: the original W03 file list omitted `BUILD.bazel` and `tests/BUILD.bazel`, but the repo uses explicit Bazel targets and Bazel is the only build/test entry point. The controller authorizes these two BUILD-file edits for W03 wiring only.

Minimal executable breakdown for coding agent:
1. Baseline and wiring guard:
   - Inspect `include/kv_index/forward_index.h`, `src/core/forward_index.cc`, W02 snapshot headers, current `BUILD.bazel`, `tests/BUILD.bazel`, and existing snapshot/forward-index tests.
   - If feasible before behavior-changing edits, run `bazel test //tests:unit_tests` and record any pre-existing failure in `Implementation Log`.
- Add the new W03 source/test files to Bazel targets early enough to support test-first work.
2. ShardState core:
   - Create `src/core/shard_state.h` and `src/core/shard_state.cc` under `kv_index::core`.
   - Store `shard_id`, `generation`, optional `CompactDeltaSnapshot`, optional `FullSnapshotView`, and a placeholder realtime layer that always returns miss. Do not implement W04 realtime data structures in W03.
   - Implement `StatusOr<std::optional<Row>> Get(std::uint64_t primary_key) const` with precedence `empty_realtime -> compact_delta -> full_snapshot`. A compact hit must return the complete compact row immediately and must not merge fields from full. A non-OK status from a higher-priority layer must fail closed and must not fall through to lower layers.
   - Implement `MGet` by preserving the order of the shard-local input keys. It may loop through `Get` for W03; optimize only if it stays simple and covered by tests.
3. ShardDirectory publish/load:
   - Create `src/core/shard_directory.h` and `src/core/shard_directory.cc`.
   - Back the directory with one `std::atomic<std::shared_ptr<const ShardState>>` per shard. Construct the vector at final size because atomic shared pointers are not copy/move friendly after placement.
   - Implement acquire-load for readers and release-store for publishing. Initial unpublished shards should load as `nullptr` and serve misses through `ForwardIndex`.
   - Reject invalid shard ids and reject `nullptr` publishes. Do not add any delete/unpublish API.
4. ForwardIndex integration and test peer:
   - Modify `include/kv_index/forward_index.h` without including internal `src/core` headers. Prefer an internal pimpl-style member, e.g. an incomplete `core::ShardDirectory` owned by `std::unique_ptr`, with an out-of-line destructor in `src/core/forward_index.cc`.
   - Add a private friend declaration for an internal test peer only; do not add any business-facing manual publish or `CurrentShard` API.
   - Create `src/core/test_peer.h` to let tests install/load shard state through the private directory. Keep it in the internal Bazel target only.
   - Wire `ForwardIndex::Get` to compute `ShardFor(primary_key)`, acquire-load the shard state, return miss on unpublished shard, and return the shard result.
   - Wire `ForwardIndex::MGet` to group input positions by shard internally, acquire each shard pointer at most once per non-empty bucket, call shard-local `MGet`, and scatter results back to the original positions. Preserve caller-visible input order and duplicate-key positions exactly.
   - Do not collapse internal `Status` errors into misses. Because W03 has no public runtime-status API, the first implementation should throw `std::runtime_error` with the status message at the public boundary, or stop for controller guidance if a different status surface is required.
5. Tests and final gate:
   - Add focused unit tests for `ShardState`, `ShardDirectory`, and `ForwardIndex` routing before/with implementation.
   - Update `tests/integration/forward_index_integration_test.cc` only within W03 scope, preferably to cover public read behavior through test-peer-installed internal state if BUILD scope permits the internal dependency.
   - Run focused tests as each stage lands, then run `bazel test //tests:all_tests`.
   - Do not commit unless the controller later requests it after verify passes.

Implementation boundaries:
- In scope for coding: W03 source/test files listed above, `include/kv_index/forward_index.h`, `src/core/forward_index.cc`, and the necessary `BUILD.bazel` / `tests/BUILD.bazel` wiring changes authorized by the controller.
- Out of scope: realtime delta implementation, Kafka/update pipeline behavior, async artifact loading, delta compaction, full rebase, public manual publish APIs, public shard accessors, index-layer delete operations, and broad `Row`/schema/snapshot API redesigns.
- Keep `ShardState`, `ShardDirectory`, and `test_peer.h` internal under `src/core`. Public SDK users should still include only `kv_index/...` headers and should not see internal headers.
- Continue using `StableHash64(primary_key, hash_seed, hash_version)` via existing `ForwardIndex::ShardFor`; do not introduce `std::hash`, `absl::Hash`, pointer hashing, or any process-dependent assignment.
- Prefer C++ standard library types. Do not add Abseil for W03 unless a concrete, documented performance/memory reason appears.
- Preserve existing `LoadAsync`, `GetLoadState`, and `CancelLoad` bootstrap behavior except for any incidental construction needed to initialize the shard directory.
- Do not change `Row` lifetime internals in W03. Current W02 snapshots materialize rows into owned `EncodedRow` objects, so returned rows can remain valid after a shard republish; if true row-to-shard-state pinning becomes required, stop and ask the controller to expand scope.

Dependency assumptions:
- W02 commit `b2f248d` is present and provides the immutable snapshot APIs now visible in `src/core/snapshot.h` and `src/core/snapshot_builder.h`.
- `FullSnapshotView` and `CompactDeltaSnapshot` are thin owned-snapshot wrappers with identical decode behavior and can be stored by value inside `std::optional` or behind `std::shared_ptr` in `ShardState`.
- `ShardState::MGet` can be implemented as ordered repeated `Get` calls in W03 because batch snapshot lookup APIs do not yet exist.
- Empty/unpublished shards are a normal bootstrap state and should serve misses, not errors.
- The existing simple test macros in `tests/test_support/test_macros.h` are sufficient; W03 test helpers can duplicate small snapshot-building utilities locally instead of expanding shared test-support scope.

Risks:
- BUILD-file edits must stay limited to W03 wiring. Avoid unrelated target reshaping.
- Accidentally including `src/core/shard_directory.h` from the public header would leak internal implementation details through the SDK surface. Use forward declarations/pimpl instead.
- Treating a corrupt compact snapshot as a miss would violate fail-closed precedence and could incorrectly return stale full rows.
- `MGet` grouping is easy to implement in a way that reorders results or collapses duplicate keys; tests must use interleaved cross-shard keys and duplicate positions.
- `std::atomic<std::shared_ptr<const ShardState>>` memory ordering is subtle. Tests cannot prove release/acquire ordering exhaustively, so the implementation must use explicit release-store and acquire-load by inspection.
- The design says rows should pin shard state, but current W02 row lifetime is preserved by owned row materialization instead. This is acceptable for W03, but future zero-copy/mmap work may require revisiting `Row`.

Concrete acceptance criteria:
- `ShardState` serves misses with no layers, serves full snapshot hits, lets compact rows override full rows, and never merges missing compact fields from full rows.
- `ShardState` propagates non-OK snapshot statuses instead of falling through to a lower-priority layer or returning miss.
- `ShardDirectory` starts empty, acquire-loads published states, release-stores replacements, rejects invalid shard ids/null publishes, and leaves previously loaded `shared_ptr` pins usable after republish.
- Public `ForwardIndex::Get` routes through `ShardDirectory` and returns installed shard rows while preserving existing miss behavior for unpublished shards.
- Public `ForwardIndex::MGet` groups by shard internally but returns a vector with exactly the same size, order, and duplicate-key positions as the input.
- No business-facing publish/current-shard/delete API is added; only internal tests can install shard state through `src/core/test_peer.h`.
- `bazel test //tests:all_tests` passes before W03 is handed to verification.

Prioritized focused tests:
- Optional baseline guard: `bazel test //tests:unit_tests`.
- Shard precedence and no-merge behavior: `bazel test //tests:shard_state_test`.
- Atomic publish/load semantics and pin survival: `bazel test //tests:shard_directory_test`.
- Public routing/order/duplicate behavior: `bazel test //tests:forward_index_unit_test`.
- Integration smoke for installed read state if internal test-peer dependency is approved: `bazel test //tests:forward_index_integration_test`.
- Task gate before handoff: `bazel test //tests:all_tests`.

## Implementation Log
- 2026-04-30 coding agent baseline guard: `bazel test //tests:unit_tests` passed before behavior-changing edits; 7/7 unit targets passed from cache.
- 2026-04-30 ShardState TDD red: added `tests/unit/shard_state_test.cc` and `//tests:shard_state_test`; `bazel test //tests:shard_state_test` failed as expected because `src/core/shard_state.h` did not exist.
- 2026-04-30 ShardState green: added `src/core/shard_state.h` and `src/core/shard_state.cc`; implemented shard id/generation, empty realtime placeholder, compact-before-full precedence, compact whole-row override with no full-field merge, fail-closed non-OK propagation, and ordered shard-local `MGet`; `bazel test //tests:shard_state_test` passed.
- 2026-04-30 ShardDirectory TDD red: added `tests/unit/shard_directory_test.cc` and `//tests:shard_directory_test`; `bazel test //tests:shard_directory_test` failed as expected because `src/core/shard_directory.h` did not exist.
- 2026-04-30 ShardDirectory green: added `src/core/shard_directory.h` and `src/core/shard_directory.cc`; implemented empty null loads, invalid-id/null-publish rejection, release publish, acquire load, and pin survival after republish. The first implementation attempted `std::atomic<std::shared_ptr<const ShardState>>`, but this Apple libc++ rejected it as non-trivially-copyable and exposed no `.load/.store` specialization. The compiling implementation uses the standard `std::atomic_store_explicit` / `std::atomic_load_explicit` shared_ptr overloads with release/acquire memory orders; `bazel test //tests:shard_directory_test` passed.
- 2026-04-30 ForwardIndex/test-peer TDD red: added public routing tests to `tests/unit/forward_index_test.cc`, installed-state integration coverage to `tests/integration/forward_index_integration_test.cc`, and the internal dependency wiring; `bazel test //tests:forward_index_unit_test` failed as expected because `src/core/test_peer.h` did not exist.
- 2026-04-30 ForwardIndex/test-peer green: added internal `src/core/test_peer.h`; updated `include/kv_index/forward_index.h` with forward declarations, pimpl ownership, out-of-line destructor, and a private test-peer friend without exposing internal headers; wired `ForwardIndex::Get` through `ShardFor` and `ShardDirectory`; wired `ForwardIndex::MGet` to group positions by shard, acquire each non-empty shard once, call shard-local `MGet`, and scatter results back preserving order and duplicates; public read boundary throws `std::runtime_error` on non-OK internal `Status`; `bazel test //tests:forward_index_unit_test` passed and `bazel test //tests:forward_index_integration_test` passed.
- 2026-04-30 focused gate: `bazel test //tests:shard_state_test //tests:shard_directory_test //tests:forward_index_unit_test //tests:forward_index_integration_test` passed; 4/4 focused targets passed.
- 2026-04-30 unit gate: `bazel test //tests:unit_tests` passed; 9/9 unit targets passed.
- 2026-04-30 W03 handoff gate: `bazel test //tests:all_tests` passed; 11/11 test targets passed.
- Changed files: `BUILD.bazel`, `include/kv_index/forward_index.h`, `src/core/forward_index.cc`, `src/core/shard_state.h`, `src/core/shard_state.cc`, `src/core/shard_directory.h`, `src/core/shard_directory.cc`, `src/core/test_peer.h`, `tests/BUILD.bazel`, `tests/unit/shard_state_test.cc`, `tests/unit/shard_directory_test.cc`, `tests/unit/forward_index_test.cc`, `tests/integration/forward_index_integration_test.cc`, and this implementation log.
- Known risks: local Apple libc++ does not compile the requested `std::atomic<std::shared_ptr<const ShardState>>` specialization, so `ShardDirectory` uses the standard atomic shared_ptr free functions instead. This preserves atomic shared_ptr load/store semantics for this toolchain but should be reviewed against the controller's literal storage-shape requirement.

## Verification Log
- 2026-04-30 verify agent:
  - Final decision: pass.
  - Diff/scope inspection:
    - `git rev-parse HEAD`: confirmed W02 baseline commit `b2f248d1251eb5944a174bef8047279581b8a571`.
    - `git status --short`: tracked edits are `BUILD.bazel`, `docs/tasks/W03-serving-read-path.md`, `docs/tasks/controller.md`, `include/kv_index/forward_index.h`, `src/core/forward_index.cc`, `tests/BUILD.bazel`, `tests/integration/forward_index_integration_test.cc`, and `tests/unit/forward_index_test.cc`.
    - `git diff --name-status`: tracked implementation edits are within W03 scope, with `docs/tasks/controller.md` limited to controller status/history edits.
    - `git ls-files --others --exclude-standard`: explicitly listed expected untracked W03 files `src/core/shard_directory.{h,cc}`, `src/core/shard_state.{h,cc}`, `src/core/test_peer.h`, `tests/unit/shard_directory_test.cc`, and `tests/unit/shard_state_test.cc`.
  - Implementation review:
    - `ShardState` serves misses when empty, serves full snapshot hits, checks compact before full, returns a compact row as the whole row without field merge, propagates non-OK compact/full statuses fail-closed, and preserves shard-local `MGet` order and duplicate positions by looping over ordered keys.
    - `ShardDirectory` starts with null shard slots, rejects invalid shard ids and null publishes, provides no delete/unpublish API, and previously loaded `shared_ptr` pins remain usable after republish.
    - Atomic shared pointer assessment: the storage is `std::vector<std::shared_ptr<const ShardState>>` rather than `std::atomic<std::shared_ptr<const ShardState>>`, but every post-construction slot access uses the standard shared_ptr atomic free functions: `std::atomic_store_explicit(..., std::memory_order_release)` for publish and `std::atomic_load_explicit(..., std::memory_order_acquire)` for load. The vector is constructed at final size and no non-atomic read/write of the slots was found after construction. This is standard-conforming and semantically equivalent to the W03 release/acquire publish/load intent on this Apple libc++ toolchain.
    - `ForwardIndex` public header forward-declares internal types and does not include `src/core` headers. No business-facing publish/current-shard/delete API was added; shard installation is confined to internal `src/core/test_peer.h` for tests.
    - `ForwardIndex::Get` routes through `ShardFor` and `ShardDirectory`. `ForwardIndex::MGet` groups by shard, loads each non-empty shard once, calls shard-local `MGet`, and scatters results back preserving vector size, order, and duplicate-key positions. Internal non-OK statuses are surfaced by throwing `std::runtime_error`, not collapsed into misses.
    - Existing `LoadAsync`, `GetLoadState`, and `CancelLoad` bootstrap implementations are unchanged except for `ForwardIndex` construction of the shard directory.
  - Commands/tests run:
    - `bazel test //tests:shard_state_test`: passed from cache, 1/1 target passing.
    - `bazel test //tests:shard_directory_test`: passed from cache, 1/1 target passing.
    - `bazel test //tests:forward_index_unit_test`: passed from cache, 1/1 target passing.
    - `bazel test //tests:forward_index_integration_test`: passed from cache, 1/1 target passing.
    - `bazel test //tests:unit_tests`: passed from cache, 9/9 targets passing.
    - `bazel test //tests:all_tests`: passed from cache, 11/11 targets passing.
    - `bazel test //tests:all_tests --nocache_test_results`: passed, executed 11/11 targets.
    - `git diff --check`: passed with no whitespace errors.
  - Review notes:
    - No blocking W03 plan-compliance issues, regressions, or test failures found.
    - Non-blocking future hardening opportunity: keep the invariant that all `ShardDirectory` slot accesses use the shared_ptr atomic free functions unless/until the toolchain supports `std::atomic<std::shared_ptr<...>>`.

## Decisions
- 2026-04-30: W02 completed in commit `b2f248d`; W03 selected as the next active task.
- 2026-04-30: Plan agent concluded the original W03 implementation plan is specific enough, but W03 should execute as serial sub-stages inside the single active task. The plan also identified a BUILD-file scope gap.
- 2026-04-30: Controller authorized `BUILD.bazel` and `tests/BUILD.bazel` edits for W03 Bazel wiring only, because the new W03 source and test files cannot compile otherwise.
- 2026-04-30: Verify agent returned `pass` after focused tests, `unit_tests`, `all_tests`, and uncached `all_tests`. Controller accepted the standard atomic shared_ptr free-function approach for this toolchain and authorized the W03 commit.

## Open Issues
- None.

## Next Step
- W03 completed. Create the W03 atomic commit, then controller may select W04.
