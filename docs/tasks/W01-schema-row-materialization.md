# W01 Schema, Layout, Row Semantics, And Row Materialization Contract

## Metadata
- Status: completed
- Owner Role: controller
- Depends on: W00
- Retry Count: 1
- Last Updated: 2026-04-30

## Scope
Define one coherent module for runtime schema, compiled layout, field accessor semantics, row decode semantics, and the shared logical encoded-row shape.

Files in scope:
- Create: `include/kv_index/schema.h`
- Modify: `include/kv_index/row.h`
- Create: `src/core/byte_io.h`
- Create: `src/core/schema.cc`
- Create: `src/core/row_storage.h`
- Create: `src/core/row_storage.cc`
- Create: `tests/unit/byte_io_test.cc`
- Create: `tests/unit/schema_test.cc`
- Create: `tests/unit/row_decoder_test.cc`
- Modify: `tests/BUILD.bazel`
- Modify: `BUILD.bazel`

Required implementation items:
- Add byte-order, alignment, and `ValueRef16` utilities.
- Implement `RuntimeSchema`, including active/deleted field tracking, encoding policy, nullability, and explicit default-value policy for a schema version.
- Implement `CompiledRowLayout` and lock down layout fingerprint, presence bitmap semantics, and row-slot alignment rules.
- Define `FieldAccessor<T>` creation and mismatch behavior together with `Row::Has`, scalar `Get`, string `Get`, and list accessors.
- Define one shared encoded-row materialization contract that can represent row slot bytes plus arena and dictionary payloads; use that shape in test builders immediately.
- Implement row decoding for scalar fields, string fields, scalar lists, and dictionary-backed string/list variants using the same module.
- Run `bazel test //tests:unit_tests`.
- Commit with a schema/row-module message only after verify passes and controller requests it.

Exit criteria:
- `Row`, `FieldAccessor<T>`, schema evolution rules, and row decode semantics are tested as one unit.
- Dictionary-backed and arena-backed field encodings are represented by the same row materialization contract.
- Accessor mismatch is observable and does not silently appear as field absence.

## Plan Notes
Planning conclusion:
- The original implementation plan is specific enough to define W01 scope, file boundaries, and required outcomes. W01 is still too broad for one undifferentiated coding pass, so keep W01 as the single active task but execute it as the serial sub-stages below.

Minimal executable breakdown for coding agent:
1. Baseline and red-test setup:
   - Inspect current `include/kv_index/row.h`, `include/kv_index/types.h`, `include/kv_index/status.h`, `BUILD.bazel`, and `tests/BUILD.bazel`.
   - If feasible before behavior-changing edits, run `bazel test //tests:unit_tests` and record any pre-existing failure in `Implementation Log`.
   - Add the three W01 test targets to `tests/BUILD.bazel` early, then write focused tests before each module is implemented.
2. Byte IO and fixed reference utilities:
   - Create `src/core/byte_io.h`.
   - Add little-endian read/write helpers for fixed-width integers without unaligned `reinterpret_cast`.
   - Add `AlignUp(value, alignment)` helpers and pin 8-byte row-slot alignment behavior.
   - Add `ValueRef16` with exactly `uint64_t offset`, `uint32_t byte_length`, and `uint32_t element_count_or_flags` semantics, plus little-endian encode/decode helpers.
   - Cover golden byte layout, offset/length/count round trips, alignment edge cases, and malformed/truncated input in `tests/unit/byte_io_test.cc`.
3. Runtime schema and compiled layout:
   - Create `include/kv_index/schema.h` and `src/core/schema.cc`.
   - Define field kinds for the first-version scalar types (`int8`, `int32`, `int64`, `uint64`, `bool`, `string`) and list forms for those types.
   - Define field encoding policies for fixed scalar storage, arena-backed string/list payloads, dictionary-backed string values, whole-list dictionary values, and element-dictionary list values.
   - Define explicit null/default policy. For W01, `Row::Has(field_id)` must mean the encoded presence bit is set; an absent field with an explicit default may return that default from `Get`, while `Has` remains false. Nullable/no-default absence returns `std::nullopt`. Non-nullable/no-default absence should be rejected during row materialization/validation.
   - Implement `RuntimeSchema` with schema version, active field lookup, deleted field history, add/delete evolution checks, and rejection of type changes or field ID reuse.
   - Implement `CompiledRowLayout` from active fields sorted by `FieldId` for deterministic presence-bit positions and offsets.
   - Lock down layout rules: presence bitmap padded to an 8-byte boundary, scalar fixed area naturally aligned up to 8 bytes, 16-byte refs in the ref area, and total `row_slot_size` rounded up to 8 bytes.
   - Use a deterministic layout fingerprint that does not rely on `std::hash`; it must change when schema version, active field set, type/list shape, encoding/default policy, offsets, or slot size changes.
   - Cover schema evolution, deleted field behavior, invalid type changes, field ID reuse, deterministic layout, bitmap indexing, offset alignment, row-slot size, and fingerprint stability in `tests/unit/schema_test.cc`.
4. Accessors, row storage contract, and row lifetime:
   - Modify `include/kv_index/row.h` so `FieldAccessor<T>` is created from the compiled layout rather than hand-filled by callers.
   - Accessor creation must reject unknown, deleted, scalar/list-shape-mismatched, physical-type-mismatched, and schema/layout-incompatible fields using `StatusOr` or an equivalent observable failure.
   - Define accessor mismatch behavior for row reads now: schema version, layout fingerprint, field ID, scalar/list shape, and physical type must be checked before reading offsets. Mismatch must throw or otherwise surface an explicit error; it must never return `std::nullopt` or an empty list as if the field were absent.
   - Create `src/core/row_storage.h` and `src/core/row_storage.cc`.
   - Define one shared encoded-row materialization shape, owned/view variants as needed, containing fixed row-slot bytes plus arena payload bytes and dictionary/list-dictionary/element-dictionary payloads. Tests must use this shape immediately instead of ad hoc raw buffers.
   - Ensure `Row` pins or otherwise owns the backing needed for row-slot bytes, arena bytes, and dictionary payloads for the lifetime of any returned string/list view.
5. Row decoding semantics:
   - Implement `Row::Has`, scalar `Get<T>(FieldId)`, scalar `Get<T>(FieldAccessor<T>)`, string `Get<std::string>`, list field-id access, and list accessor access.
   - Field-id reads should distinguish ordinary absence/default handling from type/list mismatch. Type/list mismatch is an error; physical absence follows the null/default policy.
   - Empty list is represented as present with `element_count_or_flags == 0`; missing/default list is represented by the presence bit being unset. Tests should assert callers can distinguish these via `Has`.
   - Decode arena-backed strings and scalar lists from `ValueRef16` offsets/lengths.
   - Decode dictionary-backed strings by interpreting the ref according to the field encoding policy, not by storing a per-ref encoding tag.
   - Decode whole-list dictionary values and element-dictionary list values through the same row materialization contract. For list payloads, keep scalar payloads little-endian and validate byte length against element count and element width.
6. Bazel wiring and handoff:
   - Update `BUILD.bazel` so `include/kv_index/schema.h` is part of public headers and `src/core/byte_io.h`, `src/core/schema.cc`, `src/core/row_storage.h`, and `src/core/row_storage.cc` are internal implementation files.
   - Update `tests/BUILD.bazel` with `byte_io_test`, `schema_test`, and `row_decoder_test`, and include them in `unit_tests` and `all_tests`.
   - Run focused tests as each module lands, then run `bazel test //tests:unit_tests`.
   - Do not commit unless the controller later requests it after verify passes.

Implementation boundaries:
- In scope for coding: only the files listed in W01 scope, plus this task document's `Implementation Log` if the coding agent records progress.
- Out of scope: snapshot indexes, mmap/owned snapshot backing, realtime delta hash maps, Kafka update parsing, async artifact loading, compaction/full rebase, observability metrics, manual publish APIs, and index-layer delete operations.
- Keep `src/core/byte_io.h` and `src/core/row_storage.h` internal; public SDK users should include only `kv_index/...` headers.
- Do not change W00 shard-count or stable-hash behavior. `ForwardIndexOptions::shard_count` remains default `128` and must remain a non-zero power of two; `StableHash64(primary_key, hash_seed, hash_version)` remains the shard-assignment primitive and artifact metadata contract for later tasks.
- Prefer the C++ standard library. Do not introduce Abseil in W01 unless the coding agent records a concrete standard-library gap and controller accepts the scope change.
- W01 may add schema/row API surface needed by this task, but should not implement business-specific schema loading or artifact parsing.

Dependency assumptions:
- W00 commit `dc85d5e` is the implementation baseline and provides `Status`, `StatusOr<T>`, a restricted `//:kv_index_internal` target, stable hash helpers, and bootstrap unit tests.
- Current `Row`/`FieldAccessor` implementations are placeholders and can be replaced inside W01 scope.
- The first version supports only little-endian encoded rows and little-endian hosts; future artifact loaders will fail closed on incompatible hosts.
- Field IDs are stable, never reused, and active-field layout order should be deterministic by `FieldId`.
- `tests/test_support/test_macros.h` is sufficient for W01 unit tests; add small local test helpers or builders inside W01 test files if needed.

Risks:
- Accessor mismatch can easily look like field absence because current public getters return `std::optional`; tests must assert the explicit mismatch signal.
- `GetList` returning an empty `ListView` can hide the difference between missing and present-empty lists unless `Row::Has` semantics are pinned.
- Template accessor code in public headers can accidentally expose internal storage details; prefer small public dispatch APIs plus opaque row backing where practical.
- Layout fingerprinting must be deterministic across processes and compilers; avoid `std::hash`, pointer values, unordered container iteration order, and padding bytes.
- Value decoding must avoid undefined behavior from unaligned loads and must validate `ValueRef16` bounds before producing views.
- Dictionary-backed list/string support can drift from arena-backed support if tests build separate helper shapes; force all row decoder tests through the shared materialization contract.
- Default-value behavior can be confused with physical presence; W01 tests must explicitly cover `Has == false` while `Get` returns an explicit default.

Concrete acceptance criteria:
- `RuntimeSchema` tracks schema version, active fields, deleted field history, field encoding policy, nullability, and default policy.
- Schema evolution allows adding fields and marking fields deleted, rejects field ID reuse, rejects type/list-shape changes for existing fields, and prevents accessors for deleted fields.
- `CompiledRowLayout` deterministically assigns presence bits and offsets, produces 8-byte-aligned row slots, and has a stable fingerprint that changes for material layout/schema changes.
- `ValueRef16` has pinned 16-byte little-endian encoding and row-slot refs are validated before use.
- `FieldAccessor<T>` creation succeeds only for compatible active fields and captures schema version, layout fingerprint, field ID, physical type, list/scalar shape, null/default policy, and offset/ref offset.
- `Row::Has`, scalar `Get`, string `Get`, scalar list access, and dictionary-backed string/list access all decode through the same row materialization module.
- Ordinary missing/default behavior and accessor/type mismatch behavior are tested separately; mismatch is observable and is not reported as absence.
- Arena-backed and dictionary-backed fields are represented by the same encoded-row materialization contract in `row_decoder_test.cc`.
- `bazel test //tests:unit_tests` passes before handing off to verify.

Prioritized focused tests:
- Optional baseline guard: `bazel test //tests:unit_tests`.
- Byte utilities: `bazel test //tests:byte_io_test`.
- Schema/layout/accessor creation: `bazel test //tests:schema_test`.
- Row decode/materialization/accessor mismatch: `bazel test //tests:row_decoder_test`.
- Task gate before handoff: `bazel test //tests:unit_tests`.

## Implementation Log
- 2026-04-30 coding pass:
  - Baseline guard before behavior edits: `bazel test //tests:unit_tests` passed; 2 existing unit targets passed from cache.
  - Byte IO TDD:
    - RED: `bazel test //tests:byte_io_test` failed to build because `src/core/byte_io.h` did not exist.
    - GREEN: added internal little-endian fixed-width read/write helpers, `AlignUp`, and pinned 16-byte `ValueRef16` encode/decode; `bazel test //tests:byte_io_test` passed.
  - Schema/layout TDD:
    - RED: `bazel test //tests:schema_test` failed to build because `kv_index/schema.h` did not exist.
    - GREEN: added `RuntimeSchema`, `FieldSpec`, encoding/default validation, deleted-field history, deterministic `CompiledRowLayout`, 8-byte row-slot alignment, and deterministic layout fingerprinting; `bazel test //tests:schema_test` passed.
  - Row/accessor/materialization TDD:
    - RED: `bazel test //tests:row_decoder_test` failed to build because `src/core/row_storage.h` did not exist.
    - GREEN: replaced placeholder `Row`/`FieldAccessor` APIs, added typed accessor creation with observable mismatch failures, added shared `EncodedRow` materialization with row-slot/arena/dictionary/list-dictionary/element-dictionary payloads, and implemented scalar/string/list/default decoding; `bazel test //tests:row_decoder_test` passed.
  - Bazel wiring:
    - Added `include/kv_index/schema.h` to public headers.
    - Added `src/core/byte_io.h`, `src/core/schema.cc`, `src/core/row_storage.h`, and `src/core/row_storage.cc` to `kv_index_internal`.
    - Added `byte_io_test`, `schema_test`, and `row_decoder_test` to `unit_tests` and `all_tests`.
  - Files changed by W01 coding pass:
    - `BUILD.bazel`
    - `include/kv_index/schema.h`
    - `include/kv_index/row.h`
    - `src/core/byte_io.h`
    - `src/core/schema.cc`
    - `src/core/row_storage.h`
    - `src/core/row_storage.cc`
    - `tests/BUILD.bazel`
    - `tests/unit/byte_io_test.cc`
    - `tests/unit/schema_test.cc`
    - `tests/unit/row_decoder_test.cc`
    - `docs/tasks/W01-schema-row-materialization.md`
  - Commands run and results:
    - `bazel test //tests:unit_tests` baseline guard: passed.
    - `bazel test //tests:byte_io_test` RED: failed to build on missing `src/core/byte_io.h`.
    - `bazel test //tests:byte_io_test` GREEN/final focused run: passed.
    - `bazel test //tests:schema_test` RED: failed to build on missing `kv_index/schema.h`.
    - `bazel test //tests:schema_test` GREEN/final focused run: passed.
    - `bazel test //tests:row_decoder_test` RED: failed to build on missing `src/core/row_storage.h`.
    - `bazel test //tests:row_decoder_test` GREEN/final focused run: passed.
    - `bazel test //tests:unit_tests` W01 gate: passed, 5 unit targets passed.
    - `bazel test //tests:all_tests` broader gate: passed, 7 targets passed.
  - Known risks:
    - `ListView<T>` currently returns owning decoded vectors, which is safe for W01 lifetime semantics but may be revisited for zero-copy string/list views in later snapshot workstreams.
    - Arena-backed `list<string>` decoding is supported as ref payload decoding, but W01 test coverage focuses on scalar arena lists plus dictionary-backed string lists and element-dictionary string lists.
- 2026-04-30 fix retry 1:
  - Root cause confirmed in `src/core/row_storage.cc::ResolveField`: accessor reads for a field ID missing from the row's active layout returned `Status::NotFound` before accessor metadata was validated, and public accessor overloads mapped `kNotFound` to `std::nullopt` or an empty `ListView`.
  - TDD regression coverage added in `tests/unit/row_decoder_test.cc`:
    - Scalar: accessor created from an older layout for a field deleted from the target row layout must throw rather than return `std::nullopt`.
    - List: accessor created from an older layout for a list field deleted from the target row layout must throw rather than return an empty list.
    - Direct `FieldId` reads for the same inactive fields still verify ordinary absence behavior.
  - RED result:
    - `bazel test //tests:row_decoder_test` failed as expected at `tests/unit/row_decoder_test.cc:173` with `expected exception: std::runtime_error`.
  - Fix:
    - Updated `ResolveField` so accessor-supplied reads validate accessor schema/layout metadata before ordinary missing-field handling.
    - If the active layout does not contain the accessor field ID, accessor reads now return `FailedPrecondition`; by-id reads still return `NotFound` for ordinary absence.
  - Files changed in retry:
    - `src/core/row_storage.cc`
    - `tests/unit/row_decoder_test.cc`
    - `docs/tasks/W01-schema-row-materialization.md`
  - GREEN/final verification:
    - `bazel test //tests:row_decoder_test`: passed.
    - `bazel test //tests:unit_tests`: passed, 5 test targets.
    - `bazel test //tests:all_tests`: passed, 7 test targets.
    - `bazel test //tests:all_tests --nocache_test_results`: passed, 7 of 7 tests executed.
  - Remaining risks:
    - No remaining known risk for the accessor-missing-field mismatch failure.

## Verification Log
- 2026-04-30 verify pass:
  - Diff/scope inspection:
    - `git diff master...HEAD --name-status` failed because `master` is not present/available in this working tree; verification used the current uncommitted working tree against `HEAD`, targeted file reads, `git status --short`, `git diff --name-status`, `git ls-files --others --exclude-standard`, and `git show --stat dc85d5e`.
    - Current changed files are the expected W01 files plus `docs/tasks/controller.md`; controller status edits were ignored as instructed.
    - `BUILD.bazel` keeps `src/core/byte_io.h` and `src/core/row_storage.h` in restricted `//:kv_index_internal`, while `include/kv_index/schema.h` is public as required.
  - Commands run:
    - `git status --short`: showed W01 implementation files and controller status edit.
    - `git diff --name-status`: showed tracked W01/controller edits; untracked W01 additions were confirmed separately.
    - `git ls-files --others --exclude-standard`: showed the expected new W01 files.
    - `git show --stat dc85d5e`: confirmed W00 baseline context.
    - `bazel test //tests:byte_io_test`: passed from cache.
    - `bazel test //tests:schema_test`: passed from cache.
    - `bazel test //tests:row_decoder_test`: passed from cache.
    - `bazel test //tests:unit_tests`: passed from cache, 5 test targets.
    - `bazel test //tests:all_tests`: passed from cache, 7 test targets.
    - `git diff --check`: passed.
    - `bazel test //tests:all_tests --nocache_test_results`: passed, 7 of 7 tests executed.
  - Review findings:
    - FAIL: Accessor/layout mismatch can still masquerade as absence when the accessor field ID is not active in the row's layout. In `src/core/row_storage.cc`, `ResolveField` returns `Status::NotFound` for a missing field before validating the supplied accessor metadata. In `include/kv_index/row.h`, both accessor overloads convert `kNotFound` to `std::nullopt` or an empty `ListView`. This violates the W01 requirement that row read mismatch must be observable and must never be reported as ordinary absence. A concrete failing scenario is using an accessor created from an older/newer layout for a field that was deleted or is otherwise absent from the row's current compiled layout.
    - Test gap tied to the failure: `row_decoder_test.cc` covers incompatible accessors only when the field ID still exists in the row layout. It does not cover incompatible accessors whose field ID is absent/deleted in the target row layout.
  - Final decision: fail.
- 2026-04-30 verify agent retry 1:
  - Final decision: pass.
  - Diff/scope inspection:
    - Current changed files are within W01 scope plus controller/task status edits: `BUILD.bazel`, `include/kv_index/schema.h`, `include/kv_index/row.h`, `src/core/byte_io.h`, `src/core/schema.cc`, `src/core/row_storage.h`, `src/core/row_storage.cc`, `tests/BUILD.bazel`, `tests/unit/byte_io_test.cc`, `tests/unit/schema_test.cc`, `tests/unit/row_decoder_test.cc`, `docs/tasks/W01-schema-row-materialization.md`, and `docs/tasks/controller.md`.
    - `docs/tasks/controller.md` was treated as controller status context and was not modified by this verify pass.
  - Retry fix review:
    - `src/core/row_storage.cc::ResolveField` now validates supplied accessor schema version and layout fingerprint before looking up the active field, and returns `FailedPrecondition` instead of `NotFound` when an accessor-backed read references a field ID absent from the target row layout.
    - `tests/unit/row_decoder_test.cc::IncompatibleAccessorsForInactiveFieldsSurfaceMismatch` covers both scalar and list accessors created from an older layout for fields deleted from the target layout, and also confirms direct by-id reads for those inactive fields still return ordinary absence (`std::nullopt` for scalar, empty list for list).
    - Accessor-backed reads still throw on scalar/list shape, physical type, schema version, layout fingerprint, offset, encoding, and presence-bit metadata mismatch before any field offset is read.
  - W01 sanity review:
    - Byte IO pins little-endian fixed-width access, 8-byte alignment helpers, and 16-byte `ValueRef16` encode/decode with bounds checks.
    - Runtime schema tracks active/deleted fields, rejects field ID reuse and invalid encoding/default policies, and compiled layout sorts active fields by `FieldId`, pads the presence bitmap to 8 bytes, aligns fixed slots, uses 16-byte refs, and builds a deterministic fingerprint.
    - Row materialization uses one `EncodedRow` shape for row-slot bytes, arena payloads, and dictionary/list-dictionary/element-dictionary payloads; `Row` owns pinned backing and list/string reads return owning values or copied list views for W01 lifetime safety.
    - No blocking W01 plan-compliance gaps or regressions found in this retry pass.
  - Commands/tests run:
    - `git status --short`: showed W01 implementation files, `docs/tasks/W01-schema-row-materialization.md`, and controller status edit.
    - `git diff --name-status HEAD`: tracked W01/controller edits; untracked W01 additions confirmed with `git ls-files --others --exclude-standard`.
    - `bazel test //tests:row_decoder_test`: passed from cache, 1/1 test passing.
    - `bazel test //tests:byte_io_test`: passed from cache, 1/1 test passing.
    - `bazel test //tests:schema_test`: passed from cache, 1/1 test passing.
    - `bazel test //tests:unit_tests`: passed from cache, 5/5 tests passing.
    - `bazel test //tests:all_tests`: passed from cache, 7/7 tests passing.
    - `bazel test //tests:all_tests --nocache_test_results`: passed, executed 7/7 tests.
    - `git diff --check`: passed.

## Decisions
- 2026-04-30: W00 completed in commit `dc85d5e`; W01 selected as the next active task.
- 2026-04-30: Plan agent concluded the original W01 plan is specific enough at task scope, but W01 should be implemented as serial sub-stages inside one active task to reduce schema/layout/row-decoder coupling risk.
- 2026-04-30: Verify agent returned `fail`. Controller confirmed the root cause: accessor reads can receive `NotFound` before accessor metadata is validated when the accessor field is absent from the row layout. W01 entered fix retry 1.
- 2026-04-30: Verify agent retry 1 returned `pass` after focused tests, `unit_tests`, `all_tests`, and uncached `all_tests`. Controller marked W01 completed and authorized the W01 commit.

## Open Issues
- None for fix retry 1. The accessor-missing-field verification failure has regression coverage and passes focused, unit, all-tests, and uncached all-tests verification.

## Next Step
- W01 completed. Create the W01 atomic commit, then controller may select W02.
