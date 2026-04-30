# W00 Foundation And Build Layout

## Metadata
- Status: completed
- Owner Role: controller
- Depends on: none
- Retry Count: 0
- Last Updated: 2026-04-30

## Scope
Stabilize the repository baseline and create the build/test structure needed for internal modules and white-box tests.

Files in scope:
- Modify: `BUILD.bazel`
- Modify: `tests/BUILD.bazel`
- Create: `include/kv_index/status.h`
- Modify: `include/kv_index/kv_index.h`
- Create: `src/core/hash.h`
- Create: `src/core/hash.cc`
- Modify: `src/core/forward_index.cc`
- Create: `tests/unit/status_test.cc`
- Modify: `tests/unit/forward_index_test.cc`

Required implementation items:
- Run `bazel test //tests:all_tests` and confirm the bootstrap baseline before behavior-changing edits when feasible.
- Split Bazel targets into public headers, public SDK, and internal implementation targets without exposing internal headers to SDK users.
- Add `Status` and `StatusOr<T>` as the common fail-closed primitive.
- Move stable hash helpers into a reusable internal module while preserving the public `StableHash64` declaration.
- Keep current public `ForwardIndex` smoke behavior unchanged except for internal refactoring.
- Run `bazel test //tests:all_tests`.
- Commit with a build/foundation-focused message only after verify passes and controller requests it.

Exit criteria:
- Internal tests can depend on an internal target.
- `Status` and `StatusOr<T>` exist and are covered by tests.
- Stable hash behavior remains deterministic and shard assignment is unchanged.

## Plan Notes
Planning conclusion:
- The original implementation plan is specific enough for W00. Do not redesign the module architecture in this task; execute a narrow foundation refactor that enables later internal targets, adds the shared status primitive, and preserves current public behavior.

Minimal executable breakdown for coding agent:
1. Baseline guard: run `bazel test //tests:all_tests` before behavior-changing edits when feasible. If it fails before any edit, record the exact failure in `Implementation Log` and stop for controller guidance rather than hiding a pre-existing baseline issue.
2. Add tests first: create `tests/unit/status_test.cc` covering `Status::Ok`, error status code/message behavior, `StatusOr<T>` value and error states, fail-closed construction from an OK status without a value, and a move-only value type.
3. Pin hash behavior before moving code: extend `tests/unit/forward_index_test.cc` with golden checks from the current implementation, including `StableHash64(0, 0, 1) == 12085254679833835651`, `StableHash64(1, 0, 1) == 10858953248184931039`, `StableHash64(42, 17, 1) == 13083006257041843452`, and shard assignment `ForwardIndexOptions{shard_count = 8, hash_seed = 17}.ShardFor(42) == 4`.
4. Split root Bazel targets: keep `//:kv_index` as the public SDK target, add a public-header target for SDK headers, and add an internal implementation target that owns `src/core/forward_index.cc`, `src/core/hash.cc`, and internal headers with visibility limited to the root package and `//tests`.
5. Add `include/kv_index/status.h`: keep it standard-library-only and header-only for W00 unless implementation complexity clearly requires a source file, because no `status.cc` is in W00 scope.
6. Export status from the umbrella header: include `kv_index/status.h` from `include/kv_index/kv_index.h`.
7. Move hashing into `src/core/hash.h` and `src/core/hash.cc`: preserve the public declaration of `kv_index::StableHash64` in `include/kv_index/forward_index.h`, keep `ForwardIndex::ShardFor` behavior unchanged, and do not put `src/core/hash.h` in public SDK headers.
8. Update `tests/BUILD.bazel`: add a `status_unit_test` target, include it in `unit_tests` and `all_tests`, and make at least one unit test depend on the new internal implementation target to prove white-box test access.
9. Run focused tests, then `bazel test //tests:all_tests`. Do not commit unless the controller later requests it after verify passes.

Implementation boundaries:
- In scope: only the files listed in this task scope, plus behavior-preserving refactoring inside those files.
- Out of scope: schema, row materialization, snapshots, shard directory, Kafka, async load behavior changes, new dependencies, and any public API redesign beyond adding `Status` and `StatusOr<T>`.
- Public SDK users must still depend on `//:kv_index` and include only `kv_index/...` headers.
- Internal headers under `src/core` may be visible to tests through the internal target, but must not be exposed through the public SDK target.
- Keep current bootstrap `ForwardIndex` semantics: invalid non-power-of-two shard counts throw `std::invalid_argument`, empty `Get`/`MGet` miss, `MGet` preserves shape/order, `LoadAsync` still returns a failed terminal bootstrap state, and `CancelLoad` still returns false for terminal loads.

Dependency assumptions:
- Bazel is the only build and test entry point.
- C++20 and the existing standard library are sufficient for W00; do not add Abseil.
- Existing test macros in `tests/test_support/test_macros.h` are sufficient; add small local test helpers in test files only if needed.
- Existing public `StableHash64` is intentionally public for now; W00 only moves its implementation, not its declaration.
- Later workstreams will build on `Status` and `StatusOr<T>`, so keep names and basic methods simple and stable.

Risks:
- The root `BUILD.bazel` currently has public package default visibility, so internal targets must set explicit restricted visibility or they will leak implementation headers.
- Target splitting can accidentally drop `src/version.cc` or public headers from `//:kv_index`; smoke tests should catch umbrella-header and link regressions.
- Hash refactoring is behavior-sensitive; use golden values and shard checks to catch accidental seed/version/mix changes.
- Over-designing `Status` in W00 could constrain later error modeling; implement only the common primitives needed by the plan.
- If the bootstrap baseline fails before edits, the coding agent should stop and report rather than mixing environment repair with W00 implementation.

Concrete acceptance criteria:
- `//:kv_index` remains the public SDK target and does not expose `src/core/hash.h` as a public header.
- Internal unit tests can depend on an internal implementation target from `tests/BUILD.bazel`.
- `include/kv_index/status.h` defines usable `Status` and `StatusOr<T>` primitives and `include/kv_index/kv_index.h` exports them.
- `tests/unit/status_test.cc` covers OK status, non-OK status, value `StatusOr`, error `StatusOr`, fail-closed no-value behavior, and move-only values.
- Stable hash golden values listed above pass after moving the implementation.
- Current `ForwardIndex` public smoke behavior remains unchanged.
- `bazel test //tests:all_tests` passes after implementation.

Prioritized focused tests:
- Before edits: `bazel test //tests:all_tests`.
- After adding status: `bazel test //tests:status_unit_test`.
- After target split and hash move: `bazel test //tests:forward_index_unit_test`.
- Before handing to verify: `bazel test //tests:unit_tests` and `bazel test //tests:all_tests`.

## Implementation Log
- 2026-04-30 coding agent:
  - Baseline guard before behavior-changing edits: `bazel test //tests:all_tests` passed with 3/3 existing test targets passing.
  - Added tests first for `Status`/`StatusOr<T>` and stable hash golden behavior. Red checks:
    - `bazel test //tests:status_unit_test` failed as expected because `kv_index/status.h` did not exist.
    - `bazel test //tests:forward_index_unit_test` failed as expected because `//:kv_index_internal` did not exist.
  - Split root Bazel layout into `//:kv_index_public_headers`, restricted `//:kv_index_internal`, restricted `//:kv_index_version`, and public SDK target `//:kv_index` using `implementation_deps` for implementation linkage.
  - Added standard-library-only header `include/kv_index/status.h` with `StatusCode`, `Status`, and `StatusOr<T>`, and exported it from `include/kv_index/kv_index.h`.
  - Moved stable hash implementation into `src/core/hash.h` and `src/core/hash.cc`; preserved public `kv_index::StableHash64` and existing `ForwardIndex::ShardFor` behavior.
  - Updated `tests/BUILD.bazel` with `status_unit_test`; `unit_tests` and `all_tests` now include it. `forward_index_unit_test` depends on `//:kv_index_internal` and includes `src/core/hash.h` for white-box access.
  - Files changed by coding agent: `BUILD.bazel`, `tests/BUILD.bazel`, `include/kv_index/status.h`, `include/kv_index/kv_index.h`, `src/core/hash.h`, `src/core/hash.cc`, `src/core/forward_index.cc`, `tests/unit/status_test.cc`, `tests/unit/forward_index_test.cc`, and this implementation log section.
  - Commands/tests after implementation:
    - `bazel test //tests:status_unit_test` passed.
    - `bazel test //tests:forward_index_unit_test` passed.
    - `bazel test //tests:unit_tests` passed with 2/2 targets passing.
    - `bazel test //tests:all_tests` passed with 4/4 targets passing.
    - `bazel test //tests:all_tests --nocache_test_results` passed with 4/4 targets executed and passing.
    - `bazel test //tests:unit_tests --nocache_test_results` passed with 2/2 targets executed and passing.
  - Pre-existing workspace note: `docs/tasks/controller.md` was already modified before coding and was left untouched.

## Verification Log
- 2026-04-30 verify agent:
  - Final decision: pass.
  - Diff/scope review:
    - Reviewed current working-tree changes for W00. Changed implementation files are within the W00 expected set: `BUILD.bazel`, `tests/BUILD.bazel`, `include/kv_index/status.h`, `include/kv_index/kv_index.h`, `src/core/hash.h`, `src/core/hash.cc`, `src/core/forward_index.cc`, `tests/unit/status_test.cc`, and `tests/unit/forward_index_test.cc`.
    - `docs/tasks/controller.md` is also modified, but treated as controller-only status work and not reviewed as W00 implementation scope.
    - `git diff master...HEAD` could not run because no local `master` ref exists in this workspace; reviewed `git status --short`, `git diff --name-status`, `git diff --stat`, and targeted file diffs instead.
  - Implementation review findings:
    - `Status` and `StatusOr<T>` in `include/kv_index/status.h` use only C++ standard library headers and are covered by `tests/unit/status_test.cc` for OK/error status, value/error `StatusOr`, fail-closed OK-status construction, and move-only values.
    - Public `kv_index::StableHash64` remains declared in `include/kv_index/forward_index.h`; implementation moved through `src/core/hash.cc`, and golden values plus shard assignment are pinned in `tests/unit/forward_index_test.cc`.
    - `src/core/hash.h` is not included by public SDK headers. Bazel query shows public headers are only `include/kv_index/...`; `src/core/hash.h` belongs to restricted `//:kv_index_internal`, which is visible only to `//:__pkg__` and `//tests:__pkg__`.
    - Existing public `ForwardIndex` bootstrap behavior is preserved: invalid non-power-of-two shard counts still throw, empty `Get`/`MGet` still miss while preserving `MGet` shape/order, `LoadAsync` remains a failed terminal bootstrap state, and `CancelLoad` still returns false for terminal loads.
    - No blocking regressions or plan-compliance gaps found.
  - Commands/tests run:
    - `bazel test //tests:status_unit_test` passed; cached result, 1/1 test passing.
    - `bazel test //tests:forward_index_unit_test` passed; cached result, 1/1 test passing.
    - `bazel test //tests:unit_tests` passed; cached result, 2/2 tests passing.
    - `bazel test //tests:all_tests` passed; cached result, 4/4 tests passing.
    - `bazel test //tests:all_tests --nocache_test_results` passed; executed 4/4 tests and all passed.
    - `git diff --check` passed.

## Decisions
- 2026-04-30: Selected as first active task because it has no dependencies.
- 2026-04-30: Plan agent concluded the original W00 implementation plan is already specific enough; added a minimal executable breakdown and no speculative redesign.
- 2026-04-30: Verify agent passed W00 after focused tests and implementation review. Controller marked W00 completed and authorized the W00 commit.

## Open Risks
- No open W00 verification risks.

## Next Step
- W00 completed. Create the W00 atomic commit, then controller may select W01.
