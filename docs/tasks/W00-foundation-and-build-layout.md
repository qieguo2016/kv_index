# W00 Foundation And Build Layout

## Metadata
- Status: planning
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
- Pending plan agent.

## Implementation Log
- Pending coding agent.

## Verification Log
- Pending verify agent.

## Decisions
- 2026-04-30: Selected as first active task because it has no dependencies.

## Open Issues
- None yet.

## Next Step
- Dispatch plan agent.
