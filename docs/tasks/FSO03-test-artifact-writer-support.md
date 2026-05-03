# FSO03 Test Artifact Writer Support

## Metadata
- Status: completed
- Owner Role: controller
- Depends on: FSO02
- Retry Count: 0
- Last Updated: 2026-05-02 CST

## Scope
Let shared test artifact writer omit the source-progress section while preserving correct section counts and existing defaults.

## Plan Notes
### Implementation Boundary
- Only implement Task 3 from
  `docs/superpowers/plans/2026-05-02-full-snapshot-only-mode.md`.
- Modify the shared test artifact writer so tests can omit the global
  `ArtifactSectionType::kSourceProgress` section while preserving the existing
  default behavior.
- Add `bool include_source_progress_section = true` to
  `kv_index::test_support::TestArtifactSpec`.
- In `WriteTestArtifact`, keep schema metadata mandatory and write the
  source-progress section only when `include_source_progress_section` is true.
  Compute the header section count from the actual sections:
  `1 + (include_source_progress_section ? 1 : 0) + shard_count * 7`.
- Update the FSO02 missing-source-progress tests in
  `tests/unit/artifact_format_test.cc` to build omitted-source-progress
  artifacts with:

  ```cpp
  TestArtifactSpec spec = ValidSpec();
  spec.include_source_progress_section = false;
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());
  ```

- Remove the now-unused local source-progress section-removal helper and any
  includes, using declarations, or constants that were only needed by that
  helper. Keep helpers still needed by the malformed-present-source-progress
  test.
- Do not change parser policy behavior, mmap loading, async-load branching,
  full-snapshot-only publication, BUILD files, README, or any FSO04+ behavior.

### Dependencies
- FSO02 is verified and provides
  `ArtifactSourceProgressPolicy::kOptional` plus parser tests for missing and
  malformed source-progress sections.
- Current `tests/unit/artifact_format_test.cc` contains a temporary
  `RemoveSourceProgressSection` byte-rewrite helper from FSO02; FSO03 should
  replace only the missing-section test setup with shared writer support.
- Current `tests/test_support/artifact_writer.cc` emits sections in this order:
  schema metadata, source progress, then seven sections per shard.
- `SecondSectionOffset`, source-progress corruption helpers, and any code used
  by existing range/checksum/malformed tests must remain available.

### Risk Points
- The writer option must default to including source progress so all existing
  tests and default realtime artifacts keep their current behavior.
- The omitted-section artifact must have a correct header section count,
  directory size, directory entries, payload offsets, and checksums. It should
  be produced directly by the writer, not by post-processing serialized bytes.
- Do not serialize an empty source-progress payload when the option is false;
  the `kSourceProgress` section must be absent.
- Keep malformed-present-source-progress coverage intact: optional parser
  policy must still reject a present but malformed source-progress section.
- Be careful when removing local test includes/usings/constants: delete only
  those made obsolete by `RemoveSourceProgressSection`, not those still used by
  `MakeSourceProgressTopicLengthInvalid` or existing corruption tests.

### Acceptance Criteria
- `TestArtifactSpec{}` keeps writing a source-progress section by default.
- Setting `spec.include_source_progress_section = false` makes
  `WriteTestArtifact` produce a valid artifact with no global
  `kSourceProgress` section.
- Default parser policy still rejects the omitted-section artifact with
  `StatusCode::kInvalidArgument`.
- Optional parser policy accepts the omitted-section artifact, returns empty
  `source_progress`, and preserves metadata/layout/shard section
  discoverability.
- Present malformed source-progress is still rejected under optional policy.
- `artifact_format_test.cc` no longer contains the FSO02 local
  `RemoveSourceProgressSection` helper or helper-only includes/usings.
- No source parser, mmap loader, async-load, BUILD, README, or full-only serving
  behavior is changed in this task.

### Focused Tests
- Preferred coding-agent loop:
  - First update the missing-source-progress tests to use
    `include_source_progress_section = false`, then run
    `bazel test //tests:artifact_format_test` and confirm RED because the writer
    option does not exist.
  - Add the writer option and implementation.
  - Run `bazel test //tests:artifact_format_test` and confirm GREEN.
  - Run `git diff --check -- tests/test_support/artifact_writer.h tests/test_support/artifact_writer.cc tests/unit/artifact_format_test.cc docs/tasks/FSO03-test-artifact-writer-support.md`.
- Do not run aggregate or final acceptance suites for FSO03.

## Implementation Log
- Coding completed on 2026-05-02 CST.
- Tests changed:
  - Updated missing-source-progress parser tests in
    `tests/unit/artifact_format_test.cc` to set
    `TestArtifactSpec::include_source_progress_section = false`.
  - Removed the temporary `RemoveSourceProgressSection` byte-rewrite helper and
    helper-only test plumbing while keeping malformed-present-source-progress
    corruption coverage intact.
- Files changed:
  - `tests/test_support/artifact_writer.h`
  - `tests/test_support/artifact_writer.cc`
  - `tests/unit/artifact_format_test.cc`
  - `docs/tasks/FSO03-test-artifact-writer-support.md`
- Commands run:
  - `bazel test //tests:artifact_format_test` RED: failed to build because
    `kv_index::test_support::TestArtifactSpec` had no
    `include_source_progress_section` member.
  - `bazel test //tests:artifact_format_test` GREEN: passed after writer
    support was added.
  - `git diff --check -- tests/test_support/artifact_writer.h tests/test_support/artifact_writer.cc tests/unit/artifact_format_test.cc docs/tasks/FSO03-test-artifact-writer-support.md`:
    passed with no whitespace errors.
  - `bazel test --cache_test_results=no //tests:artifact_format_test`:
    passed, executed 1/1 test.
- Implementation notes:
  - Added `bool include_source_progress_section = true` to
    `TestArtifactSpec`, preserving default source-progress emission.
  - Updated `WriteTestArtifact` to compute section count from actual emitted
    sections and omit `ArtifactSectionType::kSourceProgress` entirely when the
    option is false.
- Risks:
  - Aggregate suites were not run, per FSO03 scope.
  - No mmap loading, async-load, BUILD, README, or FSO04+ behavior was changed.

## Verification Log
- Verified on 2026-05-02 CST.
- Scoped diff reviewed:
  - `tests/test_support/artifact_writer.h`
  - `tests/test_support/artifact_writer.cc`
  - `tests/unit/artifact_format_test.cc`
- Review conclusion:
  - `TestArtifactSpec::include_source_progress_section` defaults to `true`,
    preserving default source-progress emission.
  - `WriteTestArtifact` omits `ArtifactSectionType::kSourceProgress` entirely
    when the option is false and computes the header section count from the
    actually emitted sections.
  - Missing-source-progress parser tests now use the shared writer option.
  - `artifact_format_test.cc` no longer contains
    `RemoveSourceProgressSection`.
- Commands run:
  - `git diff --check -- tests/test_support/artifact_writer.h tests/test_support/artifact_writer.cc tests/unit/artifact_format_test.cc`:
    passed with no whitespace errors.
  - `bazel test //tests:artifact_format_test`: passed from cache.
  - `bazel test --cache_test_results=no //tests:artifact_format_test`: passed,
    executed 1/1 test.
- Conclusion: pass.

## Decisions
- The writer option must default to including source progress.

## Open Issues
- None for FSO03 planning.

## Next Step
- Controller selects FSO04.
