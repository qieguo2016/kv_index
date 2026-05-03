# FSO02 Optional Source Progress Parsing

## Metadata
- Status: completed
- Owner Role: controller
- Depends on: FSO01
- Retry Count: 0
- Last Updated: 2026-05-02 CST

## Scope
Add artifact parser policy so missing `kSourceProgress` is optional only when requested, while malformed present source-progress sections still fail closed.

## Plan Notes
- Implementation boundary:
  - Modify only `src/artifact/artifact_format.h`,
    `src/artifact/artifact_format.cc`, and
    `tests/unit/artifact_format_test.cc`.
  - Add parser-level source-progress policy only. Do not thread this policy
    through mmap loading, async load, `ForwardIndex`, or full-snapshot-only
    publication; those belong to later FSO tasks.
  - Keep the default parser behavior unchanged: `ParseArtifact(bytes, {})`
    must still require a global `kSourceProgress` section.
  - Introduce a narrow public artifact enum, for example
    `ArtifactSourceProgressPolicy::{kRequired, kOptional}`, and add a
    defaulted `source_progress_policy = kRequired` field to
    `ArtifactValidationOptions`.
  - In `ParseArtifact`, change only the missing global source-progress branch:
    if the policy is optional, clear/leave `artifact.source_progress` empty and
    return the otherwise valid parsed artifact; if required, return the existing
    invalid-argument failure. If the section is present, continue to parse it
    exactly as today and propagate malformed-section errors unchanged.

- Dependencies:
  - FSO01 is verified and provides the public serving-mode option, but FSO02
    does not need to reference `ForwardIndexMode`.
  - FSO03 shared test-writer support is not available yet. For this task, tests
    may use a local helper in `tests/unit/artifact_format_test.cc` to remove the
    source-progress section from bytes emitted by `WriteTestArtifact`.

- Suggested local test helper:
  - Build a normal valid artifact with `WriteTestArtifact`, then transform the
    serialized bytes in the test only.
  - Remove the second section directory entry and its payload, because the
    current writer emits sections in this order: schema, source progress, then
    shard-local sections.
  - Decrement the header section count, compact the remaining directory entries,
    erase the source-progress payload bytes, and subtract the removed payload
    length from later section offsets. Use local copies of the artifact header
    and directory offsets, or small local read/write helpers based on
    `src/base/byte_io.h`.
  - Keep this helper private to the test file and delete/replace it in FSO03
    when shared writer support lands.

- Risk points:
  - Do not accidentally make missing source progress optional for default
    callers; realtime/default async-load behavior depends on fail-closed
    parsing.
  - Do not skip parsing when a source-progress section exists. Optional policy
    only tolerates absence; malformed present bytes, bad checksums, duplicate
    entries, invalid offsets, and trailing bytes must still fail.
  - Do not update BUILD files, README, mmap load options, async-load branching,
    or the shared test artifact writer in this task.
  - The local byte-removal helper must preserve all remaining section checksums
    and offsets so test failures exercise the policy, not accidental file
    corruption.

- Acceptance criteria:
  - `ArtifactValidationOptions{}` keeps requiring source progress.
  - `ArtifactValidationOptions{.source_progress_policy = kOptional}` accepts an
    otherwise valid artifact with no global source-progress section.
  - The accepted parsed artifact has an empty `source_progress` vector and still
    has valid metadata, layout, shard count, hash seed/version, sections, and
    row section discoverability.
  - Optional policy still rejects a present but malformed source-progress
    section with a non-ok status.
  - Existing artifact-format tests continue to pass.

- Focused tests:
  - Add `MissingSourceProgressRejectedByDefault`.
  - Add `MissingSourceProgressAllowedWhenOptional`.
  - Add `MalformedSourceProgressRejectedEvenWhenOptional`.
  - Run `bazel test //tests:artifact_format_test` for red/green verification.

## Implementation Log
- Coding completed for FSO02.
- Files changed:
  - `src/artifact/artifact_format.h`
  - `src/artifact/artifact_format.cc`
  - `tests/unit/artifact_format_test.cc`
  - `docs/tasks/FSO02-optional-source-progress-parsing.md`
- Tests added in `tests/unit/artifact_format_test.cc`:
  - `MissingSourceProgressRejectedByDefault`
  - `MissingSourceProgressAllowedWhenOptional`
  - `MalformedSourceProgressRejectedEvenWhenOptional`
- Test-only helper notes:
  - Added private local helpers to remove the second section directory entry
    and source-progress payload from serialized artifact bytes.
  - Added a private local helper to corrupt the present source-progress payload
    while recomputing its checksum, so optional policy still exercises parser
    validation rather than checksum failure.
- Implementation notes:
  - Added `ArtifactSourceProgressPolicy::{kRequired, kOptional}`.
  - Added
    `ArtifactValidationOptions::source_progress_policy = kRequired`.
  - Changed only the missing global `kSourceProgress` branch in
    `ParseArtifact`: optional policy returns the parsed artifact with empty
    `source_progress`; required policy keeps the existing invalid-argument
    failure.
  - Present source-progress sections still use the existing
    `ParseSourceProgressSection` path and propagate malformed-section errors.
- Commands run and results:
  - `bazel test //tests:artifact_format_test` RED: failed to build because
    `ArtifactSourceProgressPolicy` / `source_progress_policy` did not exist
    after the new tests were added. An earlier RED attempt also exposed missing
    test-helper `Status`/`StatusOr` using declarations, which were fixed before
    recording the clean missing-policy RED.
  - `bazel test //tests:artifact_format_test` GREEN: passed after adding the
    parser policy.
  - `git diff --check -- src/artifact/artifact_format.h src/artifact/artifact_format.cc tests/unit/artifact_format_test.cc`
    passed with no whitespace errors.
  - `bazel test //tests:artifact_format_test` GREEN after minor test cleanup:
    passed.
  - `bazel test --cache_test_results=no //tests:artifact_format_test` final
    focused verification: passed, executed 1/1 test.
- Risks:
  - No mmap loader, async-load, shared writer, BUILD, README, or full-only
    serving behavior was changed in this task.
  - Aggregate suites were not run; FSO02 handoff is limited to the focused
    parser test requested by the task.

## Verification Log
- Verify agent reviewed the current FSO02 diff for:
  - `src/artifact/artifact_format.h`
  - `src/artifact/artifact_format.cc`
  - `tests/unit/artifact_format_test.cc`
- Review result:
  - Default `ArtifactValidationOptions{}` keeps requiring a global
    `kSourceProgress` section.
  - `ArtifactSourceProgressPolicy::kOptional` changes only the missing-section
    branch and returns an otherwise validated parsed artifact with empty
    `source_progress`.
  - Present source-progress sections still flow through
    `ParseSourceProgressSection`; malformed present bytes are still rejected.
  - The focused tests cover default rejection, optional acceptance, metadata and
    row-section discoverability, and malformed-present rejection.
- Commands run and results:
  - `git diff -- src/artifact/artifact_format.h src/artifact/artifact_format.cc tests/unit/artifact_format_test.cc`
    reviewed the scoped implementation diff.
  - `bazel test //tests:artifact_format_test` passed from cache:
    `Executed 0 out of 1 test: 1 test passes`.
  - `bazel test --cache_test_results=no //tests:artifact_format_test` passed
    uncached: `Executed 1 out of 1 test: 1 test passes`.
- Conclusion: pass.

## Decisions
- Must preserve default parser behavior as source-progress required.
- Optional source-progress is a parser policy only in FSO02; no loader or
  serving-mode behavior changes are in scope.

## Open Issues
- None for FSO02 planning.

## Next Step
- Controller selects FSO03.
