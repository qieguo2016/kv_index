# W05 Kafka Update Pipeline

## Metadata
- Status: verifying
- Owner Role: verify agent
- Depends on: W04
- Retry Count: 0
- Last Updated: 2026-04-30

## Scope
Implement the Kafka-facing update path as one module covering message models, consumer interface, update application, coordinator commit semantics, and multi-generation routing.

Files in scope:
- Modify: `include/kv_index/types.h`
- Create: `src/core/kafka_update_consumer.h`
- Create: `src/core/kafka_update_consumer.cc`
- Create: `src/core/update_applier.h`
- Create: `src/core/update_applier.cc`
- Create: `src/core/update_coordinator.h`
- Create: `src/core/update_coordinator.cc`
- Create: `tests/unit/kafka_update_consumer_test.cc`
- Create: `tests/unit/update_applier_test.cc`
- Create: `tests/unit/update_coordinator_test.cc`
- Modify: `tests/BUILD.bazel`

Required implementation items:
- Define Kafka partition, checkpoint, and progress models around per-partition positions instead of a single scalar watermark.
- Add a fake/test consumer seam without introducing a public pluggable data-source abstraction.
- Implement `UpdateApplier` so it validates payloads against the target generation schema/layout and writes to realtime only after full validation succeeds.
- Replace the placeholder test-only payload story with one documented production row payload decode boundary.
- Implement `UpdateCoordinator` batch apply and commit semantics: commit only after the whole batch is locally published.
- Implement multi-generation routing for active-generation plus rebuild/rebase/compaction targets without forking row encoding or source-position logic.
- Add `librdkafka` binding only after fake-consumer tests are green.
- Run `bazel test //tests:unit_tests`.
- Commit with a kafka-update-pipeline message only after verify passes and controller requests it.

Exit criteria:
- Kafka models, payload decode, apply semantics, and commit semantics are defined in one place.
- Multi-generation routing reuses the same row materialization and source-ordering logic.
- Tests do not require a live broker.

## Plan Notes
Planning conclusion:
- The original implementation plan is specific enough to define the W05 module boundary, required behavior, and exit criteria. W05 is too broad for one undifferentiated coding pass, so keep W05 as the single active task and execute the serial sub-stages below.
- Current code context: W04 commit `2e4521f` provides `SourcePosition` as partition/offset ordering metadata, `RealtimeDeltaAtomicTable::Publish(primary_key, SourcePosition, EncodedRow)`, append-only immutable row refs, and `ShardState` precedence `realtime_delta -> compact_delta -> full_snapshot`. W01/W02 provide the shared `kv_index::internal::EncodedRow` materialization contract and `Row` decode path. W03 provides `ShardDirectory` and test-peer patterns, but `ShardState` intentionally exposes no writable realtime accessor.
- Required scope clarification: the W05 file list omits root `BUILD.bazel`, but new W05 source files cannot enter `//:kv_index_internal` without it. The list includes `tests/BUILD.bazel`, which is necessary for the three unit tests. Adding the production `librdkafka` binding may also require `MODULE.bazel` and/or a third-party/local `cc_import` target; the controller should authorize the exact dependency wiring before the coding agent reaches that stage.

Minimal executable breakdown for coding agent:
1. Baseline and model wiring:
   - Inspect `include/kv_index/types.h`, `src/core/realtime_delta.h`, `src/core/shard_state.h`, `src/core/shard_directory.h`, `src/core/row_storage.h`, current `BUILD.bazel`, and `tests/BUILD.bazel`.
   - Add W05 source/test targets to Bazel early enough for test-first work, after controller authorizes the root `BUILD.bazel` scope gap.
   - In `include/kv_index/types.h`, define Kafka models using standard library containers first: `KafkaPartition`, `KafkaPosition`, `KafkaCheckpoint`, `KafkaPartitionProgress`, `KafkaProgress`, `PollOptions`, `KafkaMessageMetadata`, and `KafkaUpsertMessage`.
   - Document offset semantics explicitly: message positions use the consumed message offset for source ordering; commit checkpoints store the next offset to consume. Tests must cover the `last_message_offset + 1` rule.
   - Keep checkpoints/progress per topic+partition. Because W04 `SourcePosition` only carries numeric partition+offset, W05 should either validate a single logical update topic per stream before converting to `SourcePosition`, or explicitly fail closed for cross-topic same-key ordering until `SourcePosition` is extended in a later authorized scope.
2. Internal Kafka consumer seam and fake-first tests:
   - Create `src/core/kafka_update_consumer.{h,cc}` with `KafkaUpdateConsumer` as the only production-facing Kafka wrapper. It owns an internal client seam such as `KafkaUpdateConsumerClient`; tests may inject a fake client through internal constructors because this header is in `src/core`, not public SDK headers.
   - Do not add a public pluggable data-source abstraction, `ForwardIndexOptions` hook, or non-Kafka source interface.
   - Cover fake-client poll, seek, per-partition progress/lag, commit checkpoint recording, invalid config, and error propagation in `tests/unit/kafka_update_consumer_test.cc`.
   - Keep this stage independent of `librdkafka`; fake-consumer tests should compile and pass before any external Kafka dependency is introduced.
3. Production row payload decode boundary:
   - In `src/core/update_applier.{h,cc}`, define one documented versioned Kafka row payload boundary, not a placeholder. Recommended V1 shape: a schema-independent binary "complete row upsert" frame with magic/version, primary-key consistency if present, field count, and typed field values keyed by `FieldId`, including explicit null/absent representation.
   - Decode the raw payload into an internal typed row-update model, then encode that model into `EncodedRow` separately for each target `CompiledRowLayout`. Unknown fields are ignored for older layouts; deleted/non-active fields are not written; missing required fields without defaults must fail validation.
   - Use existing row materialization helpers (`CreateEncodedRow`, scalar/string/list writers, `MaterializeRow`) and local internal helpers only when a currently supported layout encoding lacks a writer helper. Do not fork row-slot encoding, dictionary/list payload semantics, or source-position comparison.
   - Treat malformed payload bytes, duplicate fields, type/list-shape mismatch, unsupported field encoding, invalid null for required field, schema/layout mismatch, and invalid Kafka ordering metadata as non-OK fail-closed statuses.
4. UpdateApplier validation and multi-generation routing:
   - Define internal target route structs in `update_applier.h`, for example generation role (`active`, `rebuild`, `rebase`, `compaction`), generation id, shard count/hash metadata, target layout, and mutable `std::shared_ptr<RealtimeDeltaAtomicTable>` per routed shard.
   - Route each `KafkaUpsertMessage` to every active target generation using the same `StableHash64(primary_key, hash_seed, hash_version)` and one source-position conversion. The same decoded payload must be encoded against each target layout, allowing old active layout to ignore new fields while rebuild layout writes them.
   - Validate all targeted encoded rows before publishing any target, satisfying the "validate before realtime write" requirement. If a later publish fails after earlier target publication, return non-OK so the coordinator does not commit; replay must be safe through W04 idempotent/stale source-position handling.
   - Avoid changing `ShardState` solely to get writable access. Tests and future coordinators can hold mutable realtime table refs while serving states keep shared const views.
5. UpdateCoordinator batch and commit semantics:
   - Create `src/core/update_coordinator.{h,cc}` to poll batches from `KafkaUpdateConsumer`, pass messages to `UpdateApplier`, build per-partition commit checkpoints, and call `Commit` only after the whole batch is locally published.
   - Empty poll batches should not commit. Poll failure, decode/apply failure, invalid routing, or local publish failure must not commit. Commit failure should surface non-OK after local publication; replay remains safe because offsets were not advanced.
   - Compute checkpoints independently per partition, using the largest consumed message offset in the batch plus one. Multi-partition batches must not collapse to a scalar watermark.
   - Use an internal narrow fake seam for coordinator tests if needed, but keep it under `src/core` and do not expose it as a public data-source abstraction.
6. Add `librdkafka` binding last:
   - Only after fake-consumer, applier, and coordinator tests pass, add the concrete `librdkafka` client implementation inside `kafka_update_consumer.cc` or a private helper in the same module.
   - Map `KafkaConsumerConfig` to librdkafka properties, including bootstrap servers, group id, topics, manual commit behavior, seek, poll timeout, partition EOF/lag handling, and offset commit. Do not leak librdkafka handles, callbacks, or error codes outside `KafkaUpdateConsumer`.
   - If the repository has no approved Bazel dependency path for `librdkafka`, stop at the controller-approved dependency decision instead of inventing an untracked build mechanism.
7. Focused verification before handoff:
   - Run focused tests after each sub-stage: `bazel test //tests:kafka_update_consumer_test`, `bazel test //tests:update_applier_test`, and `bazel test //tests:update_coordinator_test`.
   - Run `bazel test //tests:unit_tests` before reporting implementation complete.
   - Do not commit unless the controller later requests it after independent verify passes.

Implementation boundaries:
- In scope for coding after controller authorization: `include/kv_index/types.h`, `src/core/kafka_update_consumer.{h,cc}`, `src/core/update_applier.{h,cc}`, `src/core/update_coordinator.{h,cc}`, the three W05 unit tests, `tests/BUILD.bazel`, and minimal root `BUILD.bazel` / dependency wiring needed to compile them.
- Out of scope: public manual publish APIs, non-Kafka data-source abstractions, async artifact loading, shard cutover execution, delta compaction execution, full rebase builders, mmap artifact loading, observability dashboards, and index-layer delete/tombstone semantics.
- Keep all update pipeline implementation under `kv_index::core` / internal headers. Public SDK users should not see fake consumers, applier target routes, or coordinator seams.
- Preserve W04 source-position behavior. Same-key cross-partition ordering remains fail-closed; equal/lower offsets are replay/idempotency no-ops at the realtime table.
- Preserve W01/W02 row materialization. Kafka decode may produce target-specific `EncodedRow` values, but it must not invent a second row slot or payload-pool format.
- Prefer standard library containers and utilities. Do not add Abseil for maps/checkpoints unless a concrete gap is documented and accepted.

Dependency assumptions:
- W04 commit `2e4521f` is present and is the source of truth for `SourcePosition`, realtime validation, and publish ordering.
- `RealtimeDeltaAtomicTable` can be written through mutable shared pointers owned by the update pipeline while the serving `ShardState` stores const shared pointers to the same table generation.
- W05 can define generation-route snapshots internally without implementing W06/W07 cutover. Later workstreams can construct those route snapshots from real active/rebuild/rebase state.
- The first payload decoder can be a compact, versioned internal binary format as long as it is documented, deterministic, covered by tests, and treated as the production boundary for W05.
- Kafka checkpoint/progress models must support multiple partitions even if the first safe source-position conversion restricts a coordinator stream to one logical update topic.

Risks:
- Offset off-by-one bugs would cause data loss or duplicate replay. Tests must distinguish message source offset from committed next offset.
- W04 `SourcePosition` lacks topic, so multi-topic configs are unsafe unless W05 validates a single logical topic or extends source-position semantics under explicit authorization.
- Multi-target publication cannot be fully atomic after realtime publish. The safe behavior is no commit on any failure and idempotent replay for targets already published.
- Payload decode can accidentally become a test-only shortcut. The coding agent must document the wire frame and reject unsupported values explicitly rather than leaving "real parser later" debt.
- Adding `librdkafka` may require dependency wiring not currently authorized in W05 scope. This should be resolved by the controller before production binding work.
- `ShardState` has no mutable realtime accessor by design. Expanding it would be a scope change; prefer update-owned target refs.

Concrete acceptance criteria:
- Kafka partition/checkpoint/progress models are per partition and document message-offset versus commit-offset semantics.
- Fake-consumer tests verify poll, seek, lag/progress, commit, and error propagation without a live broker.
- `UpdateApplier` decodes one documented production row payload format, validates against every target generation layout, and performs no realtime publish until all targeted rows validate.
- Active/rebuild/rebase/compaction routing uses the same payload decode, row materialization, stable shard hash, and `SourcePosition` conversion.
- Unknown/new fields are ignored by old layouts, target layouts write only active fields, and required missing or type-incompatible fields fail closed.
- `UpdateCoordinator` commits offsets only after every message in a batch has been locally published to all routed targets; no commit happens on poll/apply/publish failure.
- Tests do not require a live Kafka broker.
- `librdkafka` is encapsulated behind `KafkaUpdateConsumer` and does not leak into index update logic.
- `bazel test //tests:unit_tests` passes before handoff to verify.

Prioritized focused tests:
- `bazel test //tests:kafka_update_consumer_test`: fake poll batch ordering, seek checkpoint, per-partition progress/lag, commit checkpoint, config validation, and error propagation.
- `bazel test //tests:update_applier_test`: production payload decode, old/new schema routing, validation-before-publish, malformed payload fail-closed, missing required field fail-closed, stale/equal replay behavior through W04, and multi-generation target publication.
- `bazel test //tests:update_coordinator_test`: empty batch no commit, single-partition commit after success, multi-partition checkpoint aggregation, no commit on apply failure, no commit on poll failure, commit failure propagation, and replay-safe partial local publication.
- Final implementation gate: `bazel test //tests:unit_tests`.

## Implementation Log
- 2026-04-30: Baseline guard before behavior-changing edits: `bazel test //tests:unit_tests` passed (`12` unit targets passed; cached, no failures).
- 2026-04-30: Consumer/model TDD red: added `tests/unit/kafka_update_consumer_test.cc` and `//tests:kafka_update_consumer_test`; focused run failed as expected because `src/core/kafka_update_consumer.h` did not exist.
- 2026-04-30: Consumer/model green: added Kafka per-topic-partition models to `include/kv_index/types.h`, implemented internal fake-injectable `KafkaUpdateConsumer`, and wired `src/core/kafka_update_consumer.{h,cc}` into root Bazel. `bazel test //tests:kafka_update_consumer_test` passed after removing an unused test helper warning.
- 2026-04-30: Applier TDD red: added `tests/unit/update_applier_test.cc` and `//tests:update_applier_test`; focused run failed as expected because `src/core/update_applier.h` did not exist.
- 2026-04-30: Applier green: added documented `KVIU` v1 complete-row upsert decoder, all-target validation-before-publish, W01 row encoding reuse, W04 `SourcePosition` conversion, and active/rebuild/rebase/compaction route publication in `src/core/update_applier.{h,cc}`. `bazel test //tests:update_applier_test` passed after fixing the `StatusOr<T>` to decoded-variant adapter and removing an unused test helper warning.
- 2026-04-30: Coordinator TDD red: added `tests/unit/update_coordinator_test.cc` and `//tests:update_coordinator_test`; focused run failed as expected because `src/core/update_coordinator.h` did not exist.
- 2026-04-30: Coordinator green: added `src/core/update_coordinator.{h,cc}` with empty-poll no-commit, single-topic batch validation before apply, per-partition max consumed offset plus one checkpoint aggregation, no commit on poll/apply failure, and commit failure propagation after local publish. `bazel test //tests:update_coordinator_test` passed.
- 2026-04-30: Production `librdkafka` binding attempted last inside `KafkaUpdateConsumer` using the authorized local `/usr/local/include/librdkafka` and `/usr/local/lib/librdkafka.dylib` paths. `bazel test //tests:kafka_update_consumer_test` failed at link time because `/usr/local/lib/librdkafka.dylib` is `x86_64` while the Bazel target requires `arm64`; W05 stopped per controller instruction instead of bypassing the production binding.
- 2026-04-30: Files changed in this pass: `include/kv_index/types.h`, `src/core/kafka_update_consumer.h`, `src/core/kafka_update_consumer.cc`, `src/core/update_applier.h`, `src/core/update_applier.cc`, `src/core/update_coordinator.h`, `src/core/update_coordinator.cc`, `tests/unit/kafka_update_consumer_test.cc`, `tests/unit/update_applier_test.cc`, `tests/unit/update_coordinator_test.cc`, `tests/BUILD.bazel`, `BUILD.bazel`, and this task log.
- 2026-04-30: Controller installed/confirmed an `arm64` Homebrew `librdkafka` at `/opt/homebrew/opt/librdkafka`; `/opt/homebrew/opt/librdkafka/lib/librdkafka.dylib` is a Mach-O `arm64` shared library and the header exists at `/opt/homebrew/opt/librdkafka/include/librdkafka/rdkafka.h`. W05 may continue by replacing the failed `/usr/local` binding path with this Homebrew path and rerunning the focused gates.
- 2026-04-30: Continuation reproduced the original link failure with `bazel test //tests:kafka_update_consumer_test`: Bazel still ignored `/usr/local/lib/librdkafka.dylib` because it is `x86_64`, leaving `rd_kafka_*` symbols unresolved for `arm64`.
- 2026-04-30: Replaced W05 production binding with the authorized Homebrew arm64 path: `BUILD.bazel` now links `/opt/homebrew/opt/librdkafka/lib/librdkafka.dylib`, and `kafka_update_consumer.cc` includes `<librdkafka/rdkafka.h>` from `/opt/homebrew/opt/librdkafka/include`.
- 2026-04-30: A direct `-I/opt/homebrew/opt/librdkafka/include` and then `-isystem /opt/homebrew/opt/librdkafka/include` failed under Bazel because the absolute include path is outside the execution root. The final BUILD wiring uses `-Xclang -isystem -Xclang /opt/homebrew/opt/librdkafka/include`, which keeps the authorized include path while avoiding Bazel's external include-path rejection.
- 2026-04-30: Focused and aggregate W05 verification passed with the Homebrew binding. All link steps emit a non-blocking warning that Bazel is building for `macOS-11.0` while `/opt/homebrew/opt/librdkafka/lib/librdkafka.1.dylib` was built for newer version `26.0`; tests still execute successfully on this host.

## Verification Log
- 2026-04-30 continuation:
  - `file /opt/homebrew/opt/librdkafka/lib/librdkafka.dylib` reported `Mach-O 64-bit dynamically linked shared library arm64`.
  - `ls -l /opt/homebrew/opt/librdkafka/include/librdkafka/rdkafka.h /opt/homebrew/opt/librdkafka/lib/librdkafka.dylib` confirmed the authorized header and dylib paths exist.
  - `bazel test //tests:kafka_update_consumer_test` first reproduced the `/usr/local` `x86_64` link failure, then passed after switching to the Homebrew binding.
  - `bazel test //tests:update_applier_test` passed.
  - `bazel test //tests:update_coordinator_test` passed.
  - `bazel test //tests:unit_tests` passed: 15 test targets passed.
  - `bazel test //tests:all_tests` passed: 17 test targets passed.
- 2026-04-30 independent verify:
  - Commands run: `git status --short`; targeted `git diff -- BUILD.bazel include/kv_index/types.h src/core/kafka_update_consumer.h src/core/kafka_update_consumer.cc src/core/update_applier.h src/core/update_applier.cc src/core/update_coordinator.h src/core/update_coordinator.cc tests/BUILD.bazel tests/unit/kafka_update_consumer_test.cc tests/unit/update_applier_test.cc tests/unit/update_coordinator_test.cc docs/tasks/W05-kafka-update-pipeline.md`; `git diff -- MODULE.bazel`; `sw_vers`; `file /opt/homebrew/opt/librdkafka/lib/librdkafka.dylib`; `otool -l /opt/homebrew/opt/librdkafka/lib/librdkafka.1.dylib`; `ls -l /opt/homebrew/opt/librdkafka/include/librdkafka/rdkafka.h /opt/homebrew/opt/librdkafka/lib/librdkafka.dylib /opt/homebrew/opt/librdkafka/lib/librdkafka.1.dylib`; focused and aggregate Bazel tests listed below.
  - Review findings: no blocking W05 issues found. Kafka partition/checkpoint/progress models are per topic/partition and document consumed message offset versus committed next offset. The fake consumer seam stays in internal `src/core` headers; no public pluggable data-source abstraction was added. `librdkafka` handles, types, and error enums are contained in `KafkaUpdateConsumer`; applier/coordinator do not include or expose rdkafka APIs. `MODULE.bazel` has no diff.
  - Applier assessment: `UpdateApplier` documents a versioned `KVIU` v1 complete-row payload boundary, decodes once, validates/encodes against all target layouts before publishing, reuses W01 `EncodedRow`/`MaterializeRow` helpers, converts Kafka metadata to W04 `SourcePosition`, and routes active/rebuild/rebase/compaction targets through one shared path.
  - Coordinator assessment: `UpdateCoordinator` validates a single logical topic before apply/source-position conversion, builds commit checkpoints as largest consumed offset plus one per topic/partition, commits only after `ApplyBatch` succeeds, does not commit on poll/apply validation failure, and propagates commit failure after local publish.
  - macOS warning assessment: the Homebrew binding uses the controller-authorized arm64 path `/opt/homebrew/opt/librdkafka`; `file` reports arm64, `otool -l` reports `LC_BUILD_VERSION minos 26.0`, and the local host is macOS `26.0.1`. The newer-macOS deployment-target linker warning is therefore acceptable and non-blocking for this local W05 implementation context. It remains a deployment policy risk if future support must include macOS versions older than 26.0 or if Bazel's deployment target is expected to encode the production support floor.
  - Test results: `bazel test //tests:kafka_update_consumer_test` passed from cache; `bazel test //tests:update_applier_test` passed from cache; `bazel test //tests:update_coordinator_test` passed from cache; `bazel test //tests:unit_tests` passed with 15/15 tests passing from cache; `bazel test //tests:all_tests` passed with 17/17 tests passing from cache.
  - Final conclusion: pass

## Decisions
- 2026-04-30: W04 completed in commit `2e4521f`; W05 selected as the next active task.
- 2026-04-30: Plan agent concluded the original W05 plan is specific enough, but W05 should execute as serial sub-stages inside one active task. Planning identified required root `BUILD.bazel` and likely `librdkafka` dependency wiring scope clarifications.
- 2026-04-30: Controller authorized minimal root `BUILD.bazel` edits for W05 source wiring and `tests/BUILD.bazel` edits for W05 tests.
- 2026-04-30: Controller authorized a local-system librdkafka binding for this workspace using `/usr/local/include/librdkafka` and `/usr/local/lib/librdkafka.dylib`, because those paths exist locally. Do not modify `MODULE.bazel` or download dependencies. If Bazel cannot compile/link this local binding, coding must report `NEEDS_CONTEXT`.
- 2026-04-30: Controller chose not to extend `SourcePosition` with topic in W05; W05 must validate a single logical update topic per coordinator stream before converting Kafka positions to W04 `SourcePosition`.
- 2026-04-30: The `/usr/local` local-system `librdkafka` binding was rejected after link failure because its dylib is `x86_64` only. Controller authorized the existing Homebrew arm64 binding path instead: include path `/opt/homebrew/opt/librdkafka/include`, library path `/opt/homebrew/opt/librdkafka/lib`, and dylib `/opt/homebrew/opt/librdkafka/lib/librdkafka.dylib`. Continue to avoid `MODULE.bazel` changes.

## Open Issues
- No blocking W05 coding issues remain after replacing the `/usr/local` binding with `/opt/homebrew/opt/librdkafka`.
- Remaining risk: link steps warn that the Homebrew `librdkafka.1.dylib` was built for a newer macOS version than Bazel's current `macOS-11.0` deployment target. Tests pass on this host, but independent verification should confirm the warning is acceptable for the intended deployment environment.

## Next Step
- Controller has accepted the independent `pass` verification and authorized a commit-only coding agent to create the W05 atomic commit with exactly one Codex trailer.
