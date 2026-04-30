# W05 Kafka Update Pipeline

## Metadata
- Status: pending
- Owner Role: controller
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
- Pending.

## Implementation Log
- Pending.

## Verification Log
- Pending.

## Decisions
- Pending W04 completion.

## Open Issues
- None yet.

## Next Step
- Wait for W04.
