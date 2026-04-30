# W01 Schema, Layout, Row Semantics, And Row Materialization Contract

## Metadata
- Status: pending
- Owner Role: controller
- Depends on: W00
- Retry Count: 0
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
- Pending.

## Implementation Log
- Pending.

## Verification Log
- Pending.

## Decisions
- Pending W00 completion.

## Open Issues
- None yet.

## Next Step
- Wait for W00.
