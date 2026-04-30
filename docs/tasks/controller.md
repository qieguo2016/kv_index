# Multi-Agent Controller Ledger

## Metadata
- Current Date: 2026-04-30
- Last Restored: 2026-04-30
- Active Task: W00 Foundation And Build Layout
- Execution Mode: strict serial multi-agent
- Branch: main

## Global Rules
- Only one task may be active at a time.
- Only one sub-agent may run at a time.
- Controller edits only task ledger documents and dispatches role-specific agents.
- Business code changes and focused verification are delegated to role-specific agents.
- A task can enter `completed` only after verify agent records a `pass`.
- Each task allows at most 3 coding/fix verification loops before `blocked_by_human`.
- Coding agent may create the task commit only after controller confirms verify passed.
- Each commit must contain exactly one Codex trailer.

## Task Overview
| Task | Status | Depends On | Retry Count | Task Document |
| --- | --- | --- | --- | --- |
| W00 | planning | none | 0 | docs/tasks/W00-foundation-and-build-layout.md |
| W01 | pending | W00 | 0 | docs/tasks/W01-schema-row-materialization.md |
| W02 | pending | W01 | 0 | docs/tasks/W02-immutable-snapshot.md |
| W03 | pending | W02 | 0 | docs/tasks/W03-serving-read-path.md |
| W04 | pending | W03 | 0 | docs/tasks/W04-realtime-delta.md |
| W05 | pending | W04 | 0 | docs/tasks/W05-kafka-update-pipeline.md |
| W06 | pending | W05 | 0 | docs/tasks/W06-artifact-async-load.md |
| W07 | pending | W06 | 0 | docs/tasks/W07-compaction-full-rebase.md |
| W08 | pending | W07 | 0 | docs/tasks/W08-observability-finalization.md |

## Completed Tasks
- None.

## Blocked By Human
- None.

## Dispatch Log
- 2026-04-30: Initialized controller ledger from `docs/agent_prompt.md` and implementation plan. Selected W00 as the only active task because it has no dependencies.

## Next Dispatch Decision
- Dispatch W00 plan agent.
- Required W00 plan output: implementation boundary, dependency assumptions, risks, concrete acceptance criteria, and prioritized focused tests written to `docs/tasks/W00-foundation-and-build-layout.md`.
