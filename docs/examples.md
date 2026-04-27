# Examples

## Order — medium aggregate

**Location**: `examples/order/`

A compact event-sourced service with commands, projections, snapshots, workflow
wiring, and an HTTP server.

| File | Purpose |
|------|---------|
| `contracts.ts` | Domain events, commands, state, and RPC/entity definitions |
| `aggregate.ts` | Business rules and state transitions |
| `entity.ts` | Entity and stateful RPC handlers |
| `events.ts` | EventGroup wiring |
| `projector.ts` | EventLog projection handlers |
| `snapshots.ts` | Snapshot persistence |
| `workflows.ts` | Workflow example |
| `layers.ts` | Infrastructure composition |
| `server.ts` | Dev HTTP server |

Run it with:

```bash
bun examples/order/server.ts
```

## Profiler — advanced production-style service

**Location**: `services/profiler/`

The production-ops reference: snapshots, transactional outbox, durable publish
workflows, ClickHouse projections, replay, and cluster wiring.

| File | Purpose |
|------|---------|
| `contracts.ts` | Rich event/command/state schemas built with N2 helpers |
| `aggregate.ts` | `N2.define()` with typed evolve/decide maps |
| `entity.ts` | `toEntityLayer` with snapshots, postHandle, and read overrides |
| `projector.ts` | EventLog handlers coordinating projection store and outbox |
| `outbox.ts` | `makeOutboxJsonService` with worker retry defaults |
| `workflows.ts` | `makePublishWorkflow` for durable event publishing |
| `snapshots.ts` | `makeSnapshotService` plus `makeSnapshotOps` |
| `replay.ts` | `makeReplayProgram` for projection rebuilds |
| `layers.ts` | Dev and cluster infrastructure composition |
| `server.ts` | Dev HTTP server |
| `cluster.ts` | Cluster deployment |

Useful commands:

```bash
bun services/profiler/server.ts
bun services/profiler/replay.ts --dry-run
```
