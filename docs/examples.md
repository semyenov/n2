# Examples

## Inventory — minimal aggregate

**Location**: `src/examples/inventory/`

The simplest possible aggregate: 2 events, 2 commands, no projections/outbox.

| File | Purpose |
|------|---------|
| `contracts.ts` | `StockReserved`, `StockReleased` events; `ReserveStock`, `ReleaseStock` commands |
| `aggregate.ts` | `N2.define()` with simple stock math |
| `entity.ts` | `toEntityLayer` + `toStatefulRpcHandlers` |
| `events.ts` | EventGroup with `eventPayloadSchema` |
| `aggregate.test.ts` | Cluster entity integration tests |

Key patterns demonstrated:
- Common event base (`InventoryEventBase`)
- `occurredAt` convention
- `defineCommands().toPersistedEntity()`
- EntityProxy pattern

## Order — medium aggregate with sagas

**Location**: `src/examples/order/`

6 commands including a workflow-based fulfillment saga with compensation.

| File | Purpose |
|------|---------|
| `contracts.ts` | 4 events, 6 commands, `OrderEventBase` |
| `aggregate.ts` | `N2.define()` with status guards |
| `entity.ts` | `toEntityLayer` with `overrides` for GetOrder + FulfillOrder |
| `events.ts` | EventGroup with `eventPayloadSchema` |
| `projector.ts` | EventLog.group handlers (logging only) |
| `workflows.ts` | `OrderFulfillmentWorkflow` with compensation |
| `layers.ts` | Infrastructure composition |
| `http.ts` | HTTP route (`/rpc/orders`) |
| `index.ts` | Dev server entry point |
| `production.ts` | Cluster deployment |
| `bench.ts` | Benchmarks (mitata) |

Key patterns demonstrated:
- `overrides` for read queries (`GetOrder`) and workflows (`FulfillOrder`)
- `toStatefulRpcHandlers` for dev mode
- Saga with `Workflow.withCompensation`
- `DurableClock.sleep` for durable waits

## Profile Provider — advanced aggregate

**Location**: `examples/profile-provider/`

Full production-grade event-sourced service with projections, outbox, ClickHouse, and replay.

| File | Purpose |
|------|---------|
| `contracts.ts` | 7 events with `ProfileEventBase`, 7 commands, rich domain types |
| `aggregate.ts` | `N2.define()` with PII handling, branch forking |
| `entity.ts` | `toEntityLayer` with snapshots + postHandle + overrides |
| `events.ts` | EventGroup with `eventPayloadSchema` |
| `projector.ts` | EventLog.group → per-event store + outbox enqueue |
| `projection-store.ts` | Per-event typed handlers + `makeDispatch` |
| `projection-store-pg.ts` | PostgreSQL read model mutations |
| `projection-store-clickhouse.ts` | ClickHouse append-only projections |
| `outbox.ts` | `makeOutboxService` for transactional outbox |
| `workflows.ts` | `makePublishWorkflow` for durable event publishing |
| `snapshots.ts` | `makeSnapshotService` for aggregate snapshots |
| `layers.ts` | Infrastructure composition (dev + cluster) |
| `replay.ts` | `makeReplayTool` + `makeEventDecoder` for projection rebuild |
| `server.ts` | Dev HTTP server |
| `cluster.ts` | Cluster deployment |
| `clickhouse-schema.ts` | ClickHouse DDL |
| `clickhouse.ts` | ClickHouse client configuration |

Key patterns demonstrated:
- `ProfileEventBase` with `occurredAt`, `actorId`, `revision`
- `postHandle` for non-event-sourced state transforms (source asset merge)
- `afterCommit` for EventLog publishing
- Per-event projection store with `dispatch`
- Outbox decoupled from projections
- Durable publish workflow with exponential backoff
- Event replay from journal into ClickHouse
- Snapshot persistence with `makeSnapshotService`
- `makeTestAggregate` for test infrastructure
