# N2 -- DDD patterns for Effect-TS

A thin convention layer for building event-sourced microservices. N2 provides the DDD patterns (aggregates, events, projections). Effect provides everything else.

## What N2 adds

```
AggregateDefinition  →  decide(state, command) → events
                         evolve(state, event) → state
                         handleCommand(state, command) → { events, state }
```

That's it. One abstraction. The rest is Effect, used directly.

## Quick start

```bash
bun install
bun test          # 21 tests
```

## Define an aggregate

```ts
// contracts.ts -- Schema.TaggedRequest commands carry success/failure types
export class CreateOrder extends Schema.TaggedRequest<CreateOrder>("CreateOrder")(
  "CreateOrder",
  { failure: OrderError, success: CommandResult, payload: { orderId: Schema.String, customerId: Schema.String } }
) {}

// aggregate.ts -- pure business logic
export const decide = (state, command) => {
  switch (command._tag) {
    case "CreateOrder":
      if (state.status !== "empty") return yield* new OrderError(...)
      return [new OrderCreated({ orderId: cmd.orderId, ... })]
  }
}

export const evolve = (state, event) => {
  switch (event._tag) {
    case "OrderCreated": return { ...state, status: "draft", ... }
  }
}

export const handleCommand = (state, command) =>
  Effect.gen(function*() {
    const events = yield* decide(state, command)
    let newState = state
    for (const event of events) newState = evolve(newState, event)
    return { events, state: newState }
  })
```

## Expose via cluster Entity

```ts
// entity.ts -- stateful actor with Ref
export const OrderEntityLayer = OrderEntity.toLayer(
  Effect.gen(function*() {
    const address = yield* Entity.CurrentAddress
    const stateRef = yield* Ref.make(initialOrderState)
    let revision = 0

    const dispatch = (command) =>
      Effect.gen(function*() {
        const state = yield* Ref.get(stateRef)
        const result = yield* handleCommand(state, command)
        yield* Ref.set(stateRef, result.state)
        revision += result.events.length
        return new CommandResult({ orderId: address.entityId, revision })
      })

    return OrderEntity.of({
      CreateOrder: (req) => dispatch(new CreateOrder(req.payload)),
      AddItem: (req) => dispatch(new AddItem(req.payload)),
    })
  })
)
```

## Projections (native Effect)

```ts
// projector-native.ts -- fully typed, transactional
export const OrderProjectionHandlers = EventLog.group(
  OrderEventGroup,
  (handlers) =>
    handlers
      .handle("OrderCreated", ({ payload }) => ...)  // payload fully typed
      .handle("ItemAdded", ({ payload }) => ...)
)
```

## Testing

```ts
// Pure domain logic -- no infrastructure
const { events, state } = await run(
  handleCommand(emptyState, new CreateOrder({ orderId: "1", customerId: "c1" }))
)
expect(state.status).toBe("draft")

// Cluster integration -- Entity.makeTestClient
const makeClient = yield* Entity.makeTestClient(OrderEntity, EntityLayer)
const client = yield* makeClient("order-1")
const result = yield* client.CreateOrder({ orderId: "1", customerId: "c1" })
```

## Infrastructure -- use Effect directly

N2 doesn't wrap Effect's infrastructure. Use it directly:

### Authentication

```ts
import { RpcMiddleware } from "@effect/rpc"

class AuthMiddleware extends RpcMiddleware.Tag<AuthMiddleware>()(
  "Auth", { provides: AuthContext, failure: Schema.String }
) {}

// Wire: OrderRpcs.middleware(AuthMiddleware)
```

### Observability

```ts
// Tracing -- built into Effect
Effect.withSpan("handle-command", { attributes: { orderId } })

// Metrics
const commandsTotal = Metric.counter("commands.total", { incremental: true })
Effect.tap(() => Metric.increment(commandsTotal))

// Structured logging
Effect.annotateLogs({ orderId, aggregateType: "Order" })
```

### Configuration

```ts
import * as Config from "effect/Config"

const port = yield* Config.number("PORT").pipe(Config.withDefault(3000))
const dbUrl = yield* Config.string("DATABASE_URL")
```

### Health checks

```ts
import * as HttpRouter from "@effect/platform/HttpRouter"

HttpRouter.get("/health", HttpServerResponse.json({ status: "ok" }))
HttpRouter.get("/ready", checkDeps.pipe(
  Effect.map(() => HttpServerResponse.json({ status: "ok" })),
  Effect.catchAll(() => HttpServerResponse.json({ status: "down" }, { status: 503 }))
))
```

### Retry + Circuit breaker

```ts
import * as Schedule from "effect/Schedule"

effect.pipe(
  Effect.retry(Schedule.exponential("100 millis").pipe(Schedule.intersect(Schedule.recurs(3))))
)
```

### Graceful shutdown

```ts
Effect.addFinalizer(() => Effect.log("draining..."))
// Effect.runFork handles SIGTERM/SIGINT via runtime
```

### Cross-service communication

```ts
// Option 1: Cluster entities call each other via Sharding
const inventoryClient = yield* InventoryEntity.client
const stockClient = yield* inventoryClient("SKU-001")
yield* stockClient.ReserveStock({ sku: "SKU-001", quantity: 5, orderId })

// Option 2: EntityProxy exposes entities over HTTP
const ProxyRpcs = EntityProxy.toRpcGroup(OrderEntity)
const ProxyHandlers = EntityProxyServer.layerRpcHandlers(OrderEntity)

// Option 3: EventLogRemote for cross-service event sync
import { EventLogRemote } from "@effect/experimental"
```

### SQL persistence

```ts
// Already provided in src/adapters/postgres/
import { PgEventLog } from "./adapters/postgres/PgEventLog.js"
import { PgSnapshotStore } from "./adapters/postgres/PgSnapshotStore.js"
```

## Framework files (16 total)

```
framework/
  contracts/    EventEnvelope
  domain/       AggregateDefinition, BrandedId, Revision
  runtime/      EventLog, EventJournalEventLog, SnapshotStore, Clock, IdGenerator
  projection/   ProjectorDefinition, CheckpointStore, Replay
  testing/      TestClock, DeterministicIdGenerator, ProjectorTestHarness
```

## Native Effect APIs (use directly)

| Concern | Effect API |
|---|---|
| Entities | `@effect/cluster` Entity, Sharding, EntityProxy |
| RPC | `@effect/rpc` Rpc, RpcGroup, RpcServer, RpcMiddleware |
| Workflows | `@effect/workflow` Workflow, Activity, DurableClock |
| Events | `@effect/experimental` EventLog, EventGroup, EventJournal |
| Persistence | `@effect/experimental` Persistence |
| Caching | `@effect/experimental` PersistedCache |
| Reactivity | `@effect/experimental` Reactivity |
| SQL | `@effect/sql-pg` |
| HTTP | `@effect/platform` HttpRouter, HttpServer |
| IDs | `@effect/cluster` Snowflake |
| Tracing | `Effect.withSpan` |
| Metrics | `Effect.Metric` |
| Config | `effect/Config` |
| Retry | `effect/Schedule` |
| Testing | `@effect/cluster` TestRunner, `@effect/rpc` RpcTest |
