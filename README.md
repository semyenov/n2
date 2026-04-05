# N2 -- Typesafe Microservices on Effect-TS

A minimal framework for DDD + CQRS + Event Sourcing microservices, built on the native Effect ecosystem.

## Stack

- `effect` -- core runtime, Schema, Context, Layer
- `@effect/cluster` -- Entity, Sharding, EntityProxy, Snowflake, MessageStorage, TestRunner
- `@effect/rpc` -- RpcGroup, RpcServer, RpcMiddleware
- `@effect/workflow` -- Workflow, Activity, DurableClock
- `@effect/experimental` -- EventJournal, Machine, Persistence, EventGroup, Reactivity
- `@effect/platform-bun` -- HTTP server
- `@effect/sql-pg` -- Postgres adapters

## Quick start

```bash
bun install
bun test          # 17 tests across 5 files
bun src/examples/order/index.ts  # HTTP server on :3000
```

## Architecture

```
src/
  framework/          # Reusable framework (thin layer over Effect)
    domain/           # AggregateDefinition (decide/evolve), Revision, BrandedId
    runtime/          # AggregateRuntime, EventLog, SnapshotStore, Kafka, OutboxPublisher
    cluster/          # AggregateEntity + native cluster re-exports
    projection/       # Subscription, CheckpointStore, DeadLetter, Replay
    transport/        # MessageAdapter, RpcMiddleware (auth/logging)
    workflow/         # Activities, Compensation
    testing/          # InMemory doubles, AggregateTestHarness, ProjectorTestHarness
    contracts/        # EventEnvelope, Metadata, TopicKey, DLQEnvelope
  adapters/           # Concrete implementations
    kafka/            # KafkaJs publisher + consumer
    postgres/         # SQL EventLog, SnapshotStore, CheckpointStore + migrations
    http/             # BunHttpServer
  examples/
    order/            # Full Order aggregate example
    inventory/        # Inventory aggregate (multi-aggregate)
```

## Define an aggregate

```ts
import * as AggregateDefinition from "./framework/domain/AggregateDefinition.js"

export const OrderAggregate = AggregateDefinition.define({
  name: "Order",
  initialState: { status: "empty", items: [] },

  decide: (state, command) =>
    Effect.gen(function*() {
      // Business rules here. Return events.
      return [new OrderCreated({ ... })]
    }),

  evolve: (state, event) => {
    // Pure state transition. No effects.
    switch (event._tag) {
      case "OrderCreated": return { ...state, status: "draft" }
    }
  },

  schemas: { state: OrderState, command: OrderCommand, event: OrderEvent, error: OrderError }
})
```

## Expose via HTTP (two modes)

### Direct mode (no cluster)

```ts
const OrderRpcs = RpcGroup.make(
  Rpc.make("CreateOrder", { payload: { ... }, success: CommandResult, error: OrderError })
)

const handlers = OrderRpcs.toLayer(OrderRpcs.of({
  CreateOrder: (payload) => orderRuntime.handle(EntityId.make(payload.orderId), ...)
}))

RpcServer.layerHttpRouter({ group: OrderRpcs, path: "/rpc/orders" })
  .pipe(Layer.provide(handlers), Layer.provide(RpcSerialization.layerJson))
```

### Cluster mode (with EntityProxy)

```ts
const OrderEntity = Entity.make("Order", [
  Rpc.make("CreateOrder", {
    payload: { orderId: Schema.String, customerId: Schema.String },
    primaryKey: ({ orderId }) => orderId,
    success: CommandResult,
    error: OrderError
  })
]).annotateRpcs(ClusterSchema.Persisted, true)

// Auto-derived RPC group + handlers
const ProxyRpcs = EntityProxy.toRpcGroup(OrderEntity)
const ProxyHandlers = EntityProxyServer.layerRpcHandlers(OrderEntity)
```

## Run projections

### Kafka-based

```ts
Subscription.make(OrdersViewProjector, {
  topics: ["n2.aggregate.Order.events"],
  groupId: "orders-view",
  fromBeginning: true
})
```

### Native (typed, via EventLog.group)

```ts
EventLog.group(OrderEventGroup, (handlers) =>
  handlers
    .handle("OrderCreated", ({ payload }) => ...)  // payload fully typed
    .handle("ItemAdded", ({ payload }) => ...)
)
```

## Outbox pattern

```ts
import { Singleton } from "@effect/cluster"

Singleton.make("outbox-publisher",
  OutboxPublisher.run({ aggregateType: "Order" })
)
```

## Testing

```ts
// Aggregate test harness (given/when/then)
const harness = AggregateTestHarness.make(OrderAggregate)
const events = await runTest(harness.when(eid("order-1"), new CreateOrderCmd({ ... })))

// Cluster test (Entity.makeTestClient + TestRunner)
const makeClient = yield* Entity.makeTestClient(OrderEntity, EntityBehaviorLayer)
const client = yield* makeClient("order-1")
const result = yield* client.CreateOrder({ ... })

// Workflow test (WorkflowEngine.layerMemory)
const result = yield* MyWorkflow.execute({ orderId: "1" })
```

## Add a new aggregate

1. Define contracts (events, commands, state, errors) in `contracts.ts`
2. Define Entity with RPCs + `primaryKey` for cluster routing
3. Implement `AggregateDefinition.define({ decide, evolve, schemas })`
4. Wire entity behavior with `Entity.toLayer(handlers)`
5. Add to composition root

## Native Effect APIs exposed

The framework re-exports these from `@effect/cluster` and `@effect/experimental`:

| API | Purpose |
|-----|---------|
| `Entity`, `EntityProxy`, `EntityProxyServer` | Distributed entities |
| `Sharding`, `ShardingConfig` | Cluster routing |
| `Snowflake` | Distributed ID generation |
| `MessageStorage`, `SqlMessageStorage` | Command persistence |
| `TestRunner`, `SingleRunner` | Test/dev clusters |
| `Singleton` | Cluster-wide singletons |
| `ClusterWorkflowEngine` | Durable workflows |
| `Persistence`, `PersistedCache` | Key-value storage |
| `EventJournal`, `Reactivity` | Event sourcing + invalidation |
| `Machine` | State machine actors |
| `RpcMiddleware`, `RpcTest` | Middleware + testing |
