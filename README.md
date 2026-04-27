# N2 — Event-sourced microservices with Effect-TS

A framework for building event-sourced microservices using **Effect-TS** and **Bun**. Define pure business logic once — N2 derives cluster entities, RPC handlers, HTTP routes, snapshots, projections, and event publishing.

## Quick start

```bash
bun install
bun test
bun examples/order/server.ts                # order dev server
bun services/profiler/server.ts             # advanced production-style service
```

## Define an aggregate

```ts
// contracts.ts — schemas are the single source of truth
const OrderEventBase = { orderId: Schema.String, occurredAt: Schema.DateTimeUtc }

class OrderCreated extends Schema.TaggedClass<OrderCreated>()(
  "OrderCreated", { ...OrderEventBase, customerId: Schema.String }
) {}

class CreateOrder extends Schema.TaggedRequest<CreateOrder>("CreateOrder")(
  "CreateOrder", { failure: OrderError, success: CommandResult, payload: { orderId: Schema.String, customerId: Schema.String } }
) {}

const OrderCommands = N2.defineCommands(CreateOrder, AddItem, SubmitOrder, CancelOrder, GetOrder, FulfillOrder)
const OrderEntity = OrderCommands.toPersistedEntity("Order", (p) => p.orderId)

// aggregate.ts — pure business logic with exhaustive type checking
const Order = N2.define<OrderEvent, OrderCommand>()({
  initialState,
  commands: OrderCommands.constructors,
  evolve: {
    OrderCreated: (state, event) => ({ ...state, status: "draft", orderId: Option.some(event.orderId) }),
    // TypeScript enforces all events have handlers
  },
  decide: {
    CreateOrder: (state, cmd) => Effect.gen(function* () {
      if (state.status !== "empty") return yield* new OrderError({ message: "exists" })
      const now = yield* DateTime.now
      return [new OrderCreated({ orderId: cmd.orderId, customerId: cmd.customerId, occurredAt: now })]
    }),
    // TypeScript enforces all commands have handlers
  }
})
```

## Wire to infrastructure

```ts
// entity.ts — cluster entity with lifecycle hooks
const OrderEntityLayer = Order.toEntityLayer(OrderEntity, {
  toResult: ({ entityId, revision }) => new CommandResult({ orderId: entityId, revision }),
  toError: (e) => e instanceof OrderError ? e : new OrderError({ message: String(e) }),
  snapshots: OrderSnapshotOps,                     // optional
  afterCommit: ({ events }) => publishToEventLog(),  // optional
  overrides: {                                       // read queries bypass dispatch
    GetOrder: (cmd, { getState }) => Effect.flatMap(getState, (s) => Effect.succeed(s))
  }
})

// dev mode — stateful multi-entity handlers with metrics
const OrderHandlers = Order.toStatefulRpcHandlers(OrderRpcs, {
  entityId: (cmd) => cmd.orderId,
  toResult: ..., toError: ...,
  metrics: { prefix: "order" }
})
```

## Documentation

- **[Getting Started](docs/getting-started.md)** — project setup, first aggregate
- **[Framework API](docs/framework-api.md)** — complete reference for all helpers
- **[Patterns](docs/patterns.md)** — projections, outbox, workflows, replay
- **[Examples](docs/examples.md)** — order and profiler walkthroughs

## Examples

| Example | Complexity | Features |
|---------|-----------|----------|
| `examples/order/` | Medium | commands, projections, snapshots, workflows |
| `services/profiler/` | Advanced | snapshots, outbox, ClickHouse, replay |

## Effect-TS integration

N2 builds on Effect's ecosystem — use it directly:

| Concern | Effect API |
|---------|-----------|
| Async logic | `Effect.gen(function* () { ... })` |
| Dependency injection | `Layer.merge()`, `Layer.provide()` |
| Schemas | `Schema.TaggedClass`, `Schema.TaggedRequest`, `Schema.TaggedError` |
| Stateful entities | `@effect/cluster` Entity, Sharding, EntityProxy |
| RPC | `@effect/rpc` RpcGroup, RpcServer, RpcMiddleware |
| Workflows/Sagas | `@effect/workflow` Workflow, Activity, DurableClock |
| Projections | `@effect/experimental` EventLog.group |
| SQL | `@effect/sql-pg`, `@effect/sql-clickhouse` |
