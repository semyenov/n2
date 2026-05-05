# N2 — Event-sourced microservices with Effect-TS

A framework for building event-sourced microservices using **Effect-TS** and **Bun**. Define pure business logic once — N2 derives cluster entities, RPC handlers, HTTP routes, snapshots, projections, and event publishing.

## Quick start

```bash
bun install
bun run test
bun examples/order/server.ts                # order dev server
bun services/profile-provider/src/server.ts # advanced production-style service
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
- **[Effect API Playbook](docs/effect-api-playbook.md)** — Effect API map for N2 contributors
- **[Patterns](docs/patterns.md)** — projections, outbox, workflows, replay
- **[Examples](docs/examples.md)** — order and profiler walkthroughs
- **[Observability](docs/observability.md)** — local OpenTelemetry setup

## Examples

| Example | Complexity | Features |
|---------|-----------|----------|
| `examples/order/` | Medium | commands, projections, snapshots, workflows |
| `services/profile-provider/` | Advanced (submodule) | snapshots, outbox, ClickHouse, replay |
| `services/request-provider/` | Advanced (submodule) | snapshots, outbox, ClickHouse, replay |
| `services/pii-provider/` | Advanced (submodule) | encrypted payload storage, snapshots, outbox, replay |

## Service Docker images

The advanced services are git submodules, but their Dockerfiles use the
monorepo root as the build context so workspace packages and service contract
exports resolve consistently. Build them from this directory:

```bash
docker build -f services/profile-provider/Dockerfile .
docker build -f services/request-provider/Dockerfile .
docker build -f services/pii-provider/Dockerfile .
```

When preparing commits, commit service changes inside the affected submodule
first, then commit the updated submodule pointer and root package/docs changes
in this repository.

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
