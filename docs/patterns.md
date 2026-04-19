# Patterns

## Event conventions

All events share a common base with standardized fields:

```ts
const EventBase = {
  entityId: Schema.String,     // e.g., profileId, orderId, sku
  occurredAt: Schema.DateTimeUtc,  // business timestamp (not wall clock)
}

class MyEvent extends Schema.TaggedClass<MyEvent>()(
  "MyEvent", { ...EventBase, /* event-specific fields */ }
) {}
```

The `_tag` discriminant conveys what happened. `occurredAt` conveys when. No domain-specific timestamp names (`createdAt`, `mergedAt`, etc.) — the `_tag` already carries that semantics.

## Projections

### Per-event typed store

Define a `ProjectionHandlers` interface with one method per event type, plus a `dispatch` method:

```ts
interface ProjectionHandlers {
  readonly onMyEvent: (event: MyEvent) => Effect<void, SqlError>
  readonly onOtherEvent: (event: OtherEvent) => Effect<void, SqlError>
  readonly dispatch: (event: MyEventUnion) => Effect<void, SqlError>
}

const makeDispatch = (handlers: Omit<ProjectionHandlers, "dispatch">) =>
  (event: MyEventUnion) => {
    switch (event._tag) {
      case "MyEvent": return handlers.onMyEvent(event)
      case "OtherEvent": return handlers.onOtherEvent(event)
    }
  }
```

Each handler receives the specific narrowed event type — no union switching inside the handler, full type safety on field access.

### Projector layer

The projector bridges EventLog → store + outbox:

```ts
EventLog.group(MyEventGroup, (handlers) =>
  handlers
    .handle("MyEvent", ({ payload }) =>
      Effect.gen(function* () {
        const store = yield* MyProjectionStore
        const outbox = yield* MyOutbox
        const event = new MyEvent(payload)
        yield* store.onMyEvent(event)
        yield* outbox.enqueue(makeMessage(event))
      }).pipe(Effect.orDie)
    )
)
```

Outbox is **decoupled** from the projection store — the store only handles read model mutations, the projector coordinates both.

## Transactional outbox

```ts
const outbox = makeOutboxService({
  table: "my_event_outbox",
  idOf: (m) => m.id,
  serialize: (m) => JSON.stringify(encode(m)),
  deserialize: (json) => decode(JSON.parse(json))
})

// Service layer
const MyOutboxLive = outbox.makeLive(MyOutbox)

// Background worker
const MyOutboxWorkerLive = outbox.makeWorkerLive({
  outbox: MyOutbox,
  publish: (message) => startMyEventPublish(message),
  batchSize: 50,
  idleDelay: "1 second"
})
```

The outbox provides exactly-once semantics:
1. `enqueue` — idempotent INSERT (ON CONFLICT DO NOTHING)
2. `claimPending` — atomic claim with `FOR UPDATE SKIP LOCKED`
3. `markDispatched` / `markFailed` — state transitions with retry scheduling

## Durable publish workflows

```ts
const publishWorkflow = makePublishWorkflow({
  name: "MyEventPublish",
  messageSchema: MyMessage,
  publisherTag: MyPublisher
})

// Use in outbox worker:
outbox.makeWorkerLive({
  publish: publishWorkflow.start,
  // ...
})
```

The workflow retries with exponential backoff using `DurableClock.sleep` — survives process restarts.

## Snapshot persistence

```ts
const snapshots = makeSnapshotService({
  table: "my_snapshots",
  stateSchema: MyState,
  idColumn: "entity_id"
})

// Wire into entity layer:
Order.toEntityLayer(OrderEntity, {
  snapshots: {
    load: (id) => Effect.flatMap(MySnapshots, (s) => s.load(id)),
    save: (id, state, rev) => Effect.flatMap(MySnapshots, (s) => s.save(id, state, rev)),
    every: 100
  }
})
```

## Event replay

Rebuild projections from the event journal:

```ts
const decodeEvent = makeEventDecoder(MyEventGroup, { MyEvent, OtherEvent })

const replay = makeReplayTool({
  decodeEvent,
  entityIdOf: (event) => event.entityId,
  dispatch: (event) => Effect.flatMap(MyStore, (s) => s.dispatch(event)),
  eventGroup: MyEventGroup
})

// CLI: bun my-replay.ts --entity-id abc --min-revision 10 --dry-run
const options = parseReplayOptions(Bun.argv.slice(2), true)
const events = yield* replay.collectEvents(entries, options)
for (const event of events) yield* replay.dispatch(event)
```

## Sagas / workflows

For cross-aggregate coordination, use `@effect/workflow` directly:

```ts
const FulfillmentWorkflow = Workflow.make({
  name: "OrderFulfillment",
  payload: { orderId: Schema.String, sku: Schema.String, quantity: Schema.Number },
  success: Schema.Struct({ orderId: Schema.String, shipped: Schema.Boolean }),
  error: FulfillmentError,
  idempotencyKey: (p) => p.orderId
})

// With compensation:
yield* ReserveInventory.pipe(
  FulfillmentWorkflow.withCompensation(() => ReleaseInventory.pipe(Effect.ignore))
)
yield* ChargePayment.pipe(
  FulfillmentWorkflow.withCompensation(() => RefundPayment.pipe(Effect.ignore))
)
yield* DurableClock.sleep({ name: "pre-ship-delay", duration: "5 seconds" })
yield* ScheduleShipping  // point of no return
```
