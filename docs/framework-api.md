# Framework API Reference

## Core: `define<Event, Command>()`

The central abstraction. Takes pure business logic and derives infrastructure helpers.

```ts
const MyAggregate = N2.define<MyEvent, MyCommand>()({
  initialState: MyState,
  commands: MyCommands.constructors,
  evolve: { /* event tag → state transform (pure, sync) */ },
  decide: { /* command tag → Effect<events[], error> */ }
})
```

### Returns

| Method | Purpose |
|--------|---------|
| `handle(state, command)` | Decide + evolve for single command |
| `run(commands[], state?)` | Sequential command batch |
| `toEntityLayer(entity, adapter, options?)` | Cluster entity with lifecycle hooks |
| `toStatefulRpcHandlers(group, adapter)` | Dev/test mode with `SynchronizedRef<Map>` |
| `toRpcHandlers(group, adapter)` | Stateless RPC (fresh state per request) |
| `toHttpRoute(group, path, handlers)` | HTTP route wiring |

---

## `toEntityLayer` — EntityAdapterOptions

```ts
MyAggregate.toEntityLayer(entity, {
  toResult: (ctx) => Result,          // { entityId, revision, command, events, state }
  toError: (error) => MappedError,

  // Optional lifecycle hooks:
  snapshots?: {
    load: (entityId) => Effect<Option<{ state, revision }>>
    save: (entityId, state, revision) => Effect<void>
    every: number                      // save every N revisions
  },
  postHandle?: (ctx) => State,         // pure sync transform after handle
  afterCommit?: (ctx) => Effect<void>, // fire-and-forget (event publishing)
  overrides?: {                        // per-command overrides (read queries)
    [K in CommandTag]?: (command, ctx) => Effect<Result>
  }
}, layerOptions?)
```

The `overrides` map is typed — each key narrows `command` to the specific command type for that tag.

---

## `toStatefulRpcHandlers` — StatefulRpcAdapterOptions

Same hooks as `toEntityLayer`, plus:

```ts
MyAggregate.toStatefulRpcHandlers(rpcs, {
  entityId: (command) => string,       // extract entity ID
  toResult, toError,
  snapshots?, postHandle?, afterCommit?, overrides?,
  metrics?: { prefix: "my_aggregate" } // auto counters + timers
})
```

Metrics auto-instruments: `${prefix}.commands.total`, `${prefix}.commands.errors`, `${prefix}.command.duration_ms` (tagged by command type).

---

## Schema helpers

### `defineCommands(...commands)`

Builds a command collection from `Schema.TaggedRequest` classes. Returns `.schema`, `.constructors`, `.toEntity(name, pk)`, `.toPersistedEntity(name, pk)`.

### `defineEvents(...events)`

Builds an event collection. Returns `.schema`, `.constructors`.

### `eventPayloadSchema(EventClass)`

Strips `_tag` from a `Schema.TaggedClass`, producing a `Schema.Struct` for EventGroup payloads.

```ts
EventGroup.empty.add({
  tag: "OrderCreated",
  primaryKey: (p) => p.orderId,
  payload: N2.eventPayloadSchema(OrderCreated)
})
```

---

## Infrastructure helpers

Preferred package imports are `@semyenov/n2/helpers`, `@semyenov/n2/domain`,
`@semyenov/n2/testing`, and `@semyenov/n2/adapters/http`. Legacy documented
paths such as `@semyenov/n2/src/main` and `@semyenov/n2/framework/helpers`
remain exported for compatibility.

### `makeSnapshotService({ table, stateSchema, idColumn? })`

Generic SQL snapshot persistence. Returns `.makeLive(tag)` → `Layer`.

```ts
const snapshots = makeSnapshotService({ table: "my_snapshots", stateSchema: MyState })
class MySnapshots extends Context.Tag("MySnapshots")<MySnapshots, SnapshotService<MyState>>() {}
const MySnapshotsLive = snapshots.makeLive(MySnapshots)
```

The SQL table must contain the entity ID column (`entity_id` by default),
`state_json`, `revision`, and `saved_at`.

### `makeSnapshotOps(tag, every)`

Builds the `snapshots` hook object for `toEntityLayer` / `toStatefulRpcHandlers`.

```ts
export const MySnapshotOps = makeSnapshotOps(MySnapshots, 100)

MyAggregate.toEntityLayer(MyEntity, {
  snapshots: MySnapshotOps,
  // ...
})
```

### `makeOutboxService({ table, idOf, serialize, deserialize })`

Transactional outbox pattern. Returns `.makeLive(tag)`, `.makeDrainOnce(opts)`, `.makeWorkerLive(opts)`.

```ts
const outbox = makeOutboxService({
  table: "my_outbox",
  idOf: (m) => m.id,
  serialize: (m) => JSON.stringify(Schema.encodeSync(MyMessage)(m)),
  deserialize: (json) => Schema.decodeUnknownSync(MyMessage)(JSON.parse(json))
})
```

The outbox table must contain `id`, `payload_json`, `status`, `retry_count`,
`last_error`, `created_at`, `updated_at`, `published_at`, and `next_attempt_at`.

### `makeOutboxJsonService({ table, schema, idOf, metrics? })`

Schema-backed JSON variant of `makeOutboxService`; use this for the common
production case where messages are Effect Schema classes.

```ts
const outbox = makeOutboxJsonService({
  table: "my_outbox",
  schema: MyMessage,
  idOf: (m) => m.id,
  metrics: { prefix: "my_outbox" }
})

const MyOutboxWorkerLive = outbox.makeWorkerLive({
  outbox: MyOutbox,
  publish: publishWorkflow.start,
  batchSize: 50,
  idleDelay: "1 second",
  maxRetries: 5
})
```

### `makeStandardSnapshotWiring({ tag, table, stateSchema, idColumn?, every })`

Combines `makeSnapshotService(...).makeLive(tag)` and `makeSnapshotOps(tag, every)`
for services that use the standard SQL snapshot table shape.

### `makeStandardOutboxWiring({ tag, table, schema, publish, metricsPrefix? })`

Combines JSON outbox storage, one-shot drain, and worker layer with the standard
service defaults: batch size `50`, idle delay `1 second`, and max retries `5`.

### `makePublishWorkflow({ name, messageSchema, publisherTag })`

Durable publish with exponential backoff retry via `@effect/workflow`. Returns `.workflow`, `.start(message)`, `.handlers` (Layer).

```ts
const pw = makePublishWorkflow({
  name: "MyEventPublish",
  messageSchema: MyMessage,
  publisherTag: MyPublisher
})
export const startPublish = pw.start
export const publishHandlers = pw.handlers
```

### `makeEventMessageFields(entityIdKey)` / `makeEventMessage(options)`

Builds the standard publication envelope fields used by durable event workflows:
`id`, `topic`, `partitionKey`, `eventType`, entity ID, `revision`, `occurredAt`,
`payload`, and `headers`.

### Runtime wiring helpers

`makeConfiguredClickhouseLayer`, `makeMigrationsLayer`, `makePgSqlLayer`,
`makeClusterShardingLayer`, `makeHealthRoute`, `makeRpcHttpRoute`, and
`makeServiceInfrastructureLayers` cover the repeated service entrypoint and
infrastructure composition used by provider-style services.

### `makeEventDecoder(eventGroup, constructors)`

Builds a decoder function from EventGroup + event constructors for replaying journal entries.

### `makeReplayTool({ decodeEvent, entityIdOf, dispatch, eventGroup })`

Generic replay tool. Returns `.collectEvents(entries, options)`, `.dispatch(event)`, `.matchesOptions(event, options)`.

### `makeReplayProgram({ argv, resetDefault, label, entries, decodeEvent, entityIdOf, dispatch, eventGroup, reset? })`

Builds a production replay CLI program: parses options, logs filters, optionally
resets read models, honors `--dry-run`, and dispatches matching events in order.

### `parseReplayOptions(argv, resetDefault)`

CLI argument parser for `--entity-id`, `--min-revision`, `--max-revision`,
`--reset`, `--no-reset`, and `--dry-run`.

---

## Testing

### `makeTestAggregate({ eventLogSchema, noOpProjection, handlersLayer, snapshotsTag })`

Creates test infrastructure. Returns `.makeTestLayers()` (mock snapshots + wired handlers) and `.runWith(layer, program)`.

```ts
const { makeTestLayers, runWith } = makeTestAggregate<MyState>({
  eventLogSchema: MyEventLogSchema,
  noOpProjection: MyNoOpProjection,
  handlersLayer: MyHandlersRaw,
  snapshotsTag: MySnapshots
})

test("my test", async () => {
  const { handlersLayer } = makeTestLayers()
  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(MyRpcs)
    // ...
  }))
})
```
