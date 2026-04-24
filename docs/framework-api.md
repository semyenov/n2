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

Snapshot, override, and `afterCommit` requirements are preserved in the returned layer type. If a hook reads a service from context, the caller must provide that service instead of casting the entrypoint effect to `never`.

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

### `makeSnapshotService({ table, stateSchema, idColumn? })`

Generic SQL snapshot persistence. Returns `.makeLive(tag)` → `Layer`.

```ts
const snapshots = makeSnapshotService({ table: "my_snapshots", stateSchema: MyState })
class MySnapshots extends Context.Tag("MySnapshots")<MySnapshots, SnapshotService<MyState>>() {}
const MySnapshotsLive = snapshots.makeLive(MySnapshots)
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

`makeDrainOnce` preserves storage failures as `SqlError` and preserves publisher requirements in the returned `Effect`; the worker layer catches storage failures inside its loop.

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

`.start(message)` requires a `WorkflowEngine`. `.handlers` requires the configured publisher service and should be composed with the workflow engine layer in tests and entrypoints.

### `makeEventDecoder(eventGroup, constructors)`

Builds a decoder function from EventGroup + event constructors for replaying journal entries.

### `makeReplayTool({ decodeEvent, entityIdOf, dispatch, eventGroup })`

Generic replay tool. Returns `.collectEvents(entries, options)`, `.dispatch(event)`, `.matchesOptions(event, options)`.

### `parseReplayOptions(argv, resetDefault)`

CLI argument parser for `--entity-id`, `--min-revision`, `--max-revision`, `--reset`, `--dry-run`.

---

## Testing

### `makeTestAggregate({ eventLogSchema, noOpProjection, handlersLayer, snapshotsTag })`

Creates test infrastructure. Returns `.makeTestLayers()` (mock snapshots + wired handlers) and `.runWith(layer, program)`.

```ts
const { makeTestLayers, runWith } = makeTestAggregate({
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
