# Effect API Playbook

This playbook maps the Effect API surface that N2 contributors touch most often.
It is not a generated API clone. Use it as a navigation guide before opening the
full reference for exact overloads, variance details, or newer package additions.

Primary sources:

- Official generated API index: <https://effect-ts.github.io/effect/>
- Core `Effect` API: <https://effect-ts.github.io/effect/effect/Effect.ts.html>
- Core `Layer` API: <https://effect-ts.github.io/effect/effect/Layer.ts.html>
- Core `SynchronizedRef` API: <https://effect-ts.github.io/effect/effect/SynchronizedRef.ts.html>
- Context7 library used for this repo map: `/effect-ts/effect`

## Using Context7 for Effect work

Use Context7 whenever an Effect API, package, or helper behavior is unfamiliar
or plausibly version-sensitive. For this repo, the default library ID is
`/effect-ts/effect`.

If the prompt already names `/effect-ts/effect`, query it directly. Otherwise,
resolve the library ID from `Effect` first, then query the selected official
Effect library. Resolve again when package names, major versions, or Context7
results look stale or too broad.

Default query shape:

```text
For effect@^3.21.0 and <package>@<repo-baseline>, explain <module/API>
for <concrete N2 task> with current TypeScript examples and pitfalls.
```

Use the generated API docs for exact signatures, overloads, and module names.
Use Context7 examples for current usage patterns, package-family orientation,
and quick checks before editing helpers. If a result changes a helper design or
reveals version-sensitive behavior, record that in the PR, issue, or docs note
near the change.

Repo package baseline:

| Package | Baseline |
|---------|----------|
| `effect` | `^3.21.0` |
| `@effect/cluster` | `^0.58.0` |
| `@effect/experimental` | `^0.60.0` |
| `@effect/opentelemetry` | `^0.63.0` |
| `@effect/platform` | `^0.96.0` |
| `@effect/platform-bun` | `^0.89.0` |
| `@effect/rpc` | `^0.75.0` |
| `@effect/sql` | `^0.51.0` |
| `@effect/sql-clickhouse` | `^0.48.0` |
| `@effect/sql-pg` | `^0.52.1` |
| `@effect/workflow` | `^0.18.0` |
| `@effect/vitest` | `^0.29.0` |

## Effect mental model

`Effect<A, E, R>` describes a program that can:

- succeed with `A`;
- fail in the typed error channel with `E`;
- require services from the environment `R`.

The type parameters are the map. If a helper returns `Effect<A, E, never>`, it is
claiming that all required services have already been provided. Preserve `R`
until a real `Layer` or `Effect.provide*` call supplies those services.

Typed failures are expected business or infrastructure failures. N2 domain
errors such as command validation errors should stay in the typed error channel
with `Effect.fail`, `Schema.TaggedError`, or `yield* new MyError(...)`.

Defects are unexpected failures. Use `Effect.orDie` only at deliberate fatal
boundaries, such as a process entrypoint or a framework helper path where
continuing would hide a broken invariant. Do not use it to make type errors
disappear.

Requirements are service dependencies. Common requirements in this repo are SQL
clients, event journals, outbox services, workflow engines, sharding, telemetry,
snapshot stores, and test services. Keep domain logic free of concrete runtime
construction; inject capabilities through `Context.Tag` and `Layer`.

## Core composition map

| API | Use it for | N2 guidance |
|-----|------------|-------------|
| `Effect.gen` | Linear multi-step workflows with typed services and errors | Default style for aggregate decisions, handlers, projection code, and runtime wiring. |
| `Effect.map` | Transform a success value without adding effects | Prefer for simple DTO/result shaping. |
| `Effect.flatMap` | Sequence an effectful step | Use when the next step depends on a prior value and `Effect.gen` would be heavier. |
| `Effect.matchEffect` | Effectfully branch on success or failure | Good at RPC and adapter boundaries where both paths need mapping. |
| `Effect.catchAll` | Recover from all typed failures | Keep recovery local and explicit; prefer narrower catch helpers when available. |
| `Effect.tap` / `Effect.tapError` | Observe without changing success or error values | Use for metrics, logs, and spans around command dispatch or projection writes. |
| `Effect.ensuring` | Always run cleanup after success, failure, or interruption | Use for lock release, spans, temp resources, and test cleanup. |
| `Effect.orDie` | Convert typed failure into a defect | Keep this rare and intentional. It is not a general adapter shortcut. |
| `Effect.timeout` | Bound an operation by duration | Useful around network, SQL, and RPC calls when callers can handle timeout errors. |
| `Effect.retry` + `Schedule` | Retry with a policy | Use for transient publish, workflow, SQL, or network failures. Avoid retrying deterministic domain failures. |

Typical imports follow the repo style:

```ts
import * as Effect from "effect/Effect"
import * as Schedule from "effect/Schedule"
```

When adding local imports in TypeScript files, keep the repo convention of `.js`
module specifiers.

## Context7-backed orientation snippets

These snippets are intentionally small. They show the shape of current Effect
usage that Context7 surfaced for this playbook; open the generated docs before
depending on exact overloads.

Bounded collection work:

```ts
import * as Effect from "effect/Effect"

const writeAll = (events: ReadonlyArray<DomainEvent>) =>
  Effect.forEach(events, writeProjection, { concurrency: 8 })

const readBoth = (entityId: string) =>
  Effect.all({
    state: loadState(entityId),
    snapshot: loadSnapshot(entityId)
  }, { concurrency: "unbounded" })
```

Retry transient infrastructure failures with a schedule, not by hand-rolled
loops:

```ts
import * as Effect from "effect/Effect"
import * as Schedule from "effect/Schedule"

const publishWithRetry = publish(message).pipe(
  Effect.retry(
    Schedule.exponential("100 millis").pipe(
      Schedule.compose(Schedule.recurs(5))
    )
  )
)
```

Layer construction should make lifecycle ownership visible:

```ts
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"

const StoreConfigLive = Layer.succeed(StoreConfig, { table: "profiles" })

const StoreLive = Layer.effect(
  Store,
  Effect.gen(function* () {
    const config = yield* StoreConfig
    return makeStore(config)
  })
).pipe(
  Layer.provide(StoreConfigLive)
)

const WorkerLive = Layer.scoped(Worker, acquireWorker)
const AppLive = WorkerLive.pipe(Layer.provideMerge(StoreLive))
```

RPC clients and handlers should keep protocol and serialization in layers:

```ts
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { FetchHttpClient } from "@effect/platform"
import { RpcClient, RpcSerialization } from "@effect/rpc"
import { MyRpcs } from "./contracts.js"

const HandlersLive = MyRpcs.toLayer(
  Effect.gen(function* () {
    const store = yield* Store
    return {
      GetSomething: ({ id }) => store.get(id)
    }
  })
).pipe(
  Layer.provide(StoreLive)
)

const ProtocolLive = RpcClient.layerProtocolHttp({ url: "/rpc/my-service" }).pipe(
  Layer.provide([FetchHttpClient.layer, RpcSerialization.layerJson])
)

const clientProgram = Effect.gen(function* () {
  const client = yield* RpcClient.make(MyRpcs)
  return yield* client.GetSomething({ id: "example" })
}).pipe(
  Effect.provide(ProtocolLive)
)
```

Use scoped tests for scoped resources instead of smuggling a `Scope` through a
plain test:

```ts
import { it } from "@effect/vitest"
import * as Effect from "effect/Effect"

it.scoped("releases scoped resources", () =>
  Effect.gen(function* () {
    const resource = yield* acquireResource
    yield* useResource(resource)
  })
)
```

## Concurrency and state

`Effect.all` and `Effect.forEach` run collections of effects. Pass an explicit
`concurrency` option whenever parallelism matters to correctness, load, or test
determinism:

```ts
yield* Effect.forEach(commands, dispatchCommand, { concurrency: 8 })
```

Fibers are lightweight concurrent Effect programs. Use `Effect.fork` for
background work only when the fiber is tied to a lifetime you control. Prefer
`Effect.scoped`, `Layer.scoped`, or service entrypoint supervision for workers
that should stop with the process.

`Deferred` coordinates one-shot completion between fibers. It is especially
useful in tests for "wait until the worker saw this message" without sleeping.

`Ref` holds atomic in-memory state. Use it for counters, small maps, caches, and
test adapters when updates are synchronous and do not need additional effects.

`SynchronizedRef` serializes effectful updates to a value. It is the right tool
when the update itself needs `Effect`, but it can become an accidental global
bottleneck. In multi-entity paths, prefer a keyed design: separate state cells
per entity ID, or a documented semaphore when a global limit is intentional.

Semaphores bound concurrent access to a shared resource. Use them for real
resource limits such as connection pressure, API quotas, or a deliberately
serialized projection writer. Do not use a single semaphore as a substitute for
per-entity ordering unless the whole process must be serialized.

Scoped resource safety matters for servers and workers. Prefer
`Layer.scoped`, `Effect.acquireRelease`, and `Effect.ensuring` around resources
with lifetimes: HTTP servers, SQL pools, telemetry exporters, background loops,
and test fixtures.

## Dependency injection

Define capabilities with `Context.Tag`:

```ts
import * as Context from "effect/Context"

export class MyStore extends Context.Tag("MyStore")<
  MyStore,
  {
    readonly save: (id: string) => Effect.Effect<void, MyStoreError>
  }
>() {}
```

Provide capabilities with layers:

| API | Use it for |
|-----|------------|
| `Layer.succeed(Tag, service)` | Pure service values, fakes, and simple adapters. |
| `Layer.effect(Tag, effect)` | Services built by an effect, such as reading config or creating clients. |
| `Layer.scoped(Tag, scopedEffect)` | Services with acquisition and release. |
| `Layer.provide` | Satisfy one layer's requirements with another layer. |
| `Layer.provideMerge` | Provide dependencies while preserving both outputs. |
| `Layer.merge` / `Layer.mergeAll` | Combine independent layers into one environment. |

In N2, domain and aggregate code should describe requirements. Runtime modules
assemble concrete layers. Keep lifecycle ownership at clear boundaries:

- framework helpers define reusable wiring;
- service `layers.ts` files choose concrete PostgreSQL, ClickHouse, outbox,
  workflow, and telemetry implementations;
- `server.ts`, `cluster.ts`, and replay tools provide final runtimes.

When a helper accepts a user effect, preserve its requirements in the helper's
type unless the helper also accepts and applies the matching layer.

## Data and contracts

Effect Schema is the contract language for N2.

| Schema shape | Use it for |
|--------------|------------|
| `Schema.Class` | State, DTOs, read models, and structured data with constructors. |
| `Schema.TaggedClass` | Domain events and discriminated data objects. |
| `Schema.TaggedRequest` | Commands that also serve as RPC request schemas. |
| `Schema.TaggedError` | Typed domain and adapter failures. |
| Branded schemas | IDs, revisions, and domain primitives that should not mix accidentally. |

Keep encode/decode work at boundaries:

- decode inbound RPC, HTTP, queue, SQL JSON, and event journal payloads;
- encode outbound event log, outbox, SQL JSON, and wire responses;
- use `Schema.decodeUnknown` or `Schema.decodeUnknownSync` when crossing from
  raw external data into typed code;
- keep aggregate `decide` and `evolve` logic working with already-decoded
  domain values.

Event replay is a boundary too. Historical journal entries should be migrated or
otherwise normalized before decoding into the current event schema.

## Experimental package boundary

N2 currently uses `@effect/experimental` for EventGroup, EventLog, and
EventJournal only. Keep direct access to EventGroup runtime metadata centralized
in the framework helpers; service code should prefer `defineEvents(...).toEventGroup(...)`,
`makeProjectionLayer(...)`, `makeEventDecoder(...)`, and the replay helpers.

Treat other experimental modules as candidates, not defaults. Do not introduce
DevTools, EventLogRemote, Machine, Persistence, or VariantSchema into core
runtime wiring without a dedicated plan and compatibility tests.

## Runtime and service packages

| Package or module | Use in N2 |
|-------------------|-----------|
| `effect/Effect` | Composition, typed errors, requirements, interruption, retry, and resource safety. |
| `effect/Layer` | Runtime dependency graph and lifecycle assembly. |
| `effect/Context` | Service tags and dependency lookup. |
| `effect/Schema` | Commands, events, errors, state, payloads, and read models. |
| `effect/Ref` | Atomic in-memory state and small caches. |
| `effect/SynchronizedRef` | Effectful serialized state updates when keyed or intentionally global. |
| `effect/Deferred` | Fiber coordination and deterministic async tests. |
| `effect/Fiber` | Background work, supervision, joins, and interruption. |
| `effect/Schedule` | Retry, repeat, exponential backoff, and test-clock-friendly timing. |
| `effect/Metric` | Counters, gauges, histograms, and command/projection instrumentation. |
| `effect/Config` | Runtime configuration read by service layers. |
| `@effect/platform` | HTTP abstractions, routers, request/response types, command execution, and platform services. |
| `@effect/platform-bun` | Bun runtime and HTTP server integration. |
| `@effect/rpc` | RPC groups, handlers, clients, serialization, middleware, and tests. |
| `@effect/sql` | SQL client abstractions, migrations, journals, and shared SQL helpers. |
| `@effect/sql-pg` | PostgreSQL-backed SQL client and migrations. |
| `@effect/sql-clickhouse` | ClickHouse client and migrations for read projections. |
| `@effect/cluster` | Sharding, persisted entities, entity proxies, and cluster runtime pieces. |
| `@effect/workflow` | Durable publish and workflow orchestration. |
| `@effect/experimental` | Event log, event journal, and experimental infrastructure used by helpers. |
| `@effect/opentelemetry` | Tracing, metrics, logs, and OTLP export integration. |
| `@effect/vitest` | Effect-aware test utilities and test-clock integration. |

## N2-specific patterns

Aggregate helpers live around `define<Event, Command>()`:

- `decide` returns the events a command should emit;
- `evolve` applies an event to state;
- `handle` combines decide and evolve for one command;
- `run` executes command batches sequentially;
- infrastructure helpers project the same pure aggregate into RPC, entity,
  snapshot, outbox, projection, and replay workflows.

RPC handlers are contract driven. `Schema.TaggedRequest` classes are the command
schemas and the RPC request types. Keep command constructors, RPC groups, and
handler maps aligned so TypeScript can narrow by `_tag`.

Entity wiring owns durable command execution. `toEntityLayer` is the production
path for clustered services. It may load snapshots, handle commands, persist
events, run `postHandle`, save snapshots, and publish after commit.

Stateful RPC handlers are a dev/test and local-service path. They should keep
commands for the same entity ordered while allowing unrelated entity IDs to make
progress concurrently. Read overrides should bypass command dispatch only when
they do not mutate aggregate state.

Event journals and event logs are raw persistence boundaries. Encode events
before writing, decode after reading, and keep migration-before-decode behavior
explicit in replay paths.

Projections are adapter code. They consume already-decoded events and maintain
read models in PostgreSQL or ClickHouse. Cache only what the process can prove
it owns, and keep a durable lookup path for restart, replay, and repair.

Outbox workers publish durable messages asynchronously. The command path may
wait for durable enqueue/publish when the service requires write-through safety,
but worker drain concurrency should be tuned separately from aggregate command
serialization.

Snapshots are an optimization, not the source of truth. Snapshot load failures
should be explicit. Snapshot save cadence should be visible in service wiring.

Observability belongs at framework and adapter boundaries: command counters,
error counters, latency histograms, outbox counters, structured logs, and spans.
Keep metric names stable because the local Grafana dashboards depend on them.

Tests should prefer deterministic Effect tools over sleeping:

- use `Effect.runPromise` or Effect-aware test helpers consistently with the
  surrounding file;
- use `Deferred` to coordinate async workers;
- use `TestClock` for sleeps, retries, and schedules when test services are in
  scope;
- use targeted helper tests for framework contracts, then service smoke tests
  for end-to-end wiring.

## Audit checklist

Use this checklist when reviewing helpers such as `Definition.ts`, `Runtime.ts`,
projection stores, outbox workers, replay tools, and workflow wiring.

- Does every helper preserve `Effect` requirements until it genuinely provides
  them?
- Are casts isolated at real dynamic boundaries, with schema decoding or a named
  helper around the boundary?
- Are expected domain failures represented as typed errors instead of defects?
- Is `Effect.orDie` limited to deliberate fatal boundaries?
- Are `SynchronizedRef`, semaphores, and queues keyed or scoped narrowly enough
  for the service's concurrency model?
- Are long-running fibers tied to `Layer` or scoped lifecycles?
- Are SQL, ClickHouse, event journal, and outbox payloads decoded at ingress and
  encoded at egress?
- Does replay migrate or normalize historical data before current-schema decode?
- Are aggregate decisions free of infrastructure concerns?
- Do read overrides avoid mutating command state?
- Are projection caches restart-safe because they fall back to durable reads?
- Are `Layer.provide`, `provideMerge`, and `mergeAll` used in an order that makes
  lifecycle ownership readable?
- Do tests cover same-entity ordering, different-entity concurrency, snapshot
  load/save paths, outbox retry/dead-letter paths, replay filters, and workflow
  start behavior where those paths are touched?
- Are timing-sensitive tests using `Schedule`, `Deferred`, or `TestClock`
  instead of wall-clock sleeps?
- Do observability changes preserve metric names used by dashboards and docs?
- Before changing helpers against an unfamiliar Effect API, was Context7
  refreshed through `/effect-ts/effect` and any version-sensitive behavior
  recorded near the change?
