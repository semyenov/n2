# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.
Full documentation is in [docs/](docs/) — see [Getting Started](docs/getting-started.md), [Framework API](docs/framework-api.md), [Patterns](docs/patterns.md), [Examples](docs/examples.md).

## What This Is

N2 is a lightweight framework for building event-sourced microservices using **Effect-TS** and **Bun**. This is a Bun workspace monorepo:

- **`packages/n2`** — the framework package
- **`services/profile-provider`** — profile provider service (git submodule → `quaterbit/qb.service.profiler`)
- **`services/request-provider`** — request provider service (git submodule → `quaterbit/qb.service.request`)
- **`examples/order`** — order example (sagas, projections, cluster)

The two services are git submodules. Source-of-truth for service code lives in their own repos; the monorepo tracks pinned submodule pointers and continues to develop the framework. Edit the service code inside the submodule, push there, then bump the submodule pointer in the monorepo.

The framework provides:

- **`define()`** — wires pure domain logic (evolve + decide) into stateful cluster entities, stateless RPC handlers, and HTTP routes, all without duplicating schemas or transport DTOs
- **Lifecycle hooks** — snapshot persistence, post-handle transforms, event publishing, read query overrides
- **Infrastructure modules** — generic snapshot store, transactional outbox, durable publish workflows, event replay tooling

## Commands

```sh
bun install                  # install all workspace dependencies
bunx tsc --noEmit            # type check entire monorepo (strict mode)

# Run tests per workspace
cd packages/n2 && bun test
cd services/profile-provider && bun test
cd services/request-provider && bun test
cd examples/order && bun test

# Run a single test file
bun test packages/n2/src/framework/helpers/Definition.test.ts
bun test services/profile-provider/src/aggregate.test.ts
bun test examples/order/aggregate.test.ts

# Run services
bun examples/order/server.ts                          # order dev server (port 3000)
bun services/profile-provider/src/server.ts          # profile provider dev server
bun services/profile-provider/src/replay.ts --dry-run # replay projections
bun services/request-provider/src/server.ts          # request provider dev server
```

After any meaningful change, run `bunx tsc --noEmit` then tests in the affected workspace(s).

## Bun Runtime

Use Bun instead of Node.js/npm/pnpm/vite:

- `bun <file>` instead of `node <file>` or `ts-node <file>`
- `bun test` instead of jest or vitest
- `bun install` instead of npm/yarn/pnpm install
- `Bun.serve()` instead of express — supports WebSockets, HTTPS, routes
- `bun:sqlite`, `Bun.redis`, `Bun.sql` for databases
- `Bun.file` over `node:fs` readFile/writeFile
- Bun auto-loads `.env`, no dotenv needed

## Architecture

### Core Pattern

Every aggregate follows this file pattern:

1. **`contracts.ts`** — Schema definitions: commands (`Schema.TaggedRequest`), events (`Schema.TaggedClass`), errors (`Schema.TaggedError`), state (`Schema.Class`), event metadata
2. **`aggregate.ts`** — Business logic via `N2.define<Event, Command>()({...})` with exhaustive `evolve` and `decide` handlers
3. **`entity.ts`** — Infrastructure wiring: `toEntityLayer()` with lifecycle hooks, `toStatefulRpcHandlers()` for dev mode
4. **`events.ts`** — EventGroup bridging contracts to `@effect/experimental` EventLog via `eventPayloadSchema()`
5. **`projector.ts`** — Projection handlers dispatching to per-event typed store methods
6. **`http.ts`** / **`server.ts`** — HTTP route wiring

### Framework Helpers

The `define<Event, Command>()` function is the hub. It takes pure business logic and derives:

- `handle()` — combines decide + evolve for single command processing
- `run()` — runs sequential commands accumulating events
- `toEntityLayer(entity, adapter)` — cluster entity with lifecycle hooks:
  - `snapshots` — load state on init, save every N revisions
  - `postHandle` — non-event-sourced state transforms (pure, sync)
  - `afterCommit` — fire-and-forget side effects (event publishing)
  - `overrides` — typed per-command handler overrides (read queries bypass dispatch)
- `toStatefulRpcHandlers(group, adapter)` — dev/test mode with `SynchronizedRef<Map>`, optional metrics
- `toRpcHandlers(group, adapter)` — stateless RPC handlers
- `toHttpRoute(group, path, handlers)` — HTTP route wiring

Additional helpers:
- `defineCommands(...).toPersistedEntity(name, pk)` — derives Entity + RPC from command schemas
- `eventPayloadSchema(EventClass)` — strips `_tag` from TaggedClass for EventGroup payloads
- `makeSnapshotService({ table, stateSchema })` — generic SQL snapshot persistence
- `makeOutboxService({ table, serialize, deserialize })` — transactional outbox + worker loop
- `makePublishWorkflow({ name, messageSchema, publisherTag })` — durable publish with exponential backoff
- `makeEventDecoder(eventGroup, constructors)` — generic journal event decoder
- `makeReplayTool({ decodeEvent, entityIdOf, dispatch })` — journal replay with CLI option parsing

### Monorepo Layout

```
packages/n2/                  Framework package (name: "@semyenov/n2")
  index.ts                    Root entry point → src/main.js
  src/framework/
    domain/                   Revision (optimistic concurrency), BrandedId
    helpers/
      Definition.ts           define() core + toEntityLayer + toStatefulRpcHandlers
      Definitions.ts          defineCommands, defineEvents, eventPayloadSchema
      EntityBuilder.ts        rpcFromCommand (derives RPC from TaggedRequest)
      Snapshots.ts            makeSnapshotService (generic snapshot persistence)
      Outbox.ts               makeOutboxService (transactional outbox + worker)
      PublishWorkflow.ts      makePublishWorkflow (durable publish with retry)
      EventDecoder.ts         makeEventDecoder (journal entry → typed event)
      Replay.ts               makeReplayTool, parseReplayOptions
      Client.ts               makeHttpClient, makePromiseClient
      FetchClient.ts          makeFetchClient (zero-dependency)
    testing/                  DeterministicIdGenerator, TestClock for pure domain tests
  src/adapters/http/          Bun HTTP server + RPC route wiring

services/profile-provider/    Profile provider service — submodule of quaterbit/qb.service.profiler
                              (7 events, snapshots, outbox, ClickHouse projections, replay; src/ layout)
services/request-provider/    Request provider service — submodule of quaterbit/qb.service.request
                              (4 events, snapshots, outbox, ClickHouse projections, replay; src/ layout)

examples/order/               Order example (6 commands, sagas, projections, cluster)
```

### Effect-TS Integration

| Concern | Effect API |
|---------|-----------|
| Async logic | `Effect.gen(function* () { ... })` |
| Dependency injection | `Layer.merge()`, `Layer.provide()` |
| Schemas | `Schema.TaggedClass`, `Schema.TaggedRequest`, `Schema.TaggedError` |
| Stateful entities | `@effect/cluster` Entity, Sharding, EntityProxy |
| RPC | `@effect/rpc` RpcGroup, RpcServer |
| Sagas | `@effect/workflow` Workflow, Activity, DurableClock |
| Projections | `@effect/experimental` EventLog.group |

### Key Conventions

- Commands are both domain commands and RPC schemas — do not duplicate transport DTOs
- `decide` stays pure or Effect-based with no infrastructure leakage
- `evolve` is always a pure function (no effects)
- Import Effect modules as namespaces: `import * as Effect from "effect/Effect"`
- Services import framework via `import * as N2 from "@semyenov/n2/helpers"` (workspace package resolution)
- Local TypeScript import specifiers end in `.js` (ESM module resolution)
- Tests are colocated (`*.test.ts`) and use `import { test, expect } from "bun:test"`
- For Effect-heavy tests, use `Effect.runPromise` consistent with the existing suite
- When changing framework helpers (`packages/n2/src/framework/`), check profiler and order examples for impact
- Projection stores use per-event typed methods (not a single `project()` that switches on `_tag`)
- Event metadata (occurredAt field mapping) is declared in contracts.ts alongside event definitions
- Outbox is decoupled from projections — projector enqueues, store only handles read model mutations
- New non-generic services should use the `Effect.Service` class pattern (Effect 3.9+): `class MyService extends Effect.Service<MyService>()("MyService", { effect: ... }) {}`. Generic services like `OutboxService<Message>` and `SnapshotService<State>` continue to use the `Context.Tag + makeLive` factory pattern — `Effect.Service` doesn't model type parameters cleanly
