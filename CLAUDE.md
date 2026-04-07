# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

N2 is a lightweight framework for building event-sourced microservices using **Effect-TS** and **Bun**. It provides a single core abstraction (`define()`) that wires pure domain logic into stateful cluster entities, stateless RPC handlers, and HTTP routes — all without duplicating schemas or transport DTOs.

## Commands

```sh
bun install                  # install dependencies
bunx tsc --noEmit            # type check (strict mode)
bun test                     # run all tests

# Run a single test file
bun test src/examples/order/aggregate.test.ts

# Run a single test by name
bun test src/examples/order/aggregate.test.ts -t "SubmitOrder with no items fails"

# Run examples
bun src/examples/order/index.ts        # dev HTTP server (port 3000)
bun src/examples/order/production.ts  # production wiring
bun src/examples/order/bench.ts       # benchmarks (mitata)
```

After any meaningful change, run `bunx tsc --noEmit` then `bun test`.

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

Every aggregate follows this four-file pattern:

1. **`contracts.ts`** — Schema definitions: commands (`Schema.TaggedRequest`), events (`Schema.TaggedClass`), errors (`Schema.TaggedError`), state (`Schema.Class`)
2. **`aggregate.ts`** — Business logic: `evolve` (event → state transition) and `decide` (command → events or error)
3. **`entity.ts`** — Infrastructure wiring: cluster entity layer + RPC handlers
4. **`http.ts`** — HTTP route wiring

The framework's `define<Event, Command>()` function is the hub. It takes pure business logic and derives:
- `handle()` — combines decide + evolve for single command processing
- `run()` — runs sequential commands accumulating events
- `toEntityLayer()` — wires to `@effect/cluster` Entity for stateful per-ID persistence
- `toRpcHandlers()` — creates stateless RPC handlers (one request = fresh state replay)
- `toHttpRoute()` — wraps RPC handlers into HTTP routes

### Framework Layout

```
src/framework/
  domain/         Revision (optimistic concurrency), BrandedId
  helpers/        define() core, schema type utilities, RPC/entity derivation
  testing/        DeterministicIdGenerator, TestClock for pure domain tests
src/adapters/http/ Bun HTTP server + RPC route wiring
src/examples/
  order/          Primary reference (6 commands, sagas, projections, bench)
  inventory/      Secondary reference (simpler 2-command example)
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
- Local TypeScript import specifiers end in `.js`
- Tests are colocated (`*.test.ts`) and use `import { test, expect } from "bun:test"`
- For Effect-heavy tests, use `Effect.runPromise` consistent with the existing suite
- When changing framework helpers (`src/framework/`), check both order and inventory examples for impact
- If module boundaries change, update `src/main.ts` and `index.ts`
