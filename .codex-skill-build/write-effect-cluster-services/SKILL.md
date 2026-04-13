---
name: write-effect-cluster-services
description: Create or update `@effect/cluster` services in `/home/alexander/Projects/n2` using the top-level `examples/order` service as the canonical pattern. Use when Codex needs to add or refactor a sharded domain service, RPC contracts, entity handlers, workflow wiring, SQL-backed event log projections, snapshot persistence, or `server.ts` / `cluster.ts` entrypoints for this repo. Prefer this skill when the task mentions Effect cluster services, sharding, entity proxies, cluster-backed workflows, or asks to follow the order example instead of `src/examples/*`.
---

# Write Effect Cluster Services

Use the top-level `examples/order` tree as the source of truth for service structure. Do not model new work on `src/examples/order` unless the task explicitly asks for that older path.

## Workflow

1. Inspect the existing service or adjacent example files before writing code.
2. Read `references/order-service-map.md` to decide which files the requested change actually needs.
3. Read `references/cluster-patterns.md` for the repo-specific shapes of contracts, entities, layers, workflows, and entrypoints.
4. Implement the smallest coherent slice. Add full service scaffolding only when the task actually needs a new service.
5. Validate with `bunx tsc --noEmit` and `bun test`, or targeted Bun tests during iteration and the full suite at the end when feasible.

## Core Rules

- Keep business rules in a pure or Effect-based aggregate module. Put state transitions in `evolve`, decisions in `decide`, and the combined execution path in `handle`.
- Define commands as `Schema.TaggedRequest`, domain events as `Schema.TaggedClass`, and business failures as `Schema.TaggedError`.
- Treat command schemas as both domain commands and RPC contracts. Do not introduce duplicate DTOs unless the task explicitly needs a boundary-specific shape.
- Build cluster services with explicit `Entity.make` plus `Rpc.make` definitions. Use one primary key function per RPC to make sharding behavior obvious.
- Keep read RPCs read-only. If a query should not append events, do not route it through the persisted event path.
- Separate dev-mode in-memory handlers from the cluster entity layer when the service needs both local iteration and production sharding.
- Compose infrastructure with named `Layer` constants so shared references can be deduplicated by Effect at runtime.
- Keep imports and file naming aligned with nearby example files. Local TypeScript imports should end in `.js`.

## Build Order

When creating a new cluster-backed service from scratch, use this order unless the task only asks for a subset:

1. `contracts.ts`
2. `aggregate.ts`
3. `events.ts`
4. `projector.ts`
5. `snapshots.ts` if replay cost or restart recovery matters
6. `workflows.ts` if command handling launches durable side effects
7. `entity.ts`
8. `layers.ts`
9. `server.ts` for local or single-process mode
10. `cluster.ts` for sharded multi-runner mode
11. SQL migrations for projections or snapshots

## Decision Points

- Add `events.ts` and `projector.ts` only when the service needs an event log and read model.
- Add `snapshots.ts` only when state recovery from pure replay would be too slow or the example already expects snapshots.
- Add `workflows.ts` only when a command kicks off durable orchestration that should survive retries or process restarts.
- Add in-memory handlers only when tests or a dev server benefit from running without cluster infrastructure.

## Validation

- Prefer targeted test files while iterating, then finish with `bunx tsc --noEmit` and `bun test` for meaningful changes.
- If a change touches public exports or module boundaries, update the relevant entrypoint exports as part of the same task.
- If the requested service differs materially from the order example, preserve the repo conventions but state the deviations explicitly in your final summary.

## References

- Read `references/order-service-map.md` for file responsibilities and when each file is optional.
- Read `references/cluster-patterns.md` for the concrete cluster-service patterns that should be mirrored in new work.
