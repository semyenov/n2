# Cluster Patterns

Use these patterns when writing or refactoring a service in this repo.

## 1. Contracts First

- Define commands with `Schema.TaggedRequest` so the same type carries domain intent and RPC success/failure schemas.
- Define events with `Schema.TaggedClass`.
- Define business failures with `Schema.TaggedError`.
- Define the cluster protocol with explicit `Rpc.make(...)` entries inside `Entity.make(...)`.
- Use `annotateRpcs(ClusterSchema.Persisted, true)` only for calls that should flow through persisted event-sourced handling. Keep read-only queries out of that path when appropriate.

## 2. Keep Aggregate Logic Framework-Free

- `aggregate.ts` should not know about `Entity`, `Layer`, HTTP, SQL, or cluster transport.
- `handle(state, command)` should produce the emitted events and resulting state. Reuse that from both in-memory and cluster handlers.
- Convert unexpected failures at the edge into typed domain errors instead of throwing raw `Error`.

## 3. Entity Layer Shape

- In the cluster layer, call `Entity.CurrentAddress` to get the entity id for the current shard-local actor.
- Hold actor-local state in a `Ref`.
- If snapshots exist, load them during actor initialization and fall back to the initial state on failure or missing data.
- Put the write path behind a small `dispatch` helper that:
  1. reads current state,
  2. runs `handle`,
  3. stores the next state,
  4. updates revision,
  5. optionally saves a snapshot.
- Launch workflows after the state change succeeds. Use workflow idempotency keys so duplicate commands join instead of forking duplicate executions.

## 4. Dev Mode and Cluster Mode

- Expose a local handler layer for tests and a lightweight dev server.
- Expose `EntityProxy.toRpcGroup(...)` and `EntityProxyServer.layerRpcHandlers(...)` so HTTP JSON-RPC routes can forward to cluster shards.
- Keep cluster mode in `cluster.ts` and single-process mode in `server.ts`.

## 5. Infrastructure Composition

- Put shared infrastructure into named layer constants. Reuse the same constants anywhere the same service must be provided so Effect can deduplicate by layer identity.
- Split workflow engines by environment:
  `WorkflowEngine.layerMemory` for dev mode.
  `ClusterWorkflowEngine.layer` for cluster mode.
- If you use `EventLog`, keep the publish side and dispatch side on the same schema object reference.

## 6. Entry Point Pattern

- Build a `SqlLayer` with `PgClient.layerConfig(...)` when the service depends on PostgreSQL.
- Build an RPC HTTP route with `RpcServer.layerHttpRouter(...)`.
- Add a simple health route with `HttpLayerRouter.add(...)`.
- In `cluster.ts`, compose:
  `BunClusterHttp.layer({ transport: "http", storage: "sql" })`
  plus the entity layer, cluster infrastructure, migrations, and API server layer.
- In `server.ts`, compose the local handlers, infrastructure layer, migrations, SQL client, and HTTP server layer.

## 7. Testing and Verification

- Favor targeted Bun tests while iterating on one aggregate or entity.
- Finish meaningful changes with:
  `bunx tsc --noEmit`
  `bun test`
- If adding a new cluster behavior, also add or update tests that exercise command handling, reads, and any workflow-triggering path.
