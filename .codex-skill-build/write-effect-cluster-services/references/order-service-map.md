# Order Service Map

Use this file to decide which parts of `examples/order` you need to mirror.

## Canonical Path

Treat the top-level `examples/order` directory as canonical for this skill. Do not start from `src/examples/order`.

## File Roles

- `contracts.ts`: Define command, event, error, state, and result schemas. Define the `Entity.make(...)` RPC protocol here.
- `aggregate.ts`: Hold pure domain behavior. Keep `evolve`, `decide`, and `handle` together.
- `events.ts`: Define the `EventGroup` and shared event log schema used by publishers and dispatchers.
- `projector.ts`: Materialize read models from the event log into SQL tables.
- `snapshots.ts`: Persist and restore aggregate state snapshots through a tagged service.
- `workflows.ts`: Define durable workflows and activities for long-running side effects.
- `entity.ts`: Build the cluster entity layer, in-memory handlers, and proxy RPC group/handlers.
- `layers.ts`: Compose the shared infrastructure, workflow engine, event log, projections, and snapshots.
- `server.ts`: Run the service without cluster sharding for local development.
- `cluster.ts`: Run the sharded service with HTTP runner transport and SQL-backed cluster storage.
- `migrations/*.ts`: Create SQL tables for projections, snapshots, and any other durable read/write models.

## Minimal Slices

- New RPC on an existing service:
  Update `contracts.ts`, `aggregate.ts`, and `entity.ts`.
- New read model:
  Update `events.ts`, `projector.ts`, and migrations. Touch `layers.ts` only if a new group or shared service is needed.
- Durable saga:
  Update `workflows.ts`, then trigger it from `entity.ts` after the state mutation succeeds.
- New service:
  Start with `contracts.ts`, `aggregate.ts`, `entity.ts`, `layers.ts`, and one entrypoint. Add the rest only when the requirements demand them.

## Naming Pattern

- Use domain-first names such as `PaymentEntity`, `PaymentRpcs`, `PaymentProxyHandlers`, `PaymentWorkflowLayer`.
- Use `Layer` suffixes for layer values and `Handlers` for workflow or RPC handler layers.
- Keep tagged schema class names concrete: `CreatePayment`, `PaymentSettled`, `PaymentError`.
