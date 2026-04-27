# Examples

## Order — medium aggregate

**Location**: `examples/order/`

A compact event-sourced service with commands, projections, snapshots, workflow
wiring, and an HTTP server.

| File | Purpose |
|------|---------|
| `contracts.ts` | Domain events, commands, state, and RPC/entity definitions |
| `aggregate.ts` | Business rules and state transitions |
| `entity.ts` | Entity and stateful RPC handlers |
| `events.ts` | EventGroup wiring |
| `projector.ts` | EventLog projection handlers |
| `snapshots.ts` | Snapshot persistence |
| `workflows.ts` | Workflow example |
| `layers.ts` | Infrastructure composition |
| `server.ts` | Dev HTTP server |

Run it with:

```bash
bun examples/order/server.ts
```

## Profiler — advanced production-style service

**Location**: `services/profiler/`

The production-ops reference: snapshots, transactional outbox, durable publish
workflows, ClickHouse projections, replay, and cluster wiring.

| File | Purpose |
|------|---------|
| `contracts.ts` | Rich event/command/state schemas built with N2 helpers |
| `aggregate.ts` | `N2.define()` with typed evolve/decide maps |
| `entity.ts` | `toEntityLayer` with snapshots, postHandle, and read overrides |
| `projector.ts` | EventLog handlers coordinating projection store and outbox |
| `outbox.ts` | `makeOutboxJsonService` with worker retry defaults |
| `workflows.ts` | `makePublishWorkflow` for durable event publishing |
| `snapshots.ts` | `makeSnapshotService` plus `makeSnapshotOps` |
| `replay.ts` | `makeReplayProgram` for projection rebuilds |
| `layers.ts` | Dev and cluster infrastructure composition |
| `server.ts` | Dev HTTP server |
| `cluster.ts` | Cluster deployment |

Useful commands:

```bash
bun services/profiler/server.ts
bun services/profiler/replay.ts --dry-run
```

### Starting the profiler cluster

`services/profiler/cluster.ts` runs the profiler through `@effect/cluster`
sharding. Use it when you want multiple runners sharing PostgreSQL-backed
cluster storage while exposing JSON-RPC over HTTP.

Required infrastructure:

- PostgreSQL, via `DATABASE_URL`; used for sharding storage, snapshots, event
  journal, migrations, and the outbox.
- ClickHouse, via `CLICKHOUSE_URL`; used for read projections.

Environment variables:

| Variable | Required | Purpose |
|----------|----------|---------|
| `DATABASE_URL` | Yes | PostgreSQL connection string |
| `CLICKHOUSE_URL` | Yes | ClickHouse HTTP URL, for example `http://localhost:8123` |
| `CLICKHOUSE_DATABASE` | No | ClickHouse database; defaults to `default` |
| `HOST` | No | Runner host advertised to other runners; defaults to `127.0.0.1` |
| `PORT` | No | Runner-to-runner cluster port; default comes from `BunClusterHttp` |
| `API_PORT` | No | Public JSON-RPC / health HTTP port; defaults to `4100` |
| `SHARDS_PER_GROUP` | No | Total shard count; keep identical across all runners |

Single runner:

```bash
DATABASE_URL=postgres://user:pass@localhost:5432/n2 \
CLICKHOUSE_URL=http://localhost:8123 \
HOST=127.0.0.1 \
PORT=34431 \
API_PORT=4100 \
bun services/profiler/cluster.ts
```

Two local runners:

```bash
# Terminal 1
DATABASE_URL=postgres://user:pass@localhost:5432/n2 \
CLICKHOUSE_URL=http://localhost:8123 \
HOST=127.0.0.1 \
PORT=34431 \
API_PORT=4100 \
bun services/profiler/cluster.ts

# Terminal 2
DATABASE_URL=postgres://user:pass@localhost:5432/n2 \
CLICKHOUSE_URL=http://localhost:8123 \
HOST=127.0.0.1 \
PORT=34432 \
API_PORT=4101 \
bun services/profiler/cluster.ts
```

Both runners must share the same PostgreSQL and ClickHouse configuration.
Send requests to either API port; the proxy handlers forward each command to
the runner that owns the command's shard.

Smoke checks:

```bash
curl -s http://localhost:4100/health
curl -s http://localhost:4101/health
```

The JSON-RPC endpoint is `/rpc/profile-provider`. For local development
without sharding, use `bun services/profiler/server.ts` instead.
