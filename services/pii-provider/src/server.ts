/**
 * Dev server — in-memory state, PostgreSQL infrastructure, and ClickHouse read projections.
 *
 * Required:
 *   DATABASE_URL    PostgreSQL connection string
 *   CLICKHOUSE_URL  ClickHouse HTTP URL, e.g. http://localhost:8123
 *
 * Optional:
 *   CLICKHOUSE_DATABASE  ClickHouse database (default: default)
 *   PORT                 Public JSON-RPC / health port (default: 4120)
 */
import { BunRuntime } from "@effect/platform-bun"
import { makeServerEntrypoint } from "@semyenov/n2/runtime"
import { PIIProviderRpcs } from "./contracts/commands.js"
import { PIIProviderHandlers } from "./entity.js"
import { InfrastructureLayer } from "./layers.js"
import { MigrationsLayer } from "./migrate.js"

BunRuntime.runMain(
  makeServerEntrypoint({
    group: PIIProviderRpcs,
    path: "/rpc/pii-provider",
    handlers: PIIProviderHandlers,
    migrationsLayer: MigrationsLayer,
    infrastructureLayer: InfrastructureLayer,
    defaultPort: 4120,
    observability: {
      serviceName: "pii-provider",
      attributes: {
        "service.namespace": "n2",
        "service.mode": "server"
      }
    },
    banner: [
      "PII provider service (dev mode) on http://localhost:{port}",
      "Endpoints: StoreExtractedPII | CreatePIIRecord | GetPIIRecord"
    ]
  })
)
