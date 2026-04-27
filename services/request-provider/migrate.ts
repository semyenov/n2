import { makeMigrationsLayer } from "@semyenov/n2/runtime"

export const MigrationsLayer = makeMigrationsLayer({
  "./migrations/0001_request_provider_requests_read.ts": () => import("./migrations/0001_request_provider_requests_read.js"),
  "./migrations/0002_request_provider_snapshots_read.ts": () => import("./migrations/0002_request_provider_snapshots_read.js"),
  "./migrations/0003_request_provider_snapshots.ts": () => import("./migrations/0003_request_provider_snapshots.js"),
  "./migrations/0004_request_provider_event_outbox.ts": () => import("./migrations/0004_request_provider_event_outbox.js")
})
