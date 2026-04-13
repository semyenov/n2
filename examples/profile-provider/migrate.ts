/**
 * Database migrations — reads from the local migrations/ directory.
 *
 * Migrator.make({}) returns a runner function. fromGlob takes a record of
 * dynamic imports keyed by filename; it parses the numeric prefix for ordering
 * and the name segment for logging.
 *
 * MigrationsLayer has R = SqlClient — wire it before starting the service.
 * Subsequent runs are no-ops for already-applied migrations.
 */
import { Migrator } from "@effect/sql"
import * as Layer from "effect/Layer"

const runMigrations = Migrator.make({})

export const MigrationsLayer = Layer.effectDiscard(
  runMigrations({
    loader: Migrator.fromGlob({
      "./migrations/0001_profile_provider_profiles_read.ts": () => import("./migrations/0001_profile_provider_profiles_read.js"),
      "./migrations/0002_profile_provider_snapshots_read.ts": () => import("./migrations/0002_profile_provider_snapshots_read.js"),
      "./migrations/0003_profile_provider_snapshots.ts": () => import("./migrations/0003_profile_provider_snapshots.js"),
      "./migrations/0004_profile_provider_event_outbox.ts": () => import("./migrations/0004_profile_provider_event_outbox.js"),
      "./migrations/0005_profile_provider_schema_versions.ts": () => import("./migrations/0005_profile_provider_schema_versions.js")
    })
  })
)
