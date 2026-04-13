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
      "./migrations/0001_orders_read.ts":    () => import("./migrations/0001_orders_read.js"),
      "./migrations/0002_order_items_read.ts": () => import("./migrations/0002_order_items_read.js"),
      "./migrations/0003_order_snapshots.ts": () => import("./migrations/0003_order_snapshots.js")
    })
  })
)
