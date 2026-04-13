import { SqlClient } from "@effect/sql/SqlClient"
import * as Effect from "effect/Effect"

export default Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`
    CREATE TABLE IF NOT EXISTS profile_provider_snapshots_read (
      snapshot_id    TEXT PRIMARY KEY,
      profile_id     TEXT NOT NULL,
      branch_id      TEXT NOT NULL,
      revision       INTEGER NOT NULL,
      snapshot_type  TEXT NOT NULL,
      profile_json   TEXT NOT NULL,
      metadata_json  TEXT NOT NULL,
      schema_version TEXT NOT NULL,
      summary        TEXT NOT NULL,
      published      BOOLEAN NOT NULL,
      strategy_json  TEXT NOT NULL,
      created_at     TEXT NOT NULL,
      created_by     TEXT NOT NULL
    )
  `
})
