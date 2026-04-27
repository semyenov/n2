import { SqlClient } from "@effect/sql/SqlClient"
import * as Effect from "effect/Effect"

export default Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`
    CREATE TABLE IF NOT EXISTS request_provider_requests_read (
      request_id             TEXT PRIMARY KEY,
      client_id              TEXT NOT NULL,
      status                 TEXT NOT NULL,
      current_revision       INTEGER NOT NULL,
      current_schema_version TEXT NOT NULL,
      request_json           TEXT NOT NULL,
      latest_metadata_json   TEXT NOT NULL,
      latest_snapshot_id     TEXT NOT NULL,
      created_at             TEXT NOT NULL,
      updated_at             TEXT NOT NULL
    )
  `
})
