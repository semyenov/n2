import { SqlClient } from "@effect/sql/SqlClient"
import * as Effect from "effect/Effect"

export default Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`
    CREATE TABLE IF NOT EXISTS profile_provider_schema_versions (
      id            TEXT PRIMARY KEY,
      version       TEXT NOT NULL,
      schema_type   TEXT NOT NULL,
      schema_json   TEXT NOT NULL,
      is_active     BOOLEAN NOT NULL,
      deprecated_at TEXT NOT NULL
    )
  `
})
