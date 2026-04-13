import { SqlClient } from "@effect/sql/SqlClient"
import * as Effect from "effect/Effect"

export default Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`
    CREATE TABLE IF NOT EXISTS profile_provider_profiles_read (
      profile_id            TEXT PRIMARY KEY,
      owner_agent_id        TEXT NOT NULL,
      active_branch_id      TEXT NOT NULL,
      status                TEXT NOT NULL,
      current_revision      INTEGER NOT NULL,
      current_schema_version TEXT NOT NULL,
      masked_profile_json   TEXT NOT NULL,
      latest_metadata_json  TEXT NOT NULL,
      latest_pii_storage_key TEXT NOT NULL,
      pii_jurisdiction      TEXT NOT NULL,
      published_snapshot_id TEXT NOT NULL,
      created_at            TEXT NOT NULL,
      updated_at            TEXT NOT NULL
    )
  `
})
