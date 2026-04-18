import { SqlClient } from "@effect/sql/SqlClient"
import * as Effect from "effect/Effect"

export default Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`
    ALTER TABLE profile_provider_event_outbox
      ADD COLUMN IF NOT EXISTS occurred_at TEXT NOT NULL DEFAULT '',
      ADD COLUMN IF NOT EXISTS created_at TEXT NOT NULL DEFAULT '',
      ADD COLUMN IF NOT EXISTS updated_at TEXT NOT NULL DEFAULT '',
      ADD COLUMN IF NOT EXISTS published_at TEXT NOT NULL DEFAULT '',
      ADD COLUMN IF NOT EXISTS next_attempt_at TEXT NOT NULL DEFAULT ''
  `
  const now = new Date().toISOString()
  yield* sql`
    UPDATE profile_provider_event_outbox
    SET occurred_at = CASE WHEN occurred_at = '' THEN created_at ELSE occurred_at END,
        created_at = CASE WHEN created_at = '' THEN ${now} ELSE created_at END,
        updated_at = CASE WHEN updated_at = '' THEN ${now} ELSE updated_at END
  `
})
