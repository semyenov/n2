import { SqlClient } from "@effect/sql/SqlClient"
import * as Effect from "effect/Effect"

export default Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`
    ALTER TABLE profile_provider_event_outbox
      ADD COLUMN IF NOT EXISTS available_at TEXT,
      ADD COLUMN IF NOT EXISTS worker_id TEXT,
      ADD COLUMN IF NOT EXISTS locked_until TEXT,
      ADD COLUMN IF NOT EXISTS last_attempt_at TEXT,
      ADD COLUMN IF NOT EXISTS processed_at TEXT,
      ADD COLUMN IF NOT EXISTS created_at TEXT,
      ADD COLUMN IF NOT EXISTS updated_at TEXT
  `
  yield* sql`
    UPDATE profile_provider_event_outbox
    SET available_at = COALESCE(available_at, CURRENT_TIMESTAMP),
        created_at = COALESCE(created_at, CURRENT_TIMESTAMP),
        updated_at = COALESCE(updated_at, CURRENT_TIMESTAMP)
  `
  yield* sql`
    CREATE INDEX IF NOT EXISTS profile_provider_event_outbox_ready_idx
    ON profile_provider_event_outbox (status, available_at)
  `
  yield* sql`
    CREATE INDEX IF NOT EXISTS profile_provider_event_outbox_lock_idx
    ON profile_provider_event_outbox (status, locked_until)
  `
  yield* sql`
    CREATE INDEX IF NOT EXISTS profile_provider_event_outbox_profile_idx
    ON profile_provider_event_outbox (profile_id)
  `
})
