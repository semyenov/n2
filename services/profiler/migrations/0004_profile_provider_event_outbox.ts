import { SqlClient } from "@effect/sql/SqlClient"
import * as Effect from "effect/Effect"

export default Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`
    CREATE TABLE IF NOT EXISTS profile_provider_event_outbox (
      id            TEXT PRIMARY KEY,
      profile_id    TEXT NOT NULL,
      revision      INTEGER NOT NULL,
      topic         TEXT NOT NULL,
      partition_key TEXT NOT NULL,
      occurred_at   TEXT NOT NULL,
      payload_json  TEXT NOT NULL,
      headers_json  TEXT NOT NULL,
      status        TEXT NOT NULL,
      retry_count   INTEGER NOT NULL,
      last_error    TEXT NOT NULL,
      created_at    TEXT NOT NULL,
      updated_at    TEXT NOT NULL,
      published_at  TEXT NOT NULL,
      next_attempt_at TEXT NOT NULL
    )
  `
})
