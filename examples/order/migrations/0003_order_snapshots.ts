import { SqlClient } from "@effect/sql/SqlClient"
import * as Effect from "effect/Effect"

export default Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`
    CREATE TABLE IF NOT EXISTS order_snapshots (
      order_id   TEXT PRIMARY KEY,
      state_json TEXT NOT NULL,
      revision   INTEGER NOT NULL,
      saved_at   TEXT NOT NULL
    )
  `
})
