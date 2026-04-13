import { SqlClient } from "@effect/sql/SqlClient"
import * as Effect from "effect/Effect"

export default Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`
    CREATE TABLE IF NOT EXISTS orders_read (
      order_id     TEXT PRIMARY KEY,
      customer_id  TEXT NOT NULL DEFAULT '',
      status       TEXT NOT NULL,
      total_amount DOUBLE PRECISION NOT NULL DEFAULT 0,
      created_at   TEXT NOT NULL,
      updated_at   TEXT NOT NULL
    )
  `
})
