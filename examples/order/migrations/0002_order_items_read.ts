import { SqlClient } from "@effect/sql/SqlClient"
import * as Effect from "effect/Effect"

export default Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`
    CREATE TABLE IF NOT EXISTS order_items_read (
      order_id TEXT NOT NULL,
      sku      TEXT NOT NULL,
      quantity INTEGER NOT NULL,
      price    DOUBLE PRECISION NOT NULL,
      PRIMARY KEY (order_id, sku)
    )
  `
})
