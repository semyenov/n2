/**
 * Order event projection — materialises a read model from the EventLog stream.
 *
 * EventLog.group registers handlers that run whenever a matching event is
 * appended to the journal (SqlEventJournal in production, layerMemory in tests).
 *
 * Each handler upserts into the orders_read / order_items_read tables
 * (created by MigrationsLayer on startup). SqlClient is accessed via
 * `yield* SqlClient` inside each handler — the service is provided by the
 * InfrastructureLayer → EventLogLayer chain.
 *
 * Effect.orDie converts SqlError to a defect so a failed projection crashes
 * the handler loop and is restarted by the runtime, rather than silently
 * succeeding with a stale read model.
 */
import * as Effect from "effect/Effect"
import { EventLog } from "@effect/experimental"
import { SqlClient } from "@effect/sql/SqlClient"
import { OrderEventGroup } from "./events.js"

export const OrderProjectionLayer = EventLog.group(
  OrderEventGroup,
  (handlers) =>
    handlers
      .handle("OrderCreated", ({ payload }) =>
        Effect.gen(function* () {
          const sql = yield* SqlClient
          yield* sql`
            INSERT INTO orders_read
              (order_id, customer_id, status, total_amount, created_at, updated_at)
            VALUES (
              ${payload.orderId}, ${payload.customerId}, 'draft', 0,
              ${payload.createdAt.toJSON()}, ${payload.createdAt.toJSON()}
            )
            ON CONFLICT (order_id) DO NOTHING
          `
        }).pipe(Effect.orDie)
      )
      .handle("ItemAdded", ({ payload }) =>
        Effect.gen(function* () {
          const sql = yield* SqlClient
          yield* sql`
            INSERT INTO order_items_read (order_id, sku, quantity, price)
            VALUES (${payload.orderId}, ${payload.sku}, ${payload.quantity}, ${payload.price})
            ON CONFLICT (order_id, sku) DO UPDATE SET
              quantity = EXCLUDED.quantity,
              price    = EXCLUDED.price
          `
        }).pipe(Effect.orDie)
      )
      .handle("OrderSubmitted", ({ payload }) =>
        Effect.gen(function* () {
          const sql = yield* SqlClient
          yield* sql`
            UPDATE orders_read
            SET status = 'submitted', updated_at = ${payload.submittedAt.toJSON()}
            WHERE order_id = ${payload.orderId}
          `
        }).pipe(Effect.orDie)
      )
      .handle("OrderCancelled", ({ payload }) =>
        Effect.gen(function* () {
          const sql = yield* SqlClient
          yield* sql`
            UPDATE orders_read
            SET status = 'cancelled', updated_at = ${payload.cancelledAt.toJSON()}
            WHERE order_id = ${payload.orderId}
          `
        }).pipe(Effect.orDie)
      )
)
