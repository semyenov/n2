/**
 * Order snapshot service — saves and loads aggregate state to/from PostgreSQL.
 *
 * Snapshots allow an entity (or the dev-mode in-memory store) to recover
 * state without replaying every event from the beginning. A snapshot is saved
 * every SNAPSHOT_EVERY events; on startup the latest snapshot is loaded and
 * only events after it would need to be replayed.
 *
 * Serialization:
 *   OrderState uses Option<T> and DateTime.Utc — not directly JSON-friendly.
 *   encodeState/decodeState convert to/from a plain JSON-serialisable object:
 *     Option<string>      → string | null
 *     Option<DateTime>    → ISO string | null   (via Schema.DateTimeUtc codec)
 *     ReadonlyArray<LineItem> → plain object array
 */
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import { SqlClient } from "@effect/sql/SqlClient"
import type { SqlError } from "@effect/sql/SqlError"
import { LineItem, OrderState, OrderStatus } from "./contracts.js"

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/** Save a snapshot every this many events. Lower = more frequent, faster recovery. */
export const SNAPSHOT_EVERY = 50

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

const SnapshotJson = Schema.Struct({
  status: OrderStatus,
  orderId: Schema.NullOr(Schema.String),
  customerId: Schema.NullOr(Schema.String),
  items: Schema.Array(Schema.Struct({
    sku: Schema.String,
    quantity: Schema.Number,
    price: Schema.Number
  })),
  totalAmount: Schema.Number,
  cancelledAt: Schema.NullOr(Schema.String)
})

type SnapshotJson = typeof SnapshotJson.Type

const encodeDateTime = Schema.encodeSync(Schema.DateTimeUtc)
const decodeDateTime = Schema.decodeSync(Schema.DateTimeUtc)
const decodeSnapshotJson = Schema.decodeUnknownSync(SnapshotJson)

const encodeState = (state: OrderState): string =>
  JSON.stringify({
    status:      state.status,
    orderId:     Option.getOrNull(state.orderId),
    customerId:  Option.getOrNull(state.customerId),
    items:       state.items.map(i => ({ sku: i.sku, quantity: i.quantity, price: i.price })),
    totalAmount: state.totalAmount,
    cancelledAt: Option.match(state.cancelledAt, {
      onNone: () => null,
      onSome: encodeDateTime
    })
  } satisfies SnapshotJson)

const decodeState = (json: string): OrderState => {
  const d = decodeSnapshotJson(JSON.parse(json))
  return new OrderState({
    status:      d.status,
    orderId:     Option.fromNullable(d.orderId),
    customerId:  Option.fromNullable(d.customerId),
    items:       d.items.map(i => new LineItem(i)),
    totalAmount: d.totalAmount,
    cancelledAt: d.cancelledAt !== null
      ? Option.some(decodeDateTime(d.cancelledAt))
      : Option.none()
  })
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

export type SnapshotEntry = { readonly state: OrderState; readonly revision: number }

export class OrderSnapshots extends Context.Tag("OrderSnapshots")<
  OrderSnapshots,
  {
    /** Load the latest snapshot for an order. Returns none if no snapshot exists. */
    readonly load: (orderId: string) => Effect.Effect<Option.Option<SnapshotEntry>, SqlError>
    /** Persist the current state as the latest snapshot for an order. */
    readonly save: (orderId: string, state: OrderState, revision: number) => Effect.Effect<void, SqlError>
  }
>() {}

export const OrderSnapshotsLive = Layer.effect(
  OrderSnapshots,
  Effect.gen(function* () {
    const sql = yield* SqlClient

    return {
      load: (orderId) =>
        sql<{ readonly state_json: string; readonly revision: number }>`
          SELECT state_json, revision
          FROM order_snapshots
          WHERE order_id = ${orderId}
        `.pipe(
          Effect.map((rows) => {
            const row = rows[0]
            if (!row) return Option.none<SnapshotEntry>()
            return Option.some({ state: decodeState(row.state_json), revision: row.revision })
          })
        ),

      save: (orderId, state, revision) =>
        sql`
          INSERT INTO order_snapshots (order_id, state_json, revision, saved_at)
          VALUES (${orderId}, ${encodeState(state)}, ${revision}, ${new Date().toISOString()})
          ON CONFLICT (order_id) DO UPDATE SET
            state_json = EXCLUDED.state_json,
            revision   = EXCLUDED.revision,
            saved_at   = EXCLUDED.saved_at
        `.pipe(Effect.asVoid)
    }
  })
)
