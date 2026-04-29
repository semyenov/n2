/**
 * Order snapshot service — saves and loads aggregate state to/from PostgreSQL.
 *
 * Snapshots allow an entity (or the dev-mode in-memory store) to recover
 * state without replaying every event from the beginning. A snapshot is saved
 * every SNAPSHOT_EVERY events; on startup the latest snapshot is loaded and
 * only events after it would need to be replayed.
 *
 * Implementation: delegates to the framework's `makeSnapshotService` helper
 * with a `Schema.transform` codec that converts between OrderState (which uses
 * `Option`/`DateTime` runtime types) and a JSON-friendly encoded form using
 * `Schema.OptionFromNullOr` and `Schema.DateTimeUtc`. Per CLAUDE.md, generic
 * services like `SnapshotService<State>` use the `Context.Tag + makeLive`
 * factory pattern (Effect.Service doesn't model type parameters cleanly).
 */
import * as Context from "effect/Context"
import * as Schema from "effect/Schema"
import {
  makeSnapshotService,
  type SnapshotEntry as SnapshotEntryGeneric,
  type SnapshotService
} from "@semyenov/n2/framework/helpers"
import { LineItem, OrderState, OrderStatus } from "./contracts.js"

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/** Save a snapshot every this many events. Lower = more frequent, faster recovery. */
export const SNAPSHOT_EVERY = 50

// ---------------------------------------------------------------------------
// Codec
//
// OrderState's runtime form uses `OptionFromSelf` (Option<T> on both sides),
// so we transform via a JSON-friendly intermediate that uses
// `OptionFromNullOr` (Option<T> ↔ T | null) and `DateTimeUtc` (DateTime ↔ ISO).
// ---------------------------------------------------------------------------

const OrderStateJson = Schema.Struct({
  status:      OrderStatus,
  orderId:     Schema.OptionFromNullOr(Schema.String),
  customerId:  Schema.OptionFromNullOr(Schema.String),
  items:       Schema.Array(LineItem),
  totalAmount: Schema.Number,
  cancelledAt: Schema.OptionFromNullOr(Schema.DateTimeUtc)
})

const OrderStateCodec = Schema.transform(
  OrderStateJson,
  Schema.typeSchema(OrderState),
  {
    strict: true,
    decode: (json) => new OrderState(json),
    encode: (state) => state
  }
)

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

export type SnapshotEntry = SnapshotEntryGeneric<OrderState>

export class OrderSnapshots extends Context.Tag("OrderSnapshots")<
  OrderSnapshots,
  SnapshotService<OrderState>
>() {}

export const OrderSnapshotsLive = makeSnapshotService({
  table: "order_snapshots",
  stateSchema: OrderStateCodec,
  idColumn: "order_id"
}).makeLive(OrderSnapshots)
