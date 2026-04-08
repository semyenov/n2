/**
 * Typed clients for the Order service.
 *
 * Two options:
 *
 * 1. Effect client — use inside Effect programs (full tracing, composition)
 * 2. Promise client — use in plain JS/TS frontends with no Effect dependency
 */
import * as N2 from "../../framework/helpers/index.js"
import { OrderRpcs } from "./contracts.js"

const URL = "http://localhost:4000/rpc/orders"

// ---------------------------------------------------------------------------
// Option 1: Effect client (for Effect-based code)
//
//   Effect.gen(function* () {
//     const client = yield* OrderEffectClient
//     const result = yield* client.CreateOrder({ orderId: "o-1", customerId: "c-1" })
//   }).pipe(Effect.scoped, Effect.runPromise)
// ---------------------------------------------------------------------------

export const OrderEffectClient = N2.makeHttpClient(OrderRpcs, URL)

// ---------------------------------------------------------------------------
// Option 2: Promise client (for plain JS/TS frontends — React, Vue, etc.)
//
//   const result = await client.CreateOrder({ orderId: "o-1", customerId: "c-1" })
//   const result = await client.AddItem({ orderId: "o-1", sku: "SKU-1", quantity: 2, price: 9.99 })
//   const result = await client.SubmitOrder({ orderId: "o-1" })
//   const result = await client.CancelOrder({ orderId: "o-1", reason: "changed mind" })
// ---------------------------------------------------------------------------

export const client = N2.makePromiseClient(OrderRpcs, URL)
