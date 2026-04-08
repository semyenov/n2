/**
 * @since 1.0.0
 * @module N2
 *
 * Zero-dependency typed HTTP client for N2 services.
 *
 * Uses plain `fetch` — no Effect runtime in the bundle.
 * Effect types are import-only (erased at compile time).
 */
import type { Rpc, RpcGroup } from "@effect/rpc"
import type { RpcPromiseClient } from "./Client.js"

/**
 * Creates a fully typed, Promise-based HTTP client using plain `fetch`.
 *
 * - No Effect runtime in your bundle — only type imports are used
 * - Works in any browser or runtime that has `fetch`
 * - Full type safety: methods are named after commands, payloads and
 *   return types are inferred from `Schema.TaggedRequest`
 * - Errors are thrown as plain objects `{ _tag, message, ... }`
 *
 * @example
 * ```ts
 * const client = makeFetchClient(OrderRpcs, "http://localhost:4000/rpc/orders")
 *
 * const result = await client.CreateOrder({ orderId: "o-1", customerId: "c-1" })
 * // result: { orderId: string; revision: number }
 * ```
 */
export const makeFetchClient = <Rpcs extends Rpc.Any>(
  _group: RpcGroup.RpcGroup<Rpcs>,
  url: string
): RpcPromiseClient<Rpcs> => {
  let id = 0

  return new Proxy({} as RpcPromiseClient<Rpcs>, {
    get(_, method: string) {
      return async (params: unknown) => {
        const response = await fetch(url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ jsonrpc: "2.0", method, params, id: ++id })
        })

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`)
        }

        const [item] = await response.json() as [{ result?: unknown; error?: { _tag?: string; message: string; data?: unknown } }]

        if (item.error !== undefined) {
          // layerJsonRpc wraps typed errors in { _tag: "Cause", data: { _tag: "Fail", error: ... } }
          const err = item.error
          if (err._tag === "Cause" && typeof err.data === "object" && err.data !== null) {
            const cause = err.data as { _tag?: string; error?: unknown }
            if (cause._tag === "Fail" && cause.error !== undefined) throw cause.error
          }
          throw err
        }
        return item.result
      }
    }
  })
}
