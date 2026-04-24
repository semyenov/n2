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

const isObject = (value: unknown): value is object =>
  typeof value === "object" && value !== null

const getField = (value: object, key: string): unknown =>
  Reflect.get(value, key)

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

        const json = await response.json()
        if (!Array.isArray(json) || json.length === 0 || !isObject(json[0])) {
          throw new Error("Invalid JSON-RPC response")
        }

        const item = json[0]
        const error = getField(item, "error")

        if (error !== undefined) {
          // layerJsonRpc wraps typed errors in { _tag: "Cause", data: { _tag: "Fail", error: ... } }
          if (isObject(error) && getField(error, "_tag") === "Cause") {
            const data = getField(error, "data")
            if (isObject(data) && getField(data, "_tag") === "Fail") {
              const causeError = getField(data, "error")
              if (causeError !== undefined) throw causeError
            }
          }
          throw error
        }
        return getField(item, "result")
      }
    }
  })
}
