/**
 * @since 1.0.0
 * @module N2
 *
 * Typed HTTP client factory for N2 services.
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { FetchHttpClient } from "@effect/platform"
import type { Rpc, RpcGroup } from "@effect/rpc"
import { RpcClient, RpcSerialization } from "@effect/rpc"

/**
 * Maps an RpcClient to a plain Promise-based client shape.
 * Each method returns `Promise<SuccessType>` instead of `Effect<SuccessType, ErrorType>`.
 * Errors are thrown as rejected Promises.
 */
export type RpcPromiseClient<Rpcs extends Rpc.Any> = {
  readonly [K in keyof RpcClient.RpcClient<Rpcs>]:
    RpcClient.RpcClient<Rpcs>[K] extends (input: infer I, ...args: ReadonlyArray<never>) => Effect.Effect<infer A, infer _E, infer _R>
      ? (input: I) => Promise<A>
      : never
}

/**
 * Creates a fully typed HTTP client for an RpcGroup.
 *
 * The returned Effect is scoped — acquire it inside `Effect.scoped`.
 * Each method is named after the command `_tag` and carries the correct
 * input/output types from the command's `Schema.TaggedRequest`.
 *
 * Uses `FetchHttpClient` (browser + Bun) and JSON serialization to match
 * the server's `RpcSerialization.layerJson`.
 *
 * @example
 * ```ts
 * const client = yield* makeHttpClient(OrderRpcs, "http://localhost:4000/rpc/orders")
 * const result = yield* client.CreateOrder({ orderId: "o-1", customerId: "c-1" })
 * ```
 */
export const makeHttpClient = <Rpcs extends Rpc.Any>(
  group: RpcGroup.RpcGroup<Rpcs>,
  url: string
) => {
  const protocol = RpcClient.layerProtocolHttp({ url }).pipe(
    Layer.provide([FetchHttpClient.layer, RpcSerialization.layerJson])
  )
  return RpcClient.make(group).pipe(Effect.provide(protocol))
}

/**
 * Creates a Promise-based client for use in non-Effect frontends.
 *
 * Each call creates a fresh scoped connection — suitable for standard
 * HTTP RPC (stateless, one request per call).
 *
 * @example
 * ```ts
 * const client = makePromiseClient(OrderRpcs, "http://localhost:4000/rpc/orders")
 *
 * const result = await client.CreateOrder({ orderId: "o-1", customerId: "c-1" })
 * console.log(result) // { orderId: "o-1", revision: 1 }
 * ```
 */
export const makePromiseClient = <Rpcs extends Rpc.Any>(
  group: RpcGroup.RpcGroup<Rpcs>,
  url: string
): RpcPromiseClient<Rpcs> => {
  const getClient = makeHttpClient(group, url)

  return new Proxy({} as RpcPromiseClient<Rpcs>, {
    get(_, method: string) {
      return (payload: unknown) =>
        Effect.gen(function* () {
          const client = yield* getClient
          const fn = (client as Record<string, (p: unknown) => Effect.Effect<unknown>>)[method]!
          return yield* fn(payload)
        }).pipe(Effect.scoped, Effect.runPromise)
    }
  })
}
