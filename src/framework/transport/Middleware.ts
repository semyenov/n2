/**
 * @since 1.0.0
 * @module Middleware
 *
 * RPC middleware for cross-cutting concerns (auth, logging, metrics).
 * Uses @effect/rpc RpcMiddleware natively.
 */
import * as Effect from "effect/Effect"
import * as Context from "effect/Context"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { RpcMiddleware } from "@effect/rpc"

/**
 * Authentication middleware. Extracts auth context from RPC headers
 * and provides it to handlers.
 *
 * @since 1.0.0
 * @category middleware
 */
export interface AuthContext {
  readonly userId: string
  readonly tenantId: string
}

export class Auth extends Context.Tag("n2/Auth")<Auth, AuthContext>() {}

export class AuthMiddleware extends RpcMiddleware.Tag<AuthMiddleware>()(
  "n2/AuthMiddleware",
  {
    provides: Auth,
    failure: Schema.String
  }
) {}

/**
 * Layer that implements AuthMiddleware by extracting from headers.
 *
 * @since 1.0.0
 * @category layers
 */
export const AuthMiddlewareLive = Layer.succeed(
  AuthMiddleware,
  ({ headers }) => {
    const userId = (headers as Record<string, string>)["x-user-id"]
    const tenantId = (headers as Record<string, string>)["x-tenant-id"]
    if (!userId || !tenantId) {
      return Effect.fail("Missing auth headers: x-user-id, x-tenant-id")
    }
    return Effect.succeed({ userId, tenantId })
  }
)

/**
 * Logging middleware. Logs every RPC call.
 *
 * @since 1.0.0
 * @category middleware
 */
export class LoggingMiddleware extends RpcMiddleware.Tag<LoggingMiddleware>()(
  "n2/LoggingMiddleware",
  { wrap: true, optional: true }
) {}

export const LoggingMiddlewareLive = Layer.succeed(
  LoggingMiddleware,
  ({ rpc, payload, next }) =>
    Effect.gen(function*() {
      yield* Effect.log(`RPC ${rpc.key} called`)
      const result = yield* next
      yield* Effect.log(`RPC ${rpc.key} completed`)
      return result
    })
)

/**
 * Re-export RpcMiddleware for custom middleware.
 *
 * @since 1.0.0
 * @category re-exports
 */
export { RpcMiddleware }
