/**
 * @since 1.0.0
 * @module BunHttpServer
 *
 * Wires @effect/platform-bun HTTP server with RPC routes.
 */
import * as Layer from "effect/Layer"
import { BunHttpServer } from "@effect/platform-bun"
import { HttpLayerRouter } from "@effect/platform"

/**
 * Creates a Bun HTTP server layer that serves the provided RPC routes.
 *
 * @since 1.0.0
 * @category constructors
 */
export const make = (config: {
  readonly port: number
  readonly rpcRoutes: Layer.Layer<never, never, any>
}): Layer.Layer<never, never, any> =>
  HttpLayerRouter.serve(config.rpcRoutes).pipe(
    Layer.provide(BunHttpServer.layer({ port: config.port }))
  )
