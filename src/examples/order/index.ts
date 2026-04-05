/**
 * Order service entry point (dev mode).
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { BunHttpServer } from "@effect/platform-bun"
import { HttpLayerRouter } from "@effect/platform"
import { OrderRpcRoute } from "./http.js"
import { InfrastructureLayer } from "./layers.js"

const ServerLayer = HttpLayerRouter.serve(OrderRpcRoute).pipe(
  Layer.provide(BunHttpServer.layer({ port: 3000 })),
  Layer.provide(InfrastructureLayer)
)

const main = Effect.gen(function* () {
  yield* Effect.log("Order service on port 3000")
  yield* Effect.never
}).pipe(Effect.provide(ServerLayer))

Effect.runPromise(main as Effect.Effect<void, never, never>)
