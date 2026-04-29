import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { BunHttpServer } from "@effect/platform-bun"
import { HttpLayerRouter } from "@effect/platform"
import type * as Rpc from "@effect/rpc/Rpc"
import type * as RpcGroup from "@effect/rpc/RpcGroup"
import { makeHealthRoute, makePgSqlLayer, makeRpcHttpRoute } from "./Runtime.js"

type PgSqlLayer = ReturnType<typeof makePgSqlLayer>

export interface ServerEntrypointConfig<Rpcs extends Rpc.Any, HE, HR, MO, ME, MR, IO, IE, IR> {
  readonly group: RpcGroup.RpcGroup<Rpcs>
  readonly path: HttpLayerRouter.PathInput
  readonly handlers: Layer.Layer<
    Rpc.ToHandler<Rpcs> | Rpc.Context<Rpcs> | Rpc.Middleware<Rpcs>,
    HE,
    HR
  >
  readonly migrationsLayer: Layer.Layer<MO, ME, MR>
  readonly infrastructureLayer: Layer.Layer<IO, IE, IR>
  readonly defaultPort: number
  readonly banner: ReadonlyArray<string>
  readonly portConfig?: Config.Config<number>
  readonly sqlLayer?: PgSqlLayer
}

/**
 * Compose the dev-server entrypoint Effect. Reads the port config exactly once
 * and threads it to both `BunHttpServer.layerConfig` and the startup banner.
 *
 * Pass the result directly to `BunRuntime.runMain(...)`.
 */
export const makeServerEntrypoint = <Rpcs extends Rpc.Any, HE, HR, MO, ME, MR, IO, IE, IR>(
  config: ServerEntrypointConfig<Rpcs, HE, HR, MO, ME, MR, IO, IE, IR>
) => {
  const portConfig = config.portConfig ?? Config.integer("PORT").pipe(Config.withDefault(config.defaultPort))
  const sqlLayer = config.sqlLayer ?? makePgSqlLayer()

  const RpcRoute = makeRpcHttpRoute({
    group: config.group,
    path: config.path,
    handlers: config.handlers
  })

  const HealthRoute = makeHealthRoute()

  const ServerLayer = HttpLayerRouter.serve(
    Layer.mergeAll(RpcRoute, HealthRoute)
  ).pipe(
    Layer.provide(BunHttpServer.layerConfig(
      Config.map(portConfig, (port) => ({ port }))
    )),
    Layer.provide(config.migrationsLayer),
    Layer.provide(config.infrastructureLayer),
    Layer.provide(sqlLayer)
  )

  const main = Effect.gen(function* () {
    const port = yield* portConfig
    for (const line of config.banner) {
      yield* Effect.log(line.replace("{port}", String(port)))
    }
    return yield* Effect.never
  })

  return main.pipe(Effect.provide(ServerLayer), Effect.orDie)
}
