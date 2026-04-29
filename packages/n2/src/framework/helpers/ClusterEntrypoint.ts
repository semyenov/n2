import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { BunHttpServer } from "@effect/platform-bun"
import { HttpLayerRouter } from "@effect/platform"
import type * as Rpc from "@effect/rpc/Rpc"
import type * as RpcGroup from "@effect/rpc/RpcGroup"
import {
  makeClusterShardingLayer,
  makeHealthRoute,
  makeHttpTraceMiddleware,
  makePgSqlLayer,
  makeRpcHttpRoute
} from "./Runtime.js"
import { makeObservabilityLayer, ObservabilityDisabled, type ObservabilityOptions } from "./Observability.js"

type PgSqlLayer = ReturnType<typeof makePgSqlLayer>

export interface ClusterEntrypointConfig<
  ProxyRpcs extends Rpc.Any,
  PHE, PHR,
  EO, EE, ER,
  CIO, CIE, CIR,
  MO, ME, MR
> {
  readonly proxyGroup: RpcGroup.RpcGroup<ProxyRpcs>
  readonly path: HttpLayerRouter.PathInput
  readonly proxyHandlers: Layer.Layer<
    Rpc.ToHandler<ProxyRpcs> | Rpc.Context<ProxyRpcs> | Rpc.Middleware<ProxyRpcs>,
    PHE,
    PHR
  >
  readonly entityLayer: Layer.Layer<EO, EE, ER>
  readonly clusterInfrastructureLayer: Layer.Layer<CIO, CIE, CIR>
  readonly migrationsLayer: Layer.Layer<MO, ME, MR>
  readonly defaultApiPort: number
  readonly banner: ReadonlyArray<string>
  readonly apiPortConfig?: Config.Config<number>
  readonly sqlLayer?: PgSqlLayer
  readonly observability?: ObservabilityOptions
}

/**
 * Compose the cluster-mode entrypoint Effect. Reads `API_PORT` exactly once,
 * builds the sharding + entity layers, and serves `proxyGroup` at `path`.
 *
 * Pass the result directly to `BunRuntime.runMain(...)`.
 */
export const makeClusterEntrypoint = <
  ProxyRpcs extends Rpc.Any,
  PHE, PHR,
  EO, EE, ER,
  CIO, CIE, CIR,
  MO, ME, MR
>(
  config: ClusterEntrypointConfig<ProxyRpcs, PHE, PHR, EO, EE, ER, CIO, CIE, CIR, MO, ME, MR>
) => {
  const apiPortConfig = config.apiPortConfig
    ?? Config.integer("API_PORT").pipe(Config.withDefault(config.defaultApiPort))
  const sqlLayer = config.sqlLayer ?? makePgSqlLayer({ minConnections: 4 })
  const ObservabilityLayer = config.observability === undefined
    ? ObservabilityDisabled
    : makeObservabilityLayer(config.observability)

  const RpcRoute = makeRpcHttpRoute({
    group: config.proxyGroup,
    path: config.path,
    handlers: config.proxyHandlers
  })

  const HealthRoute = makeHealthRoute()
  const ShardingLayer = makeClusterShardingLayer(sqlLayer)

  const EntitiesLayer = Layer.provide(
    config.entityLayer,
    Layer.provide(
      Layer.merge(config.clusterInfrastructureLayer, config.migrationsLayer),
      Layer.merge(sqlLayer, ShardingLayer)
    )
  )

  const main = Effect.gen(function* () {
    const apiPort = yield* apiPortConfig
    for (const line of config.banner) {
      yield* Effect.log(line.replace("{port}", String(apiPort)))
    }
    return yield* Effect.never
  })

  return main.pipe(
    Effect.provide(
      Layer.provideMerge(
        Layer.mergeAll(
          HttpLayerRouter.serve(
            Layer.mergeAll(RpcRoute, HealthRoute),
            { middleware: makeHttpTraceMiddleware }
          ),
          EntitiesLayer
        ),
        ObservabilityLayer
      )
    ),
    Effect.provide(
      Layer.mergeAll(
        BunHttpServer.layerConfig(
          Config.map(apiPortConfig, (port) => ({ port }))
        ),
        ShardingLayer
      )
    ),
    Effect.orDie
  )
}
