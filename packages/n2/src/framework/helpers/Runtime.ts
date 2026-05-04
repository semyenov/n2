import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Redacted from "effect/Redacted"
import { BunClusterHttp } from "@effect/platform-bun"
import { ClusterWorkflowEngine } from "@effect/cluster"
import * as Reactivity from "@effect/experimental/Reactivity"
import { Identity } from "@effect/experimental/EventLog"
import { HttpLayerRouter, HttpServerRequest, HttpServerResponse } from "@effect/platform"
import { RpcSerialization, RpcServer } from "@effect/rpc"
import type * as Rpc from "@effect/rpc/Rpc"
import type * as RpcGroup from "@effect/rpc/RpcGroup"
import { Migrator } from "@effect/sql"
import { SqlClient } from "@effect/sql/SqlClient"
import { PgClient } from "@effect/sql-pg"
import { WorkflowEngine } from "@effect/workflow"
import * as ClickhouseClient from "@effect/sql-clickhouse/ClickhouseClient"
import {
  makeSqlEventJournalLayer,
  type EventJournalTableOptions
} from "./EventJournalLayer.js"
import { ObservabilityTracer } from "./Observability.js"

export {
  makeServerEntrypoint,
  type ServerEntrypointConfig
} from "./ServerEntrypoint.js"

export {
  makeClusterEntrypoint,
  type ClusterEntrypointConfig
} from "./ClusterEntrypoint.js"

export {
  makeReplayInfrastructureLayer,
  type ReplayInfrastructureConfig
} from "./ReplayLayer.js"

export {
  makeSqlEventJournalLayer,
  type EventJournalTableOptions
} from "./EventJournalLayer.js"

export {
  ObservabilityDisabled,
  ObservabilityTracer,
  makeObservabilityLayer,
  type ObservabilityOptions
} from "./Observability.js"

export type MigrationGlob = Record<string, () => Promise<unknown>>

export interface MigrationsLayerOptions {
  readonly table?: string
}

export const makeMigrationsLayer = (
  migrations: MigrationGlob,
  options?: MigrationsLayerOptions
) => {
  const runMigrations = Migrator.make({})
  return Layer.effectDiscard(
    runMigrations({
      loader: Migrator.fromGlob(migrations),
      ...(options?.table === undefined ? {} : { table: options.table })
    })
  )
}

export const makeConfiguredClickhouseLayer = () =>
  Layer.unwrapEffect(
    Effect.gen(function* () {
      const redactedUrl = yield* Config.redacted("CLICKHOUSE_URL")
      const url = Redacted.value(redactedUrl)
      const database = yield* Config.string("CLICKHOUSE_DATABASE").pipe(Config.withDefault("default"))

      return Layer.scoped(
        ClickhouseClient.ClickhouseClient,
        ClickhouseClient.make({ url, database })
      ).pipe(
        Layer.provide(Reactivity.layer)
      )
    })
  )

export const makePgSqlLayer = (options?: { readonly minConnections?: number }) =>
  PgClient.layerConfig(
    Config.map(Config.redacted("DATABASE_URL"), (url) => ({
      url,
      ...(options?.minConnections === undefined ? {} : { minConnections: options.minConnections })
    }))
  )

export const makeClusterShardingLayer = <SqlOut, SqlErr, SqlReq>(
  sqlLayer: Layer.Layer<SqlOut, SqlErr, SqlReq>
) =>
  BunClusterHttp.layer({
    transport: "http",
    storage: "sql"
  }).pipe(
    Layer.provide(sqlLayer)
  )

export const makeHealthRoute = (body: unknown = { status: "ok" }) =>
  HttpLayerRouter.add(
    "GET",
    "/health",
    HttpServerResponse.json(body)
  )

export const makeHttpTraceMiddleware = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest
    const tracer = yield* ObservabilityTracer
    const traced = Effect.withSpan(effect, `${request.method} ${request.url}`, {
      kind: "server",
      captureStackTrace: false,
      attributes: {
        "http.request.method": request.method,
        "url.path": request.url
      }
    })

    return yield* Option.match(tracer, {
      onNone: () => traced,
      onSome: (activeTracer) => Effect.withTracer(traced, activeTracer)
    })
  })

export const makeRpcHttpRoute = <Rpcs extends Rpc.Any, E = never, R = never>(options: {
  readonly group: RpcGroup.RpcGroup<Rpcs>
  readonly path: HttpLayerRouter.PathInput
  readonly handlers: Layer.Layer<
    Rpc.ToHandler<Rpcs> | Rpc.Context<Rpcs> | Rpc.Middleware<Rpcs>,
    E,
    R
  >
}) =>
  RpcServer
    .layerHttpRouter({ group: options.group, path: options.path, protocol: "http" })
    .pipe(
      Layer.provide(options.handlers),
      Layer.provide(RpcSerialization.layerJsonRpc())
    )

export interface ServiceInfrastructureConfig<
  EventLogOut,
  EventLogErr,
  EventLogReq,
  ProjectionOut,
  ProjectionErr,
  ProjectionReq,
  ProjectionStoreOut,
  ProjectionStoreErr,
  ProjectionStoreReq,
  OutboxOut,
  OutboxErr,
  OutboxReq,
  OutboxWorkerOut,
  OutboxWorkerErr,
  OutboxWorkerReq,
  SnapshotsOut,
  SnapshotsErr,
  SnapshotsReq,
  PublishHandlersOut,
  PublishHandlersErr,
  PublishHandlersReq,
  PublisherOut,
  PublisherErr,
  PublisherReq,
  ClickhouseOut = never,
  ClickhouseErr = never,
  ClickhouseReq = never,
  ClickhouseBootstrapOut = never,
  ClickhouseBootstrapErr = never,
  ClickhouseBootstrapReq = never
> {
  readonly eventLogLayer: Layer.Layer<EventLogOut, EventLogErr, EventLogReq>
  readonly projectionLayer: Layer.Layer<ProjectionOut, ProjectionErr, ProjectionReq>
  readonly projectionStoreLayer: Layer.Layer<ProjectionStoreOut, ProjectionStoreErr, ProjectionStoreReq>
  readonly outboxLive: Layer.Layer<OutboxOut, OutboxErr, OutboxReq>
  readonly outboxWorkerLive: Layer.Layer<OutboxWorkerOut, OutboxWorkerErr, OutboxWorkerReq>
  readonly snapshotsLive: Layer.Layer<SnapshotsOut, SnapshotsErr, SnapshotsReq>
  readonly publishHandlers: Layer.Layer<PublishHandlersOut, PublishHandlersErr, PublishHandlersReq>
  readonly publisherLive: Layer.Layer<PublisherOut, PublisherErr, PublisherReq>
  readonly clickhouseLayer?: Layer.Layer<ClickhouseOut, ClickhouseErr, ClickhouseReq>
  readonly clickhouseBootstrapLayer?: Layer.Layer<ClickhouseBootstrapOut, ClickhouseBootstrapErr, ClickhouseBootstrapReq>
  readonly eventJournal?: EventJournalTableOptions
}

export const makeServiceInfrastructureLayers = <
  EventLogOut,
  EventLogErr,
  EventLogReq,
  ProjectionOut,
  ProjectionErr,
  ProjectionReq,
  ProjectionStoreOut,
  ProjectionStoreErr,
  ProjectionStoreReq,
  OutboxOut,
  OutboxErr,
  OutboxReq,
  OutboxWorkerOut,
  OutboxWorkerErr,
  OutboxWorkerReq,
  SnapshotsOut,
  SnapshotsErr,
  SnapshotsReq,
  PublishHandlersOut,
  PublishHandlersErr,
  PublishHandlersReq,
  PublisherOut,
  PublisherErr,
  PublisherReq,
  ClickhouseOut = never,
  ClickhouseErr = never,
  ClickhouseReq = never,
  ClickhouseBootstrapOut = never,
  ClickhouseBootstrapErr = never,
  ClickhouseBootstrapReq = never
>(
  config: ServiceInfrastructureConfig<
    EventLogOut,
    EventLogErr,
    EventLogReq,
    ProjectionOut,
    ProjectionErr,
    ProjectionReq,
    ProjectionStoreOut,
    ProjectionStoreErr,
    ProjectionStoreReq,
    OutboxOut,
    OutboxErr,
    OutboxReq,
    OutboxWorkerOut,
    OutboxWorkerErr,
    OutboxWorkerReq,
    SnapshotsOut,
    SnapshotsErr,
    SnapshotsReq,
    PublishHandlersOut,
    PublishHandlersErr,
    PublishHandlersReq,
    PublisherOut,
    PublisherErr,
    PublisherReq,
    ClickhouseOut,
    ClickhouseErr,
    ClickhouseReq,
    ClickhouseBootstrapOut,
    ClickhouseBootstrapErr,
    ClickhouseBootstrapReq
  >
) => {
  const identityLayer = Layer.succeed(Identity, Identity.makeRandom())
  const sqlJournalLayer = makeSqlEventJournalLayer(config.eventJournal)
  const sqlClientLayer = Layer.service(SqlClient)
  const clickhouseLayer = config.clickhouseLayer ?? Layer.empty
  const clickhouseBootstrapLayer = config.clickhouseBootstrapLayer ?? Layer.empty
  const clickhouseReadyLayer = Layer.merge(
    clickhouseLayer,
    Layer.provide(clickhouseBootstrapLayer, clickhouseLayer)
  )
  const projectionStoreRequirements = Layer.merge(sqlClientLayer, clickhouseReadyLayer)
  const projectionStoreLayer = Layer.provide(config.projectionStoreLayer, projectionStoreRequirements)

  const WorkflowLayer = Layer.provideMerge(
    config.publishHandlers,
    Layer.merge(WorkflowEngine.layerMemory, config.publisherLive)
  )

  const ClusterWorkflowLayer = Layer.provideMerge(
    config.publishHandlers,
    Layer.merge(ClusterWorkflowEngine.layer, config.publisherLive)
  )

  const projectionLayer = Layer.provide(
    config.projectionLayer,
    Layer.merge(projectionStoreLayer, config.outboxLive)
  )

  const OutboxWorkerLayer = Layer.provide(
    config.outboxWorkerLive,
    Layer.merge(config.outboxLive, WorkflowLayer)
  )

  const ClusterOutboxWorkerLayer = Layer.provide(
    config.outboxWorkerLive,
    Layer.merge(config.outboxLive, ClusterWorkflowLayer)
  )

  const EventLogLayer = config.eventLogLayer.pipe(
    Layer.provide(projectionLayer),
    Layer.provide(Layer.merge(sqlJournalLayer, identityLayer))
  )

  return {
    identityLayer,
    sqlJournalLayer,
    clickhouseReadyLayer,
    projectionStoreLayer,
    WorkflowLayer,
    ClusterWorkflowLayer,
    EventLogLayer,
    InfrastructureLayer: Layer.mergeAll(
      sqlJournalLayer,
      identityLayer,
      WorkflowLayer,
      config.outboxLive,
      projectionStoreLayer,
      OutboxWorkerLayer,
      EventLogLayer,
      config.snapshotsLive
    ),
    ClusterInfrastructureLayer: Layer.mergeAll(
      sqlJournalLayer,
      identityLayer,
      ClusterWorkflowLayer,
      config.outboxLive,
      projectionStoreLayer,
      ClusterOutboxWorkerLayer,
      EventLogLayer,
      config.snapshotsLive
    )
  } as const
}
