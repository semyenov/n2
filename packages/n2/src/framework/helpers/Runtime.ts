import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { BunClusterHttp } from "@effect/platform-bun"
import { ClusterWorkflowEngine } from "@effect/cluster"
import { Identity } from "@effect/experimental/EventLog"
import { HttpLayerRouter, HttpServerResponse } from "@effect/platform"
import { RpcSerialization, RpcServer } from "@effect/rpc"
import type * as Rpc from "@effect/rpc/Rpc"
import type * as RpcGroup from "@effect/rpc/RpcGroup"
import { Migrator } from "@effect/sql"
import * as SqlEventJournal from "@effect/sql/SqlEventJournal"
import { PgClient } from "@effect/sql-pg"
import { WorkflowEngine } from "@effect/workflow"
import * as ClickhouseClient from "@effect/sql-clickhouse/ClickhouseClient"

export type MigrationGlob = Record<string, () => Promise<unknown>>

export const makeMigrationsLayer = (migrations: MigrationGlob) => {
  const runMigrations = Migrator.make({})
  return Layer.effectDiscard(
    runMigrations({
      loader: Migrator.fromGlob(migrations)
    })
  )
}

export const makeConfiguredClickhouseLayer = () =>
  Layer.unwrapEffect(
    Effect.gen(function* () {
      const url = yield* Config.string("CLICKHOUSE_URL")
      const database = yield* Config.string("CLICKHOUSE_DATABASE").pipe(Config.withDefault("default"))

      return ClickhouseClient.layer({
        url,
        database
      })
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
  ClickhouseOut,
  ClickhouseErr,
  ClickhouseReq,
  ClickhouseBootstrapOut,
  ClickhouseBootstrapErr,
  ClickhouseBootstrapReq,
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
  PublisherReq
> {
  readonly eventLogLayer: Layer.Layer<EventLogOut, EventLogErr, EventLogReq>
  readonly projectionLayer: Layer.Layer<ProjectionOut, ProjectionErr, ProjectionReq>
  readonly projectionStoreLayer: Layer.Layer<ProjectionStoreOut, ProjectionStoreErr, ProjectionStoreReq>
  readonly clickhouseLayer: Layer.Layer<ClickhouseOut, ClickhouseErr, ClickhouseReq>
  readonly clickhouseBootstrapLayer: Layer.Layer<ClickhouseBootstrapOut, ClickhouseBootstrapErr, ClickhouseBootstrapReq>
  readonly outboxLive: Layer.Layer<OutboxOut, OutboxErr, OutboxReq>
  readonly outboxWorkerLive: Layer.Layer<OutboxWorkerOut, OutboxWorkerErr, OutboxWorkerReq>
  readonly snapshotsLive: Layer.Layer<SnapshotsOut, SnapshotsErr, SnapshotsReq>
  readonly publishHandlers: Layer.Layer<PublishHandlersOut, PublishHandlersErr, PublishHandlersReq>
  readonly publisherLive: Layer.Layer<PublisherOut, PublisherErr, PublisherReq>
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
  ClickhouseOut,
  ClickhouseErr,
  ClickhouseReq,
  ClickhouseBootstrapOut,
  ClickhouseBootstrapErr,
  ClickhouseBootstrapReq,
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
  PublisherReq
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
    ClickhouseOut,
    ClickhouseErr,
    ClickhouseReq,
    ClickhouseBootstrapOut,
    ClickhouseBootstrapErr,
    ClickhouseBootstrapReq,
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
    PublisherReq
  >
) => {
  const identityLayer = Layer.succeed(Identity, Identity.makeRandom())
  const sqlJournalLayer = SqlEventJournal.layer()
  const clickhouseReadyLayer = Layer.merge(
    config.clickhouseLayer,
    Layer.provide(config.clickhouseBootstrapLayer, config.clickhouseLayer)
  )
  const projectionStoreLayer = Layer.provide(config.projectionStoreLayer, clickhouseReadyLayer)

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
      Layer.provide(config.outboxWorkerLive, config.outboxLive),
      EventLogLayer,
      config.snapshotsLive
    ),
    ClusterInfrastructureLayer: Layer.mergeAll(
      sqlJournalLayer,
      identityLayer,
      ClusterWorkflowLayer,
      config.outboxLive,
      Layer.provide(config.outboxWorkerLive, config.outboxLive),
      EventLogLayer,
      config.snapshotsLive
    )
  } as const
}
