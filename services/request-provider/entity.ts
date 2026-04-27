import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { EntityProxy, EntityProxyServer } from "@effect/cluster"
import * as EventLogApi from "@effect/experimental/EventLog"
import * as Schedule from "effect/Schedule"
import { RequestProvider, toCommandResult } from "./aggregate.js"
import { InfrastructureLayer } from "./layers.js"
import { RequestProviderEventLogSchema } from "./events.js"
import { RequestProviderSnapshotOps } from "./snapshots.js"
import {
  type RequestProviderCommand,
  type RequestProviderEvent,
  type RequestState,
  RequestError,
  RequestHistory,
  RequestNotFound,
  RequestProviderEntity,
  RequestProviderRpcs,
  SourceAsset
} from "./contracts.js"

const mergeSourceAssets = (current: ReadonlyArray<SourceAsset>, incoming: ReadonlyArray<SourceAsset>) => {
  const next = new Map(current.map((asset) => [asset.sourceId, asset]))
  for (const asset of incoming) {
    next.set(asset.sourceId, asset)
  }
  return Array.from(next.values())
}

const toHistory = (state: RequestState) =>
  new RequestHistory({
    requestId: state.requestId,
    currentRevision: state.revision,
    latestSnapshotId: state.latestSnapshotId,
    revisions: state.revisions,
    snapshots: state.snapshots
  })

const postHandle = ({ command, state }: {
  command: RequestProviderCommand
  events: ReadonlyArray<RequestProviderEvent>
  state: RequestState
  entityId: string
}) => {
  if (command._tag === "CreateRequest" || command._tag === "UpdateRequest") {
    return { ...state, sourceAssets: mergeSourceAssets(state.sourceAssets, command.sources) }
  }
  return state
}

const toError = (error: unknown) =>
  error instanceof RequestError ? error : new RequestError({ message: String(error) })

const publishRetry = Schedule.exponential("100 millis").pipe(
  Schedule.jittered,
  Schedule.intersect(Schedule.recurs(3))
)

const overrides = {
  GetRequest: (command: Extract<RequestProviderCommand, { readonly _tag: "GetRequest" }>, ctx: {
    readonly getState: (entityId: string) => Effect.Effect<RequestState, unknown, unknown>
  }) =>
    ctx.getState(command.requestId).pipe(
      Effect.flatMap((state) =>
        state.status === "empty"
          ? Effect.fail(new RequestNotFound({ requestId: command.requestId }))
          : Effect.succeed(state)
      )
    ),
  GetRequestHistory: (command: Extract<RequestProviderCommand, { readonly _tag: "GetRequestHistory" }>, ctx: {
    readonly getState: (entityId: string) => Effect.Effect<RequestState, unknown, unknown>
  }) =>
    ctx.getState(command.requestId).pipe(
      Effect.flatMap((state) =>
        state.status === "empty"
          ? Effect.fail(new RequestNotFound({ requestId: command.requestId }))
          : Effect.succeed(toHistory(state))
      )
    )
}

export const RequestProviderEntityLayer = RequestProvider.toEntityLayer(
  RequestProviderEntity,
  {
    toResult: ({ entityId, state }) => toCommandResult(entityId, state),
    toError,
    snapshots: RequestProviderSnapshotOps,
    postHandle,
    overrides
  },
  { maxIdleTime: "10 minutes", concurrency: "unbounded" }
)

export const RequestProviderProxyRpcs = EntityProxy.toRpcGroup(RequestProviderEntity)
export const RequestProviderProxyHandlers = EntityProxyServer.layerRpcHandlers(RequestProviderEntity)

export const RequestProviderHandlersRaw = RequestProvider.toStatefulRpcHandlers(
  RequestProviderRpcs,
  {
    entityId: (command) => command.requestId,
    toResult: ({ entityId, state }) => toCommandResult(entityId, state),
    toError,
    snapshots: RequestProviderSnapshotOps,
    postHandle,
    afterCommit: ({ events }) =>
      Effect.gen(function* () {
        const publish = yield* EventLogApi.makeClient(RequestProviderEventLogSchema)
        yield* Effect.forEach(
          events,
          (event) => publish(event._tag, event),
          { discard: true }
        )
      }).pipe(
        Effect.retry(publishRetry),
        Effect.tapError((error) => Effect.logError(`[request-provider] event publish failed: ${String(error)}`))
      ),
    metrics: { prefix: "request_provider" },
    overrides
  }
)

export const RequestProviderHandlers = Layer.provide(RequestProviderHandlersRaw, InfrastructureLayer)
