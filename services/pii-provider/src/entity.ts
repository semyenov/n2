import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { EntityProxy, EntityProxyServer } from "@effect/cluster"
import {
  makeToError,
  makeWriteThroughAfterCommitPublisher
} from "@semyenov/n2/helpers"
import {
  PIIProvider,
  decryptSensitiveData,
  toCommandResult,
  toPIIRecordDocument
} from "./aggregate.js"
import { InfrastructureLayer } from "./layers.js"
import { PIIProviderEventGroup } from "./events.js"
import type { PIIProviderEvent } from "./contracts/events.js"
import {
  type PIIProviderCommand,
  PIIProviderEntity,
  PIIProviderRpcs,
  storageKeyOfCommand
} from "./contracts/commands.js"
import { PIIAuditLog, storageKeyFromEntityReference } from "./contracts/common.js"
import { PIIError, PIINotFound } from "./contracts/errors.js"
import { PIIHistory, type PIIState } from "./contracts/state.js"
import { PIIProviderOutbox } from "./outbox.js"
import { PIIProviderProjectionStore } from "./projection-store.js"
import { PIIProviderSnapshotOps } from "./snapshots.js"
import { makePIIProviderEventMessage } from "./workflows.js"

const toHistory = (state: PIIState) =>
  new PIIHistory({
    storageKey: state.storageKey,
    recordId: state.recordId,
    currentRevision: state.revision,
    status: state.status,
    requests: state.dataSubjectRequests,
    auditEntries: state.auditEntries,
    revisions: state.revisions
  })

const ensureReadableState = (storageKey: string, state: PIIState) =>
  state.status === "empty" || state.status === "DELETED"
    ? Effect.fail(new PIINotFound({ storageKey }))
    : Effect.succeed(state)

const getRecord = (storageKey: string, ctx: {
  readonly getState: (entityId: string) => Effect.Effect<PIIState, never, unknown>
}) =>
  ctx.getState(storageKey).pipe(
    Effect.flatMap((state) => ensureReadableState(storageKey, state)),
    Effect.flatMap((state) =>
      decryptSensitiveData(state).pipe(
        Effect.map((sensitiveData) => toPIIRecordDocument(state, sensitiveData)),
        Effect.mapError(() => new PIINotFound({ storageKey }))
      )
    )
  )

const overrides = {
  GetPIIRecord: (command: Extract<PIIProviderCommand, { readonly _tag: "GetPIIRecord" }>, ctx: {
    readonly getState: (entityId: string) => Effect.Effect<PIIState, never, unknown>
  }) => getRecord(command.storageKey, ctx),

  GetPIIRecordByEntity: (command: Extract<PIIProviderCommand, { readonly _tag: "GetPIIRecordByEntity" }>, ctx: {
    readonly getState: (entityId: string) => Effect.Effect<PIIState, never, unknown>
  }) => getRecord(storageKeyFromEntityReference(command.entityReference), ctx),

  GetPIIHistory: (command: Extract<PIIProviderCommand, { readonly _tag: "GetPIIHistory" }>, ctx: {
    readonly getState: (entityId: string) => Effect.Effect<PIIState, never, unknown>
  }) =>
    ctx.getState(command.storageKey).pipe(
      Effect.flatMap((state) =>
        state.status === "empty"
          ? Effect.fail(new PIINotFound({ storageKey: command.storageKey }))
          : Effect.succeed(toHistory(state))
      )
    ),

  GetPIIAuditLog: (command: Extract<PIIProviderCommand, { readonly _tag: "GetPIIAuditLog" }>, ctx: {
    readonly getState: (entityId: string) => Effect.Effect<PIIState, never, unknown>
  }) =>
    ctx.getState(command.storageKey).pipe(
      Effect.flatMap((state) =>
        state.status === "empty"
          ? Effect.fail(new PIINotFound({ storageKey: command.storageKey }))
          : Effect.succeed(new PIIAuditLog({
              storageKey: command.storageKey,
              auditEntries: state.auditEntries,
              total: state.auditEntries.length
            }))
      )
    )
}

const toError = makeToError(PIIError)

const afterCommit = makeWriteThroughAfterCommitPublisher({
  group: PIIProviderEventGroup,
  storeTag: PIIProviderProjectionStore,
  outboxTag: PIIProviderOutbox,
  makeMessage: (event: PIIProviderEvent) =>
    makePIIProviderEventMessage({
      recordId: event.recordId,
      revision: event.revision,
      eventType: event._tag,
      occurredAt: String(event.occurredAt.toJSON()),
      payload: event
    }),
  logPrefix: "[pii-provider]"
})

export const PIIProviderEntityLayer = PIIProvider.toEntityLayer(
  PIIProviderEntity,
  {
    toResult: ({ entityId, state }) => toCommandResult(entityId, state),
    toError,
    snapshots: PIIProviderSnapshotOps,
    overrides
  },
  { maxIdleTime: "10 minutes", concurrency: "unbounded" }
)

export const PIIProviderProxyRpcs = EntityProxy.toRpcGroup(PIIProviderEntity)
export const PIIProviderProxyHandlers = EntityProxyServer.layerRpcHandlers(PIIProviderEntity)

export const PIIProviderHandlersRaw = PIIProvider.toStatefulRpcHandlers(
  PIIProviderRpcs,
  {
    entityId: (command) => storageKeyOfCommand(command),
    toResult: ({ entityId, state }) => toCommandResult(entityId, state),
    toError,
    snapshots: PIIProviderSnapshotOps,
    afterCommit,
    metrics: { prefix: "pii_provider" },
    overrides
  }
)

export const PIIProviderHandlers = Layer.provide(PIIProviderHandlersRaw, InfrastructureLayer)
