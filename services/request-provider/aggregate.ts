import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as N2 from "@semyenov/n2/helpers"
import {
  type MetadataScope,
  type RequestProviderCommand,
  type RequestProviderEvent,
  type RequestState,
  initialRequestState,
  CommandResult,
  RequestCreated,
  RequestError,
  RequestMetaDataCreated,
  RequestProviderCommands,
  RequestRevisionEntry,
  RequestSnapshot,
  RequestSnapshotCreated,
  RequestUpdated
} from "./contracts.js"

export { initialRequestState }

const nextRevision = (state: RequestState, offset: number) => state.revision + offset + 1

const snapshotExists = (state: RequestState, snapshotId: string) =>
  state.snapshots.some((snapshot) => snapshot.snapshotId === snapshotId)

const ensureRequestIdMatches = (requestId: string, candidateRequestId: string) =>
  candidateRequestId === requestId
    ? Effect.void
    : Effect.fail(new RequestError({ message: `requestJson.uuid must match requestId "${requestId}"` }))

const appendRevision = (
  state: RequestState,
  revision: number,
  eventType: string,
  summary: string,
  occurredAt: DateTime.Utc,
  actorId: string
) =>
  [
    ...state.revisions,
    new RequestRevisionEntry({
      revision,
      eventType,
      summary,
      occurredAt,
      actorId
    })
  ]

const metadataEvent = (
  state: RequestState,
  requestId: string,
  scope: MetadataScope,
  scopeId: string,
  metadataJson: string,
  schemaVersion: string,
  occurredAt: DateTime.Utc,
  actorId: string,
  offset: number
) =>
  new RequestMetaDataCreated({
    requestId,
    scope,
    scopeId,
    metadataJson,
    schemaVersion,
    occurredAt,
    actorId,
    revision: nextRevision(state, offset)
  })

export const RequestProvider = N2.define<RequestProviderEvent, RequestProviderCommand>()({
  initialState: initialRequestState,
  commands: RequestProviderCommands.constructors,
  evolve: {
    RequestCreated: (state, event) => ({
      ...state,
      status: "draft" as const,
      requestId: event.requestId,
      clientId: event.clientId,
      currentSchemaVersion: event.schemaVersion,
      requestJson: event.requestJson,
      revisions: appendRevision(
        state,
        event.revision,
        event._tag,
        event.summary,
        event.occurredAt,
        event.actorId
      ),
      revision: event.revision
    }),
    RequestUpdated: (state, event) => ({
      ...state,
      status: "draft" as const,
      currentSchemaVersion: event.schemaVersion,
      requestJson: event.requestJson,
      revisions: appendRevision(
        state,
        event.revision,
        event._tag,
        event.summary,
        event.occurredAt,
        event.actorId
      ),
      revision: event.revision
    }),
    RequestMetaDataCreated: (state, event) => ({
      ...state,
      latestMetadataJson: event.metadataJson,
      currentSchemaVersion: event.schemaVersion,
      snapshots: state.snapshots.map((snapshot) =>
        event.scope === "snapshot" && snapshot.snapshotId === event.scopeId
          ? new RequestSnapshot({ ...snapshot, metadataJson: event.metadataJson, schemaVersion: event.schemaVersion })
          : snapshot
      ),
      revisions: appendRevision(
        state,
        event.revision,
        event._tag,
        `${event.scope}:${event.scopeId}`,
        event.occurredAt,
        event.actorId
      ),
      revision: event.revision
    }),
    RequestSnapshotCreated: (state, event) => ({
      ...state,
      status: "snapshotted" as const,
      currentSchemaVersion: event.schemaVersion,
      requestJson: event.requestJson,
      latestSnapshotId: event.snapshotId,
      snapshots: [
        ...state.snapshots,
        new RequestSnapshot({
          snapshotId: event.snapshotId,
          revision: event.revision,
          snapshotType: event.snapshotType,
          requestJson: event.requestJson,
          metadataJson: event.metadataJson,
          schemaVersion: event.schemaVersion,
          summary: event.summary,
          createdAt: event.occurredAt,
          createdBy: event.actorId
        })
      ],
      revisions: appendRevision(
        state,
        event.revision,
        event._tag,
        event.summary,
        event.occurredAt,
        event.actorId
      ),
      revision: event.revision
    })
  },
  decide: {
    CreateRequest: (state, command) =>
      Effect.gen(function* () {
        if (state.status !== "empty") {
          return yield* Effect.fail(new RequestError({ message: "Request already exists" }))
        }
        yield* ensureRequestIdMatches(command.requestId, command.requestJson.uuid)
        const now = yield* DateTime.now
        return [
          new RequestCreated({
            requestId: command.requestId,
            clientId: command.clientId,
            schemaVersion: command.schemaVersion,
            requestJson: command.requestJson,
            occurredAt: now,
            actorId: command.actorId,
            summary: command.summary,
            sourceCount: command.sources.length,
            revision: nextRevision(state, 0)
          }),
          metadataEvent(
            state,
            command.requestId,
            "aggregate",
            command.requestId,
            command.metadataJson,
            command.schemaVersion,
            now,
            command.actorId,
            1
          )
        ]
      }),

    UpdateRequest: (state, command) =>
      Effect.gen(function* () {
        if (state.status === "empty") {
          return yield* Effect.fail(new RequestError({ message: "Request does not exist" }))
        }
        yield* ensureRequestIdMatches(command.requestId, command.requestJson.uuid)
        const now = yield* DateTime.now
        return [
          new RequestUpdated({
            requestId: command.requestId,
            schemaVersion: command.schemaVersion,
            requestJson: command.requestJson,
            occurredAt: now,
            actorId: command.actorId,
            summary: command.summary,
            sourceCount: command.sources.length,
            revision: nextRevision(state, 0)
          }),
          metadataEvent(
            state,
            command.requestId,
            "aggregate",
            command.requestId,
            command.metadataJson,
            command.schemaVersion,
            now,
            command.actorId,
            1
          )
        ]
      }),

    CreateRequestSnapshot: (state, command) =>
      Effect.gen(function* () {
        if (state.status === "empty") {
          return yield* Effect.fail(new RequestError({ message: "Request does not exist" }))
        }
        if (snapshotExists(state, command.snapshotId)) {
          return yield* Effect.fail(new RequestError({ message: `Snapshot "${command.snapshotId}" already exists` }))
        }
        yield* ensureRequestIdMatches(command.requestId, command.requestJson.uuid)
        const now = yield* DateTime.now
        return [
          new RequestSnapshotCreated({
            requestId: command.requestId,
            snapshotId: command.snapshotId,
            snapshotType: command.snapshotType,
            schemaVersion: command.schemaVersion,
            requestJson: command.requestJson,
            metadataJson: command.metadataJson,
            occurredAt: now,
            actorId: command.actorId,
            summary: command.summary,
            revision: nextRevision(state, 0)
          }),
          metadataEvent(
            state,
            command.requestId,
            "snapshot",
            command.snapshotId,
            command.metadataJson,
            command.schemaVersion,
            now,
            command.actorId,
            1
          )
        ]
      }),

    GetRequest: () => Effect.succeed([]),
    GetRequestHistory: () => Effect.succeed([])
  }
})

export const handle = RequestProvider.handle
export const run = RequestProvider.run

export const toCommandResult = (requestId: string, state: RequestState) =>
  new CommandResult({
    requestId,
    revision: state.revision,
    latestSnapshotId: state.latestSnapshotId
  })
