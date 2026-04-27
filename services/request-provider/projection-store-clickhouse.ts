import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as ClickhouseClient from "@effect/sql-clickhouse/ClickhouseClient"
import { RequestDocument } from "./contracts.js"
import type {
  RequestCreated,
  RequestMetaDataCreated,
  RequestSnapshotCreated,
  RequestUpdated
} from "./contracts.js"
import { makeDispatch, RequestProviderProjectionStore, type ProjectionHandlers } from "./projection-store.js"

type RequestCurrentRow = {
  readonly request_id: string
  readonly revision: number
  readonly client_id: string
  readonly status: string
  readonly current_schema_version: string
  readonly request_json: string
  readonly latest_metadata_json: string
  readonly latest_snapshot_id: string
  readonly created_at: string
  readonly updated_at: string
}

type SnapshotCurrentRow = {
  readonly snapshot_id: string
  readonly request_id: string
  readonly revision: number
  readonly snapshot_type: string
  readonly request_json: string
  readonly metadata_json: string
  readonly schema_version: string
  readonly summary: string
  readonly created_at: string
  readonly created_by: string
}

const encodeRequest = Schema.encodeSync(RequestDocument)

const initialRequestRow = (requestId: string): RequestCurrentRow => ({
  request_id: requestId,
  revision: 0,
  client_id: "",
  status: "empty",
  current_schema_version: "",
  request_json: "",
  latest_metadata_json: "{}",
  latest_snapshot_id: "",
  created_at: "",
  updated_at: ""
})

const loadLatestRequest = (ch: ClickhouseClient.ClickhouseClient, requestId: string) =>
  ch<RequestCurrentRow>`
    SELECT request_id, revision, client_id, status, current_schema_version,
           request_json, latest_metadata_json, latest_snapshot_id, created_at, updated_at
    FROM request_provider_requests_current
    WHERE request_id = ${requestId}
    ORDER BY revision DESC LIMIT 1
  `.pipe(Effect.map((rows) => rows[0]))

const loadLatestSnapshot = (ch: ClickhouseClient.ClickhouseClient, snapshotId: string) =>
  ch<SnapshotCurrentRow>`
    SELECT snapshot_id, request_id, revision, snapshot_type, request_json,
           metadata_json, schema_version, summary, created_at, created_by
    FROM request_provider_snapshots_current
    WHERE snapshot_id = ${snapshotId}
    ORDER BY revision DESC LIMIT 1
  `.pipe(Effect.map((rows) => rows[0]))

const insertEvent = (
  ch: ClickhouseClient.ClickhouseClient,
  requestId: string,
  revision: number,
  eventType: string,
  occurredAt: string,
  snapshotId: string,
  payload: unknown
) =>
  ch.insertQuery({
    table: "request_provider_projection_events",
    values: [{
      request_id: requestId,
      revision,
      event_type: eventType,
      occurred_at: occurredAt,
      snapshot_id: snapshotId,
      payload_json: JSON.stringify(payload)
    }]
  }).pipe(Effect.asVoid)

export const RequestProviderProjectionStoreClickhouseLive = Layer.effect(
  RequestProviderProjectionStore,
  Effect.gen(function* () {
    const ch = yield* ClickhouseClient.ClickhouseClient

    const handlers: Omit<ProjectionHandlers, "dispatch"> = {
      onRequestCreated: (event: RequestCreated) =>
        Effect.gen(function* () {
          yield* insertEvent(ch, event.requestId, event.revision, "RequestCreated", String(event.occurredAt.toJSON()), "", event)
          const current = (yield* loadLatestRequest(ch, event.requestId)) ?? initialRequestRow(event.requestId)
          yield* ch.insertQuery({
            table: "request_provider_requests_current",
            values: [{
              ...current,
              revision: event.revision,
              client_id: event.clientId,
              status: "draft",
              current_schema_version: event.schemaVersion,
              request_json: JSON.stringify(encodeRequest(event.requestJson)),
              created_at: String(event.occurredAt.toJSON()),
              updated_at: String(event.occurredAt.toJSON())
            }]
          }).pipe(Effect.asVoid)
        }),

      onRequestUpdated: (event: RequestUpdated) =>
        Effect.gen(function* () {
          yield* insertEvent(ch, event.requestId, event.revision, "RequestUpdated", String(event.occurredAt.toJSON()), "", event)
          const current = (yield* loadLatestRequest(ch, event.requestId)) ?? initialRequestRow(event.requestId)
          yield* ch.insertQuery({
            table: "request_provider_requests_current",
            values: [{
              ...current,
              revision: event.revision,
              status: "draft",
              current_schema_version: event.schemaVersion,
              request_json: JSON.stringify(encodeRequest(event.requestJson)),
              updated_at: String(event.occurredAt.toJSON())
            }]
          }).pipe(Effect.asVoid)
        }),

      onRequestMetaDataCreated: (event: RequestMetaDataCreated) =>
        Effect.gen(function* () {
          const snapshotId = event.scope === "snapshot" ? event.scopeId : ""
          yield* insertEvent(ch, event.requestId, event.revision, "RequestMetaDataCreated", String(event.occurredAt.toJSON()), snapshotId, event)
          if (event.scope === "snapshot") {
            const current = yield* loadLatestSnapshot(ch, event.scopeId)
            if (current) {
              yield* ch.insertQuery({
                table: "request_provider_snapshots_current",
                values: [{ ...current, revision: event.revision, metadata_json: event.metadataJson, schema_version: event.schemaVersion }]
              }).pipe(Effect.asVoid)
            }
          } else {
            const current = (yield* loadLatestRequest(ch, event.requestId)) ?? initialRequestRow(event.requestId)
            yield* ch.insertQuery({
              table: "request_provider_requests_current",
              values: [{
                ...current,
                revision: event.revision,
                current_schema_version: event.schemaVersion,
                latest_metadata_json: event.metadataJson,
                updated_at: String(event.occurredAt.toJSON())
              }]
            }).pipe(Effect.asVoid)
          }
        }),

      onRequestSnapshotCreated: (event: RequestSnapshotCreated) =>
        Effect.gen(function* () {
          yield* insertEvent(ch, event.requestId, event.revision, "RequestSnapshotCreated", String(event.occurredAt.toJSON()), event.snapshotId, event)
          yield* ch.insertQuery({
            table: "request_provider_snapshots_current",
            values: [{
              snapshot_id: event.snapshotId,
              request_id: event.requestId,
              revision: event.revision,
              snapshot_type: event.snapshotType,
              request_json: JSON.stringify(encodeRequest(event.requestJson)),
              metadata_json: event.metadataJson,
              schema_version: event.schemaVersion,
              summary: event.summary,
              created_at: String(event.occurredAt.toJSON()),
              created_by: event.actorId
            }]
          }).pipe(Effect.asVoid)
          const current = (yield* loadLatestRequest(ch, event.requestId)) ?? initialRequestRow(event.requestId)
          yield* ch.insertQuery({
            table: "request_provider_requests_current",
            values: [{
              ...current,
              revision: event.revision,
              status: "snapshotted",
              current_schema_version: event.schemaVersion,
              request_json: JSON.stringify(encodeRequest(event.requestJson)),
              latest_snapshot_id: event.snapshotId,
              updated_at: String(event.occurredAt.toJSON())
            }]
          }).pipe(Effect.asVoid)
        })
    }
    return { ...handlers, dispatch: makeDispatch(handlers) }
  })
)
