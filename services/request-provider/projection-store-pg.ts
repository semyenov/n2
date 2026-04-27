import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { SqlClient } from "@effect/sql/SqlClient"
import { RequestDocument } from "./contracts.js"
import { makeDispatch, RequestProviderProjectionStore, type ProjectionHandlers } from "./projection-store.js"

const encodeRequest = Schema.encodeSync(RequestDocument)

export const RequestProviderProjectionStorePgLive = Layer.effect(
  RequestProviderProjectionStore,
  Effect.gen(function* () {
    const sql = yield* SqlClient

    const handlers: Omit<ProjectionHandlers, "dispatch"> = {
      onRequestCreated: (event) =>
        sql`
          INSERT INTO request_provider_requests_read
            (request_id, client_id, status, current_revision, current_schema_version,
             request_json, latest_metadata_json, latest_snapshot_id, created_at, updated_at)
          VALUES (
            ${event.requestId}, ${event.clientId}, ${"draft"},
            ${event.revision}, ${event.schemaVersion},
            ${JSON.stringify(encodeRequest(event.requestJson))},
            ${"{}"}, ${""},
            ${String(event.occurredAt.toJSON())}, ${String(event.occurredAt.toJSON())}
          )
          ON CONFLICT (request_id) DO UPDATE SET
            client_id = EXCLUDED.client_id,
            status = EXCLUDED.status,
            current_revision = EXCLUDED.current_revision,
            current_schema_version = EXCLUDED.current_schema_version,
            request_json = EXCLUDED.request_json,
            updated_at = EXCLUDED.updated_at
        `.pipe(Effect.asVoid),

      onRequestUpdated: (event) =>
        sql`
          UPDATE request_provider_requests_read
          SET status = ${"draft"},
              current_revision = ${event.revision},
              current_schema_version = ${event.schemaVersion},
              request_json = ${JSON.stringify(encodeRequest(event.requestJson))},
              updated_at = ${String(event.occurredAt.toJSON())}
          WHERE request_id = ${event.requestId}
        `.pipe(Effect.asVoid),

      onRequestMetaDataCreated: (event) => {
        if (event.scope === "snapshot") {
          return sql`
            UPDATE request_provider_snapshots_read
            SET metadata_json = ${event.metadataJson},
                schema_version = ${event.schemaVersion}
            WHERE snapshot_id = ${event.scopeId}
          `.pipe(Effect.asVoid)
        }
        return sql`
          UPDATE request_provider_requests_read
          SET latest_metadata_json = ${event.metadataJson},
              current_schema_version = ${event.schemaVersion},
              current_revision = ${event.revision},
              updated_at = ${String(event.occurredAt.toJSON())}
          WHERE request_id = ${event.requestId}
        `.pipe(Effect.asVoid)
      },

      onRequestSnapshotCreated: (event) =>
        Effect.gen(function* () {
          yield* sql`
            INSERT INTO request_provider_snapshots_read
              (snapshot_id, request_id, revision, snapshot_type, request_json, metadata_json,
               schema_version, summary, created_at, created_by)
            VALUES (
              ${event.snapshotId}, ${event.requestId}, ${event.revision}, ${event.snapshotType},
              ${JSON.stringify(encodeRequest(event.requestJson))},
              ${event.metadataJson}, ${event.schemaVersion}, ${event.summary},
              ${String(event.occurredAt.toJSON())}, ${event.actorId}
            )
            ON CONFLICT (snapshot_id) DO UPDATE SET
              revision = EXCLUDED.revision,
              request_json = EXCLUDED.request_json,
              metadata_json = EXCLUDED.metadata_json,
              schema_version = EXCLUDED.schema_version,
              summary = EXCLUDED.summary
          `.pipe(Effect.asVoid)
          yield* sql`
            UPDATE request_provider_requests_read
            SET status = ${"snapshotted"},
                latest_snapshot_id = ${event.snapshotId},
                current_revision = ${event.revision},
                current_schema_version = ${event.schemaVersion},
                request_json = ${JSON.stringify(encodeRequest(event.requestJson))},
                updated_at = ${String(event.occurredAt.toJSON())}
            WHERE request_id = ${event.requestId}
          `.pipe(Effect.asVoid)
        })
    }
    return { ...handlers, dispatch: makeDispatch(handlers) }
  })
)
