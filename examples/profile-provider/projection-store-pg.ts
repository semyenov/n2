import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { SqlClient, type SqlClient as SqlClientInstance } from "@effect/sql/SqlClient"
import { type ProfileEvent, ProfileDocument } from "./contracts.js"
import { ProfileProviderProjectionStore } from "./projection-store.js"
import type { ProfileProviderEventMessage } from "./workflows.js"

const encodeProfile = Schema.encodeSync(ProfileDocument)

const insertOutbox = (sql: SqlClientInstance, message: ProfileProviderEventMessage) =>
  {
    const now = new Date().toISOString()
    return sql`
      INSERT INTO profile_provider_event_outbox
      (id, profile_id, revision, topic, partition_key, occurred_at, payload_json, headers_json, status, retry_count, last_error, created_at, updated_at, published_at, next_attempt_at)
      VALUES (
        ${message.id},
        ${message.profileId},
        ${message.revision},
        ${message.topic},
        ${message.partitionKey},
        ${message.occurredAt},
        ${JSON.stringify(message.payload)},
        ${JSON.stringify(message.headers)},
        ${"pending"},
        ${0},
        ${""},
        ${now},
        ${now},
        ${""},
        ${""}
      )
      ON CONFLICT (id) DO NOTHING
    `.pipe(Effect.asVoid)
  }

const projectEvent = (sql: SqlClientInstance, event: ProfileEvent) => {
  switch (event._tag) {
    case "ProfileCreated":
      return sql`
        INSERT INTO profile_provider_profiles_read
          (profile_id, owner_agent_id, active_branch_id, status, current_revision, current_schema_version,
           masked_profile_json, latest_metadata_json, latest_pii_storage_key, pii_jurisdiction,
           published_snapshot_id, created_at, updated_at)
        VALUES (
          ${event.profileId},
          ${event.ownerAgentId},
          ${event.branchId},
          ${"draft"},
          ${event.revision},
          ${event.schemaVersion},
          ${JSON.stringify(encodeProfile(event.maskedProfileJson))},
          ${"{}"},
          ${""},
          ${""},
          ${""},
          ${event.createdAt.toJSON()},
          ${event.createdAt.toJSON()}
        )
        ON CONFLICT (profile_id) DO UPDATE SET
          owner_agent_id = EXCLUDED.owner_agent_id,
          active_branch_id = EXCLUDED.active_branch_id,
          status = EXCLUDED.status,
          current_revision = EXCLUDED.current_revision,
          current_schema_version = EXCLUDED.current_schema_version,
          masked_profile_json = EXCLUDED.masked_profile_json,
          updated_at = EXCLUDED.updated_at
      `.pipe(Effect.asVoid)
    case "MergedDataProfile":
      return sql`
        UPDATE profile_provider_profiles_read
        SET active_branch_id = ${event.branchId},
            status = ${"draft"},
            current_revision = ${event.revision},
            current_schema_version = ${event.schemaVersion},
            masked_profile_json = ${JSON.stringify(encodeProfile(event.maskedProfileJson))},
            updated_at = ${event.mergedAt.toJSON()}
        WHERE profile_id = ${event.profileId}
      `.pipe(Effect.asVoid)
    case "SnapshotCreatedProfile":
      return sql`
        INSERT INTO profile_provider_snapshots_read
          (snapshot_id, profile_id, branch_id, revision, snapshot_type, profile_json, metadata_json,
           schema_version, summary, published, strategy_json, created_at, created_by)
        VALUES (
          ${event.snapshotId},
          ${event.profileId},
          ${event.branchId},
          ${event.revision},
          ${event.snapshotType},
          ${JSON.stringify(encodeProfile(event.profileJson))},
          ${event.metadataJson},
          ${event.schemaVersion},
          ${event.summary},
          ${false},
          ${""},
          ${event.createdAt.toJSON()},
          ${event.createdBy}
        )
        ON CONFLICT (snapshot_id) DO UPDATE SET
          revision = EXCLUDED.revision,
          profile_json = EXCLUDED.profile_json,
          metadata_json = EXCLUDED.metadata_json,
          schema_version = EXCLUDED.schema_version,
          summary = EXCLUDED.summary
      `.pipe(Effect.asVoid)
    case "MetaDataCreated":
      if (event.scope === "snapshot") {
        return sql`
          UPDATE profile_provider_snapshots_read
          SET metadata_json = ${event.metadataJson},
              schema_version = ${event.schemaVersion}
          WHERE snapshot_id = ${event.scopeId}
        `.pipe(Effect.asVoid)
      }
      return sql`
        UPDATE profile_provider_profiles_read
        SET latest_metadata_json = ${event.metadataJson},
            current_schema_version = ${event.schemaVersion},
            updated_at = ${event.createdAt.toJSON()}
        WHERE profile_id = ${event.profileId}
      `.pipe(Effect.asVoid)
    case "PersonalDataExtracted":
      return sql`
        UPDATE profile_provider_profiles_read
        SET latest_pii_storage_key = ${event.piiStorageKey},
            pii_jurisdiction = ${event.jurisdiction},
            updated_at = ${event.extractedAt.toJSON()}
        WHERE profile_id = ${event.profileId}
      `.pipe(Effect.asVoid)
    case "ProfileBranchForked":
      return sql`
        UPDATE profile_provider_profiles_read
        SET active_branch_id = ${event.branchId},
            current_revision = ${event.revision},
            updated_at = ${event.createdAt.toJSON()}
        WHERE profile_id = ${event.profileId}
      `.pipe(Effect.asVoid)
    case "SnapshotPublishedProfile":
      return Effect.gen(function* () {
        yield* sql`
          UPDATE profile_provider_profiles_read
          SET status = ${"published"},
              published_snapshot_id = ${event.snapshotId},
              current_revision = ${event.revision},
              updated_at = ${event.publishedAt.toJSON()}
          WHERE profile_id = ${event.profileId}
        `.pipe(Effect.asVoid)
        yield* sql`
          UPDATE profile_provider_snapshots_read
          SET published = ${true},
              strategy_json = ${event.strategyJson}
          WHERE snapshot_id = ${event.snapshotId}
        `.pipe(Effect.asVoid)
      })
  }
}

export const ProfileProviderProjectionStorePgLive = Layer.effect(
  ProfileProviderProjectionStore,
  Effect.gen(function* () {
    const sql = yield* SqlClient

    return {
      project: (event: ProfileEvent, message: ProfileProviderEventMessage) =>
        sql.withTransaction(Effect.gen(function* () {
          yield* projectEvent(sql, event)
          yield* insertOutbox(sql, message)
        }))
    }
  })
)
