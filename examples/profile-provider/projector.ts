import * as Effect from "effect/Effect"
import { EventLog } from "@effect/experimental"
import { SqlClient, type SqlClient as SqlClientService } from "@effect/sql/SqlClient"
import { ProfileProviderEventGroup } from "./events.js"

const enqueueOutbox = (
  sql: SqlClientService,
  profileId: string,
  revision: number,
  eventType: string,
  payload: unknown
) =>
  sql`
    INSERT INTO profile_provider_event_outbox
      (id, profile_id, revision, topic, partition_key, payload_json, headers_json, status, retry_count, last_error)
    VALUES (
      ${`${profileId}:${revision}:${eventType}`},
      ${profileId},
      ${revision},
      ${"profile-provider.events"},
      ${profileId},
      ${JSON.stringify(payload)},
      ${JSON.stringify({ eventType, revision })},
      ${"PENDING"},
      0,
      ${""}
    )
    ON CONFLICT (id) DO NOTHING
  `

export const ProfileProviderProjectionLayer = EventLog.group(
  ProfileProviderEventGroup,
  (handlers) =>
    handlers
      .handle("ProfileCreated", ({ payload }) =>
        Effect.gen(function* () {
          const sql = yield* SqlClient
          yield* sql`
            INSERT INTO profile_provider_profiles_read
              (profile_id, owner_agent_id, active_branch_id, status, current_revision, current_schema_version,
               masked_profile_json, latest_metadata_json, latest_pii_storage_key, pii_jurisdiction,
               published_snapshot_id, created_at, updated_at)
            VALUES (
              ${payload.profileId},
              ${payload.ownerAgentId},
              ${payload.branchId},
              ${"draft"},
              ${payload.revision},
              ${payload.schemaVersion},
              ${payload.maskedProfileJson},
              ${"{}"},
              ${""},
              ${""},
              ${""},
              ${payload.createdAt.toJSON()},
              ${payload.createdAt.toJSON()}
            )
            ON CONFLICT (profile_id) DO UPDATE SET
              owner_agent_id = EXCLUDED.owner_agent_id,
              active_branch_id = EXCLUDED.active_branch_id,
              status = EXCLUDED.status,
              current_revision = EXCLUDED.current_revision,
              current_schema_version = EXCLUDED.current_schema_version,
              masked_profile_json = EXCLUDED.masked_profile_json,
              updated_at = EXCLUDED.updated_at
          `
          yield* enqueueOutbox(sql, payload.profileId, payload.revision, "ProfileCreated", payload)
        }).pipe(Effect.orDie)
      )
      .handle("MergedDataProfile", ({ payload }) =>
        Effect.gen(function* () {
          const sql = yield* SqlClient
          yield* sql`
            UPDATE profile_provider_profiles_read
            SET active_branch_id = ${payload.branchId},
                status = ${"draft"},
                current_revision = ${payload.revision},
                current_schema_version = ${payload.schemaVersion},
                masked_profile_json = ${payload.maskedProfileJson},
                updated_at = ${payload.mergedAt.toJSON()}
            WHERE profile_id = ${payload.profileId}
          `
          yield* enqueueOutbox(sql, payload.profileId, payload.revision, "MergedDataProfile", payload)
        }).pipe(Effect.orDie)
      )
      .handle("SnapshotCreatedProfile", ({ payload }) =>
        Effect.gen(function* () {
          const sql = yield* SqlClient
          yield* sql`
            INSERT INTO profile_provider_snapshots_read
              (snapshot_id, profile_id, branch_id, revision, snapshot_type, profile_json, metadata_json,
               schema_version, summary, published, strategy_json, created_at, created_by)
            VALUES (
              ${payload.snapshotId},
              ${payload.profileId},
              ${payload.branchId},
              ${payload.revision},
              ${payload.snapshotType},
              ${payload.profileJson},
              ${payload.metadataJson},
              ${payload.schemaVersion},
              ${payload.summary},
              ${false},
              ${""},
              ${payload.createdAt.toJSON()},
              ${payload.createdBy}
            )
            ON CONFLICT (snapshot_id) DO UPDATE SET
              revision = EXCLUDED.revision,
              profile_json = EXCLUDED.profile_json,
              metadata_json = EXCLUDED.metadata_json,
              schema_version = EXCLUDED.schema_version,
              summary = EXCLUDED.summary
          `
          yield* enqueueOutbox(sql, payload.profileId, payload.revision, "SnapshotCreatedProfile", payload)
        }).pipe(Effect.orDie)
      )
      .handle("MetaDataCreated", ({ payload }) =>
        Effect.gen(function* () {
          const sql = yield* SqlClient
          if (payload.scope === "snapshot") {
            yield* sql`
              UPDATE profile_provider_snapshots_read
              SET metadata_json = ${payload.metadataJson},
                  schema_version = ${payload.schemaVersion}
              WHERE snapshot_id = ${payload.scopeId}
            `
          } else {
            yield* sql`
              UPDATE profile_provider_profiles_read
              SET latest_metadata_json = ${payload.metadataJson},
                  current_schema_version = ${payload.schemaVersion},
                  updated_at = ${payload.createdAt.toJSON()}
              WHERE profile_id = ${payload.profileId}
            `
          }
          yield* enqueueOutbox(sql, payload.profileId, payload.revision, "MetaDataCreated", payload)
        }).pipe(Effect.orDie)
      )
      .handle("PersonalDataExtracted", ({ payload }) =>
        Effect.gen(function* () {
          const sql = yield* SqlClient
          yield* sql`
            UPDATE profile_provider_profiles_read
            SET latest_pii_storage_key = ${payload.piiStorageKey},
                pii_jurisdiction = ${payload.jurisdiction},
                updated_at = ${payload.extractedAt.toJSON()}
            WHERE profile_id = ${payload.profileId}
          `
          yield* enqueueOutbox(sql, payload.profileId, payload.revision, "PersonalDataExtracted", payload)
        }).pipe(Effect.orDie)
      )
      .handle("ProfileBranchForked", ({ payload }) =>
        Effect.gen(function* () {
          const sql = yield* SqlClient
          yield* sql`
            UPDATE profile_provider_profiles_read
            SET active_branch_id = ${payload.branchId},
                current_revision = ${payload.revision},
                updated_at = ${payload.createdAt.toJSON()}
            WHERE profile_id = ${payload.profileId}
          `
          yield* enqueueOutbox(sql, payload.profileId, payload.revision, "ProfileBranchForked", payload)
        }).pipe(Effect.orDie)
      )
      .handle("SnapshotPublishedProfile", ({ payload }) =>
        Effect.gen(function* () {
          const sql = yield* SqlClient
          yield* sql`
            UPDATE profile_provider_profiles_read
            SET status = ${"published"},
                published_snapshot_id = ${payload.snapshotId},
                current_revision = ${payload.revision},
                updated_at = ${payload.publishedAt.toJSON()}
            WHERE profile_id = ${payload.profileId}
          `
          yield* sql`
            UPDATE profile_provider_snapshots_read
            SET published = ${true},
                strategy_json = ${payload.strategyJson}
            WHERE snapshot_id = ${payload.snapshotId}
          `
          yield* enqueueOutbox(sql, payload.profileId, payload.revision, "SnapshotPublishedProfile", payload)
        }).pipe(Effect.orDie)
      )
)
