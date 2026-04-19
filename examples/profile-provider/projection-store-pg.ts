import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { SqlClient } from "@effect/sql/SqlClient"
import { ProfileDocument } from "./contracts.js"
import { makeDispatch, ProfileProviderProjectionStore, type ProjectionHandlers } from "./projection-store.js"

const encodeProfile = Schema.encodeSync(ProfileDocument)

export const ProfileProviderProjectionStorePgLive = Layer.effect(
  ProfileProviderProjectionStore,
  Effect.gen(function* () {
    const sql = yield* SqlClient

    const handlers: Omit<ProjectionHandlers, "dispatch"> = {
      onProfileCreated: (event) =>
        sql`
          INSERT INTO profile_provider_profiles_read
            (profile_id, owner_agent_id, active_branch_id, status, current_revision, current_schema_version,
             masked_profile_json, latest_metadata_json, latest_pii_storage_key, pii_jurisdiction,
             published_snapshot_id, created_at, updated_at)
          VALUES (
            ${event.profileId}, ${event.ownerAgentId}, ${event.branchId}, ${"draft"},
            ${event.revision}, ${event.schemaVersion},
            ${JSON.stringify(encodeProfile(event.maskedProfileJson))},
            ${"{}"},  ${""},  ${""},  ${""},
            ${String(event.occurredAt.toJSON())}, ${String(event.occurredAt.toJSON())}
          )
          ON CONFLICT (profile_id) DO UPDATE SET
            owner_agent_id = EXCLUDED.owner_agent_id,
            active_branch_id = EXCLUDED.active_branch_id,
            status = EXCLUDED.status,
            current_revision = EXCLUDED.current_revision,
            current_schema_version = EXCLUDED.current_schema_version,
            masked_profile_json = EXCLUDED.masked_profile_json,
            updated_at = EXCLUDED.updated_at
        `.pipe(Effect.asVoid),

      onMergedDataProfile: (event) =>
        sql`
          UPDATE profile_provider_profiles_read
          SET active_branch_id = ${event.branchId},
              status = ${"draft"},
              current_revision = ${event.revision},
              current_schema_version = ${event.schemaVersion},
              masked_profile_json = ${JSON.stringify(encodeProfile(event.maskedProfileJson))},
              updated_at = ${String(event.occurredAt.toJSON())}
          WHERE profile_id = ${event.profileId}
        `.pipe(Effect.asVoid),

      onSnapshotCreatedProfile: (event) =>
        sql`
          INSERT INTO profile_provider_snapshots_read
            (snapshot_id, profile_id, branch_id, revision, snapshot_type, profile_json, metadata_json,
             schema_version, summary, published, strategy_json, created_at, created_by)
          VALUES (
            ${event.snapshotId}, ${event.profileId}, ${event.branchId},
            ${event.revision}, ${event.snapshotType},
            ${JSON.stringify(encodeProfile(event.profileJson))},
            ${event.metadataJson}, ${event.schemaVersion}, ${event.summary},
            ${false}, ${""}, ${String(event.occurredAt.toJSON())}, ${event.actorId}
          )
          ON CONFLICT (snapshot_id) DO UPDATE SET
            revision = EXCLUDED.revision,
            profile_json = EXCLUDED.profile_json,
            metadata_json = EXCLUDED.metadata_json,
            schema_version = EXCLUDED.schema_version,
            summary = EXCLUDED.summary
        `.pipe(Effect.asVoid),

      onMetaDataCreated: (event) => {
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
              updated_at = ${String(event.occurredAt.toJSON())}
          WHERE profile_id = ${event.profileId}
        `.pipe(Effect.asVoid)
      },

      onPersonalDataExtracted: (event) =>
        sql`
          UPDATE profile_provider_profiles_read
          SET latest_pii_storage_key = ${event.piiStorageKey},
              pii_jurisdiction = ${event.jurisdiction},
              updated_at = ${String(event.occurredAt.toJSON())}
          WHERE profile_id = ${event.profileId}
        `.pipe(Effect.asVoid),

      onProfileBranchForked: (event) =>
        sql`
          UPDATE profile_provider_profiles_read
          SET active_branch_id = ${event.branchId},
              current_revision = ${event.revision},
              updated_at = ${String(event.occurredAt.toJSON())}
          WHERE profile_id = ${event.profileId}
        `.pipe(Effect.asVoid),

      onSnapshotPublishedProfile: (event) =>
        Effect.gen(function* () {
          yield* sql`
            UPDATE profile_provider_profiles_read
            SET status = ${"published"},
                published_snapshot_id = ${event.snapshotId},
                current_revision = ${event.revision},
                updated_at = ${String(event.occurredAt.toJSON())}
            WHERE profile_id = ${event.profileId}
          `.pipe(Effect.asVoid)
          yield* sql`
            UPDATE profile_provider_snapshots_read
            SET published = ${true},
                strategy_json = ${event.strategyJson}
            WHERE snapshot_id = ${event.snapshotId}
          `.pipe(Effect.asVoid)
        }),

    }
    return { ...handlers, dispatch: makeDispatch(handlers) }
  })
)
