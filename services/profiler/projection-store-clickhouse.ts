import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as ClickhouseClient from "@effect/sql-clickhouse/ClickhouseClient"
import { ProfileDocument } from "./contracts.js"
import type {
  ProfileCreated,
  MergedDataProfile,
  SnapshotCreatedProfile,
  MetaDataCreated,
  PersonalDataExtracted,
  ProfileBranchForked,
  SnapshotPublishedProfile
} from "./contracts.js"
import { makeDispatch, ProfileProviderProjectionStore, type ProjectionHandlers } from "./projection-store.js"

type ProfileCurrentRow = {
  readonly profile_id: string
  readonly revision: number
  readonly owner_agent_id: string
  readonly active_branch_id: string
  readonly status: string
  readonly current_schema_version: string
  readonly masked_profile_json: string
  readonly latest_metadata_json: string
  readonly latest_pii_storage_key: string
  readonly pii_jurisdiction: string
  readonly published_snapshot_id: string
  readonly created_at: string
  readonly updated_at: string
}

type SnapshotCurrentRow = {
  readonly snapshot_id: string
  readonly profile_id: string
  readonly branch_id: string
  readonly revision: number
  readonly snapshot_type: string
  readonly profile_json: string
  readonly metadata_json: string
  readonly schema_version: string
  readonly summary: string
  readonly published: number
  readonly strategy_json: string
  readonly created_at: string
  readonly created_by: string
}

const encodeProfile = Schema.encodeSync(ProfileDocument)

const initialProfileRow = (profileId: string): ProfileCurrentRow => ({
  profile_id: profileId,
  revision: 0,
  owner_agent_id: "",
  active_branch_id: "",
  status: "empty",
  current_schema_version: "",
  masked_profile_json: "",
  latest_metadata_json: "{}",
  latest_pii_storage_key: "",
  pii_jurisdiction: "",
  published_snapshot_id: "",
  created_at: "",
  updated_at: ""
})

const loadLatestProfile = (ch: ClickhouseClient.ClickhouseClient, profileId: string) =>
  ch<ProfileCurrentRow>`
    SELECT profile_id, revision, owner_agent_id, active_branch_id, status,
           current_schema_version, masked_profile_json, latest_metadata_json,
           latest_pii_storage_key, pii_jurisdiction, published_snapshot_id,
           created_at, updated_at
    FROM profile_provider_profiles_current
    WHERE profile_id = ${profileId}
    ORDER BY revision DESC LIMIT 1
  `.pipe(Effect.map((rows) => rows[0]))

const loadLatestSnapshot = (ch: ClickhouseClient.ClickhouseClient, snapshotId: string) =>
  ch<SnapshotCurrentRow>`
    SELECT snapshot_id, profile_id, branch_id, revision, snapshot_type,
           profile_json, metadata_json, schema_version, summary, published,
           strategy_json, created_at, created_by
    FROM profile_provider_snapshots_current
    WHERE snapshot_id = ${snapshotId}
    ORDER BY revision DESC LIMIT 1
  `.pipe(Effect.map((rows) => rows[0]))

const insertEvent = (
  ch: ClickhouseClient.ClickhouseClient,
  profileId: string,
  revision: number,
  eventType: string,
  occurredAt: string,
  branchId: string,
  snapshotId: string,
  payload: unknown
) =>
  ch.insertQuery({
    table: "profile_provider_projection_events",
    values: [{
      profile_id: profileId,
      revision,
      event_type: eventType,
      occurred_at: occurredAt,
      branch_id: branchId,
      snapshot_id: snapshotId,
      payload_json: JSON.stringify(payload)
    }]
  }).pipe(Effect.asVoid)

export const ProfileProviderProjectionStoreClickhouseLive = Layer.effect(
  ProfileProviderProjectionStore,
  Effect.gen(function* () {
    const ch = yield* ClickhouseClient.ClickhouseClient

    const handlers: Omit<ProjectionHandlers, "dispatch"> = {
      onProfileCreated: (event: ProfileCreated) =>
        Effect.gen(function* () {
          yield* insertEvent(ch, event.profileId, event.revision, "ProfileCreated", String(event.occurredAt.toJSON()), event.branchId, "", event)
          const current = (yield* loadLatestProfile(ch, event.profileId)) ?? initialProfileRow(event.profileId)
          yield* ch.insertQuery({
            table: "profile_provider_profiles_current",
            values: [{
              ...current,
              revision: event.revision,
              owner_agent_id: event.ownerAgentId,
              active_branch_id: event.branchId,
              status: "draft",
              current_schema_version: event.schemaVersion,
              masked_profile_json: JSON.stringify(encodeProfile(event.maskedProfileJson)),
              created_at: String(event.occurredAt.toJSON()),
              updated_at: String(event.occurredAt.toJSON())
            }]
          }).pipe(Effect.asVoid)
        }),

      onMergedDataProfile: (event: MergedDataProfile) =>
        Effect.gen(function* () {
          yield* insertEvent(ch, event.profileId, event.revision, "MergedDataProfile", String(event.occurredAt.toJSON()), event.branchId, "", event)
          const current = (yield* loadLatestProfile(ch, event.profileId)) ?? initialProfileRow(event.profileId)
          yield* ch.insertQuery({
            table: "profile_provider_profiles_current",
            values: [{
              ...current,
              revision: event.revision,
              active_branch_id: event.branchId,
              status: "draft",
              current_schema_version: event.schemaVersion,
              masked_profile_json: JSON.stringify(encodeProfile(event.maskedProfileJson)),
              updated_at: String(event.occurredAt.toJSON())
            }]
          }).pipe(Effect.asVoid)
        }),

      onSnapshotCreatedProfile: (event: SnapshotCreatedProfile) =>
        Effect.gen(function* () {
          yield* insertEvent(ch, event.profileId, event.revision, "SnapshotCreatedProfile", String(event.occurredAt.toJSON()), event.branchId, event.snapshotId, event)
          yield* ch.insertQuery({
            table: "profile_provider_snapshots_current",
            values: [{
              snapshot_id: event.snapshotId,
              profile_id: event.profileId,
              branch_id: event.branchId,
              revision: event.revision,
              snapshot_type: event.snapshotType,
              profile_json: JSON.stringify(encodeProfile(event.profileJson)),
              metadata_json: event.metadataJson,
              schema_version: event.schemaVersion,
              summary: event.summary,
              published: 0,
              strategy_json: "",
              created_at: String(event.occurredAt.toJSON()),
              created_by: event.actorId
            }]
          }).pipe(Effect.asVoid)
        }),

      onMetaDataCreated: (event: MetaDataCreated) =>
        Effect.gen(function* () {
          yield* insertEvent(ch, event.profileId, event.revision, "MetaDataCreated", String(event.occurredAt.toJSON()), event.branchId, "", event)
          if (event.scope === "snapshot") {
            const current = yield* loadLatestSnapshot(ch, event.scopeId)
            if (current) {
              yield* ch.insertQuery({
                table: "profile_provider_snapshots_current",
                values: [{ ...current, revision: event.revision, metadata_json: event.metadataJson, schema_version: event.schemaVersion }]
              }).pipe(Effect.asVoid)
            }
          } else {
            const current = (yield* loadLatestProfile(ch, event.profileId)) ?? initialProfileRow(event.profileId)
            yield* ch.insertQuery({
              table: "profile_provider_profiles_current",
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

      onPersonalDataExtracted: (event: PersonalDataExtracted) =>
        Effect.gen(function* () {
          yield* insertEvent(ch, event.profileId, event.revision, "PersonalDataExtracted", String(event.occurredAt.toJSON()), event.branchId, "", event)
          const current = (yield* loadLatestProfile(ch, event.profileId)) ?? initialProfileRow(event.profileId)
          yield* ch.insertQuery({
            table: "profile_provider_profiles_current",
            values: [{
              ...current,
              revision: event.revision,
              latest_pii_storage_key: event.piiStorageKey,
              pii_jurisdiction: event.jurisdiction,
              updated_at: String(event.occurredAt.toJSON())
            }]
          }).pipe(Effect.asVoid)
        }),

      onProfileBranchForked: (event: ProfileBranchForked) =>
        Effect.gen(function* () {
          yield* insertEvent(ch, event.profileId, event.revision, "ProfileBranchForked", String(event.occurredAt.toJSON()), event.branchId, "", event)
          const current = (yield* loadLatestProfile(ch, event.profileId)) ?? initialProfileRow(event.profileId)
          yield* ch.insertQuery({
            table: "profile_provider_profiles_current",
            values: [{
              ...current,
              revision: event.revision,
              active_branch_id: event.branchId,
              updated_at: String(event.occurredAt.toJSON())
            }]
          }).pipe(Effect.asVoid)
        }),

      onSnapshotPublishedProfile: (event: SnapshotPublishedProfile) =>
        Effect.gen(function* () {
          yield* insertEvent(ch, event.profileId, event.revision, "SnapshotPublishedProfile", String(event.occurredAt.toJSON()), "", event.snapshotId, event)
          const currentProfile = (yield* loadLatestProfile(ch, event.profileId)) ?? initialProfileRow(event.profileId)
          yield* ch.insertQuery({
            table: "profile_provider_profiles_current",
            values: [{
              ...currentProfile,
              revision: event.revision,
              status: "published",
              published_snapshot_id: event.snapshotId,
              updated_at: String(event.occurredAt.toJSON())
            }]
          }).pipe(Effect.asVoid)
          const currentSnapshot = yield* loadLatestSnapshot(ch, event.snapshotId)
          if (currentSnapshot) {
            yield* ch.insertQuery({
              table: "profile_provider_snapshots_current",
              values: [{ ...currentSnapshot, revision: event.revision, published: 1, strategy_json: event.strategyJson }]
            }).pipe(Effect.asVoid)
          }
        })
    }
    return { ...handlers, dispatch: makeDispatch(handlers) }
  })
)
