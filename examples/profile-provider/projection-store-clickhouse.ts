import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as ClickhouseClient from "@effect/sql-clickhouse/ClickhouseClient"
import {
  type ProfileEvent,
  ProfileDocument
} from "./contracts.js"
import { ProfileProviderOutbox } from "./outbox.js"
import { ProfileProviderProjectionStore } from "./projection-store.js"
import type { ProfileProviderEventMessage } from "./workflows.js"

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

const loadLatestProfile = (clickhouse: ClickhouseClient.ClickhouseClient, profileId: string) =>
  clickhouse<ProfileCurrentRow>`
    SELECT
      profile_id,
      revision,
      owner_agent_id,
      active_branch_id,
      status,
      current_schema_version,
      masked_profile_json,
      latest_metadata_json,
      latest_pii_storage_key,
      pii_jurisdiction,
      published_snapshot_id,
      created_at,
      updated_at
    FROM profile_provider_profiles_current
    WHERE profile_id = ${profileId}
    ORDER BY revision DESC
    LIMIT 1
  `.pipe(Effect.map((rows) => rows[0]))

const loadLatestSnapshot = (clickhouse: ClickhouseClient.ClickhouseClient, snapshotId: string) =>
  clickhouse<SnapshotCurrentRow>`
    SELECT
      snapshot_id,
      profile_id,
      branch_id,
      revision,
      snapshot_type,
      profile_json,
      metadata_json,
      schema_version,
      summary,
      published,
      strategy_json,
      created_at,
      created_by
    FROM profile_provider_snapshots_current
    WHERE snapshot_id = ${snapshotId}
    ORDER BY revision DESC
    LIMIT 1
  `.pipe(Effect.map((rows) => rows[0]))

export const insertProfileProviderProjectionEvent = (
  clickhouse: ClickhouseClient.ClickhouseClient,
  event: ProfileEvent
) => {
  const branchId = "branchId" in event ? event.branchId : ""
  const snapshotId = "snapshotId" in event ? event.snapshotId : ""
  const occurredAt = (() => {
    switch (event._tag) {
      case "ProfileCreated":
        return event.createdAt.toJSON()
      case "MergedDataProfile":
        return event.mergedAt.toJSON()
      case "SnapshotCreatedProfile":
        return event.createdAt.toJSON()
      case "MetaDataCreated":
        return event.createdAt.toJSON()
      case "PersonalDataExtracted":
        return event.extractedAt.toJSON()
      case "ProfileBranchForked":
        return event.createdAt.toJSON()
      case "SnapshotPublishedProfile":
        return event.publishedAt.toJSON()
    }
  })()

  return clickhouse.insertQuery({
    table: "profile_provider_projection_events",
    values: [{
      profile_id: event.profileId,
      revision: event.revision,
      event_type: event._tag,
      occurred_at: occurredAt,
      branch_id: branchId,
      snapshot_id: snapshotId,
      payload_json: JSON.stringify(event)
    }]
  }).pipe(Effect.asVoid)
}

export const upsertProfileProviderCurrentProfile = (
  clickhouse: ClickhouseClient.ClickhouseClient,
  event: ProfileEvent
) =>
  Effect.gen(function* () {
    const current = (yield* loadLatestProfile(clickhouse, event.profileId)) ?? initialProfileRow(event.profileId)
    const next = (() => {
      switch (event._tag) {
        case "ProfileCreated":
          return {
            ...current,
            revision: event.revision,
            owner_agent_id: event.ownerAgentId,
            active_branch_id: event.branchId,
            status: "draft",
            current_schema_version: event.schemaVersion,
            masked_profile_json: JSON.stringify(encodeProfile(event.maskedProfileJson)),
            created_at: event.createdAt.toJSON(),
            updated_at: event.createdAt.toJSON()
          }
        case "MergedDataProfile":
          return {
            ...current,
            revision: event.revision,
            active_branch_id: event.branchId,
            status: "draft",
            current_schema_version: event.schemaVersion,
            masked_profile_json: JSON.stringify(encodeProfile(event.maskedProfileJson)),
            updated_at: event.mergedAt.toJSON()
          }
        case "MetaDataCreated":
          return event.scope === "snapshot"
            ? null
            : {
                ...current,
                revision: event.revision,
                current_schema_version: event.schemaVersion,
                latest_metadata_json: event.metadataJson,
                updated_at: event.createdAt.toJSON()
              }
        case "PersonalDataExtracted":
          return {
            ...current,
            revision: event.revision,
            latest_pii_storage_key: event.piiStorageKey,
            pii_jurisdiction: event.jurisdiction,
            updated_at: event.extractedAt.toJSON()
          }
        case "ProfileBranchForked":
          return {
            ...current,
            revision: event.revision,
            active_branch_id: event.branchId,
            updated_at: event.createdAt.toJSON()
          }
        case "SnapshotPublishedProfile":
          return {
            ...current,
            revision: event.revision,
            status: "published",
            published_snapshot_id: event.snapshotId,
            updated_at: event.publishedAt.toJSON()
          }
        case "SnapshotCreatedProfile":
          return null
      }
    })()

    if (next === null) return

    yield* clickhouse.insertQuery({
      table: "profile_provider_profiles_current",
      values: [next]
    }).pipe(Effect.asVoid)
  })

export const upsertProfileProviderCurrentSnapshot = (
  clickhouse: ClickhouseClient.ClickhouseClient,
  event: ProfileEvent
) =>
  Effect.gen(function* () {
    const next = (() => {
      switch (event._tag) {
        case "SnapshotCreatedProfile":
          return {
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
            created_at: event.createdAt.toJSON(),
            created_by: event.createdBy
          }
        case "MetaDataCreated":
          return event.scope !== "snapshot"
            ? null
            : "scopeId" in event
              ? event.scopeId
              : null
        case "SnapshotPublishedProfile":
          return event.snapshotId
        default:
          return null
      }
    })()

    if (next === null) return

    if (typeof next !== "string") {
      yield* clickhouse.insertQuery({
        table: "profile_provider_snapshots_current",
        values: [next]
      }).pipe(Effect.asVoid)
      return
    }

    const current = yield* loadLatestSnapshot(clickhouse, next)
    if (!current) return

    const merged = (() => {
      switch (event._tag) {
        case "MetaDataCreated":
          return {
            ...current,
            revision: event.revision,
            metadata_json: event.metadataJson,
            schema_version: event.schemaVersion
          }
        case "SnapshotPublishedProfile":
          return {
            ...current,
            revision: event.revision,
            published: 1,
            strategy_json: event.strategyJson
          }
        default:
          return current
      }
    })()

    yield* clickhouse.insertQuery({
      table: "profile_provider_snapshots_current",
      values: [merged]
    }).pipe(Effect.asVoid)
  })

export const ProfileProviderProjectionStoreClickhouseLive = Layer.effect(
  ProfileProviderProjectionStore,
  Effect.gen(function* () {
    const clickhouse = yield* ClickhouseClient.ClickhouseClient
    const outbox = yield* ProfileProviderOutbox

    return {
      project: (event: ProfileEvent, message: ProfileProviderEventMessage) =>
        Effect.gen(function* () {
          yield* insertProfileProviderProjectionEvent(clickhouse, event)
          yield* upsertProfileProviderCurrentProfile(clickhouse, event)
          yield* upsertProfileProviderCurrentSnapshot(clickhouse, event)
          yield* outbox.enqueue(message)
        })
    }
  })
)
