/**
 * Profile provider snapshot service — saves and loads aggregate state to/from PostgreSQL.
 *
 * Snapshots allow the entity (or the dev-mode in-memory store) to recover state without
 * replaying every event from the beginning. A snapshot is saved every SNAPSHOT_EVERY
 * events; on startup the latest snapshot is loaded and only events after it need replaying.
 *
 * SNAPSHOT_EVERY = 25 is lower than the order service (50) because profile commands
 * typically emit 2–3 events each, so a snapshot every 25 events ≈ every 8–12 commands.
 *
 * Serialization:
 *   ProfileState contains DateTime.Utc values and nested classes — not directly JSON-friendly.
 *   encodeState/decodeState convert to/from a plain SnapshotJson object:
 *     DateTime.Utc       → ISO string  (via Schema.DateTimeUtc codec)
 *     ProfileBranch[]    → plain object array (createdAt encoded)
 *     ProfileRevisionEntry[] → plain object array (occurredAt encoded)
 *     ProfileSnapshot[]  → plain object array (createdAt encoded)
 */
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import { SqlClient } from "@effect/sql/SqlClient"
import type { SqlError } from "@effect/sql/SqlError"
import {
  ProfileBranch,
  ProfileRevisionEntry,
  ProfileSnapshot,
  ProfileState,
  SourceAsset
} from "./contracts.js"

export const SNAPSHOT_EVERY = 25

type SnapshotJson = {
  status: "empty" | "draft" | "published"
  profileId: string
  ownerAgentId: string
  activeBranchId: string
  currentSchemaVersion: string
  maskedProfileJson: string
  latestMetadataJson: string
  latestPiiStorageKey: string
  piiJurisdiction: string
  sourceAssets: Array<{
    sourceId: string
    kind: "file" | "url" | "manual" | "parsed"
    uri: string
    mediaType: string
    storageKey: string
    summary: string
  }>
  branches: Array<{
    branchId: string
    label: string
    baseBranchId: string
    baseRevision: number
    baseSnapshotId: string
    createdAt: string
    createdBy: string
  }>
  revisions: Array<{
    revision: number
    branchId: string
    eventType: string
    summary: string
    occurredAt: string
    actorId: string
  }>
  snapshots: Array<{
    snapshotId: string
    branchId: string
    revision: number
    snapshotType: "AUTO" | "MANUAL" | "MIGRATION" | "LLM"
    profileJson: string
    metadataJson: string
    schemaVersion: string
    summary: string
    createdAt: string
    createdBy: string
    published: boolean
    strategyJson: string
  }>
  publishedSnapshotId: string
  revision: number
}

const encodeDateTime = Schema.encodeSync(Schema.DateTimeUtc)
const decodeDateTime = Schema.decodeSync(Schema.DateTimeUtc)

const encodeState = (state: ProfileState): string =>
  JSON.stringify({
    ...state,
    sourceAssets: state.sourceAssets.map((asset) => ({ ...asset })),
    branches: state.branches.map((branch) => ({ ...branch, createdAt: encodeDateTime(branch.createdAt) })),
    revisions: state.revisions.map((entry) => ({ ...entry, occurredAt: encodeDateTime(entry.occurredAt) })),
    snapshots: state.snapshots.map((snapshot) => ({ ...snapshot, createdAt: encodeDateTime(snapshot.createdAt) }))
  } satisfies SnapshotJson)

const decodeState = (json: string): ProfileState => {
  const data = JSON.parse(json) as SnapshotJson
  return new ProfileState({
    status: data.status,
    profileId: data.profileId,
    ownerAgentId: data.ownerAgentId,
    activeBranchId: data.activeBranchId,
    currentSchemaVersion: data.currentSchemaVersion,
    maskedProfileJson: data.maskedProfileJson,
    latestMetadataJson: data.latestMetadataJson,
    latestPiiStorageKey: data.latestPiiStorageKey,
    piiJurisdiction: data.piiJurisdiction,
    sourceAssets: data.sourceAssets.map((asset) => new SourceAsset(asset)),
    branches: data.branches.map((branch) =>
      new ProfileBranch({ ...branch, createdAt: decodeDateTime(branch.createdAt) })
    ),
    revisions: data.revisions.map((entry) =>
      new ProfileRevisionEntry({ ...entry, occurredAt: decodeDateTime(entry.occurredAt) })
    ),
    snapshots: data.snapshots.map((snapshot) =>
      new ProfileSnapshot({ ...snapshot, createdAt: decodeDateTime(snapshot.createdAt) })
    ),
    publishedSnapshotId: data.publishedSnapshotId,
    revision: data.revision
  })
}

export type SnapshotEntry = { readonly state: ProfileState; readonly revision: number }

export class ProfileProviderSnapshots extends Context.Tag("ProfileProviderSnapshots")<
  ProfileProviderSnapshots,
  {
    /** Load the latest snapshot for a profile. Returns none if no snapshot exists. */
    readonly load: (profileId: string) => Effect.Effect<Option.Option<SnapshotEntry>, SqlError>
    /** Persist the current state as the latest snapshot for a profile. */
    readonly save: (profileId: string, state: ProfileState, revision: number) => Effect.Effect<void, SqlError>
  }
>() {}

export const ProfileProviderSnapshotsLive = Layer.effect(
  ProfileProviderSnapshots,
  Effect.gen(function* () {
    const sql = yield* SqlClient
    return {
      load: (profileId) =>
        sql`
          SELECT state_json, revision
          FROM profile_provider_snapshots
          WHERE profile_id = ${profileId}
        `.pipe(
          Effect.map((rows) => {
            const row = rows[0] as { state_json: string; revision: number } | undefined
            if (!row) return Option.none<SnapshotEntry>()
            return Option.some({ state: decodeState(row.state_json), revision: row.revision })
          })
        ),
      save: (profileId, state, revision) =>
        sql`
          INSERT INTO profile_provider_snapshots (profile_id, state_json, revision, saved_at)
          VALUES (${profileId}, ${encodeState(state)}, ${revision}, ${new Date().toISOString()})
          ON CONFLICT (profile_id) DO UPDATE SET
            state_json = EXCLUDED.state_json,
            revision = EXCLUDED.revision,
            saved_at = EXCLUDED.saved_at
        `.pipe(Effect.asVoid)
    }
  })
)
