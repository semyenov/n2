/**
 * Profile provider snapshot service — saves and loads aggregate state to/from PostgreSQL.
 *
 * Uses the framework's generic makeSnapshotService factory, which handles
 * Schema codec serialization and SQL upsert logic.
 */
import * as Context from "effect/Context"
import { makeSnapshotService, type SnapshotService, type SnapshotEntry } from "../../src/framework/helpers/Snapshots.js"
import { ProfileState } from "./contracts.js"

export const SNAPSHOT_EVERY = 25

export type { SnapshotEntry }

export class ProfileProviderSnapshots extends Context.Tag("ProfileProviderSnapshots")<
  ProfileProviderSnapshots,
  SnapshotService<ProfileState>
>() {}

export const ProfileProviderSnapshotsLive = makeSnapshotService({
  table: "profile_provider_snapshots",
  stateSchema: ProfileState,
  idColumn: "profile_id"
}).makeLive(ProfileProviderSnapshots)
