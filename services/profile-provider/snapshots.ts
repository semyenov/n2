/**
 * Profile provider snapshot service — saves and loads aggregate state to/from PostgreSQL.
 *
 * Uses the framework's generic makeSnapshotService factory, which handles
 * Schema codec serialization and SQL upsert logic.
 */
import * as Context from "effect/Context"
import { makeStandardSnapshotWiring, type SnapshotService, type SnapshotEntry } from "@semyenov/n2/helpers"
import { ProfileState } from "./contracts.js"

export const SNAPSHOT_EVERY = 25

export type { SnapshotEntry }

export class ProfileProviderSnapshots extends Context.Tag("ProfileProviderSnapshots")<
  ProfileProviderSnapshots,
  SnapshotService<ProfileState>
>() {}

const snapshots = makeStandardSnapshotWiring({
  tag: ProfileProviderSnapshots,
  table: "profile_provider_snapshots",
  stateSchema: ProfileState,
  idColumn: "profile_id",
  every: SNAPSHOT_EVERY
})

export const ProfileProviderSnapshotsLive = snapshots.live
export const ProfileProviderSnapshotOps = snapshots.ops
