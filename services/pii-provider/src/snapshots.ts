import * as Context from "effect/Context"
import { makeStandardSnapshotWiring, type SnapshotEntry, type SnapshotService } from "@semyenov/n2/helpers"
import { PIIState } from "./contracts/state.js"

export const SNAPSHOT_EVERY = 25

export type { SnapshotEntry }

export class PIIProviderSnapshots extends Context.Tag("PIIProviderSnapshots")<
  PIIProviderSnapshots,
  SnapshotService<PIIState>
>() {}

const snapshots = makeStandardSnapshotWiring({
  tag: PIIProviderSnapshots,
  table: "pii_provider_snapshots",
  stateSchema: PIIState,
  idColumn: "storage_key",
  every: SNAPSHOT_EVERY
})

export const PIIProviderSnapshotsLive = snapshots.live
export const PIIProviderSnapshotOps = snapshots.ops
