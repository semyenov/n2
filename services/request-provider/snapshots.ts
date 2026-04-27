import * as Context from "effect/Context"
import { makeStandardSnapshotWiring, type SnapshotEntry, type SnapshotService } from "@semyenov/n2/helpers"
import { RequestState } from "./contracts.js"

export const SNAPSHOT_EVERY = 25

export type { SnapshotEntry }

export class RequestProviderSnapshots extends Context.Tag("RequestProviderSnapshots")<
  RequestProviderSnapshots,
  SnapshotService<RequestState>
>() {}

const snapshots = makeStandardSnapshotWiring({
  tag: RequestProviderSnapshots,
  table: "request_provider_snapshots",
  stateSchema: RequestState,
  idColumn: "request_id",
  every: SNAPSHOT_EVERY
})

export const RequestProviderSnapshotsLive = snapshots.live
export const RequestProviderSnapshotOps = snapshots.ops
