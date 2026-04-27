import * as Context from "effect/Context"
import { makeSnapshotOps, makeSnapshotService, type SnapshotEntry, type SnapshotService } from "@semyenov/n2/helpers"
import { RequestState } from "./contracts.js"

export const SNAPSHOT_EVERY = 25

export type { SnapshotEntry }

export class RequestProviderSnapshots extends Context.Tag("RequestProviderSnapshots")<
  RequestProviderSnapshots,
  SnapshotService<RequestState>
>() {}

export const RequestProviderSnapshotsLive = makeSnapshotService({
  table: "request_provider_snapshots",
  stateSchema: RequestState,
  idColumn: "request_id"
}).makeLive(RequestProviderSnapshots)

export const RequestProviderSnapshotOps = makeSnapshotOps(
  RequestProviderSnapshots,
  SNAPSHOT_EVERY
)
