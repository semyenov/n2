import { EventGroup } from "@effect/experimental"
import * as EventLogApi from "@effect/experimental/EventLog"
import * as N2 from "@semyenov/n2/helpers"
import {
  RequestCreated,
  RequestMetaDataCreated,
  RequestSnapshotCreated,
  RequestUpdated
} from "./contracts.js"

export const RequestProviderEventGroup = EventGroup.empty
  .add({
    tag: "RequestCreated",
    primaryKey: (p: { requestId: string }) => p.requestId,
    payload: N2.eventPayloadSchema(RequestCreated)
  })
  .add({
    tag: "RequestUpdated",
    primaryKey: (p: { requestId: string }) => p.requestId,
    payload: N2.eventPayloadSchema(RequestUpdated)
  })
  .add({
    tag: "RequestMetaDataCreated",
    primaryKey: (p: { requestId: string }) => p.requestId,
    payload: N2.eventPayloadSchema(RequestMetaDataCreated)
  })
  .add({
    tag: "RequestSnapshotCreated",
    primaryKey: (p: { requestId: string }) => p.requestId,
    payload: N2.eventPayloadSchema(RequestSnapshotCreated)
  })

export const RequestProviderEventLogSchema = EventLogApi.schema(RequestProviderEventGroup)
