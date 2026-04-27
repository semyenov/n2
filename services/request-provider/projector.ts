import { EventLog } from "@effect/experimental"
import { wireProjectionHandler } from "@semyenov/n2/helpers"
import {
  type RequestProviderEvent,
  RequestCreated,
  RequestMetaDataCreated,
  RequestSnapshotCreated,
  RequestUpdated
} from "./contracts.js"
import { RequestProviderEventGroup } from "./events.js"
import { RequestProviderOutbox } from "./outbox.js"
import { RequestProviderProjectionStore } from "./projection-store.js"
import { makeRequestProviderEventMessage } from "./workflows.js"

const makeMessage = (event: RequestProviderEvent) =>
  makeRequestProviderEventMessage({
    requestId: event.requestId,
    revision: event.revision,
    eventType: event._tag,
    occurredAt: String(event.occurredAt.toJSON()),
    payload: event
  })

export const RequestProviderProjectionLayer = EventLog.group(
  RequestProviderEventGroup,
  (handlers) =>
    handlers
      .handle("RequestCreated", wireProjectionHandler(RequestProviderProjectionStore, RequestProviderOutbox, RequestCreated, "onRequestCreated", makeMessage))
      .handle("RequestUpdated", wireProjectionHandler(RequestProviderProjectionStore, RequestProviderOutbox, RequestUpdated, "onRequestUpdated", makeMessage))
      .handle("RequestMetaDataCreated", wireProjectionHandler(RequestProviderProjectionStore, RequestProviderOutbox, RequestMetaDataCreated, "onRequestMetaDataCreated", makeMessage))
      .handle("RequestSnapshotCreated", wireProjectionHandler(RequestProviderProjectionStore, RequestProviderOutbox, RequestSnapshotCreated, "onRequestSnapshotCreated", makeMessage))
)
