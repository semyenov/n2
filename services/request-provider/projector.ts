import * as Effect from "effect/Effect"
import { EventLog } from "@effect/experimental"
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
      .handle("RequestCreated", ({ payload }) =>
        Effect.gen(function* () {
          const store = yield* RequestProviderProjectionStore
          const outbox = yield* RequestProviderOutbox
          const event = new RequestCreated(payload)
          yield* store.onRequestCreated(event)
          yield* outbox.enqueue(makeMessage(event))
        }).pipe(Effect.orDie)
      )
      .handle("RequestUpdated", ({ payload }) =>
        Effect.gen(function* () {
          const store = yield* RequestProviderProjectionStore
          const outbox = yield* RequestProviderOutbox
          const event = new RequestUpdated(payload)
          yield* store.onRequestUpdated(event)
          yield* outbox.enqueue(makeMessage(event))
        }).pipe(Effect.orDie)
      )
      .handle("RequestMetaDataCreated", ({ payload }) =>
        Effect.gen(function* () {
          const store = yield* RequestProviderProjectionStore
          const outbox = yield* RequestProviderOutbox
          const event = new RequestMetaDataCreated(payload)
          yield* store.onRequestMetaDataCreated(event)
          yield* outbox.enqueue(makeMessage(event))
        }).pipe(Effect.orDie)
      )
      .handle("RequestSnapshotCreated", ({ payload }) =>
        Effect.gen(function* () {
          const store = yield* RequestProviderProjectionStore
          const outbox = yield* RequestProviderOutbox
          const event = new RequestSnapshotCreated(payload)
          yield* store.onRequestSnapshotCreated(event)
          yield* outbox.enqueue(makeMessage(event))
        }).pipe(Effect.orDie)
      )
)
