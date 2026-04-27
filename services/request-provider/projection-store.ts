import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import type { SqlError } from "@effect/sql/SqlError"
import type {
  RequestCreated,
  RequestMetaDataCreated,
  RequestProviderEvent,
  RequestSnapshotCreated,
  RequestUpdated
} from "./contracts.js"

export interface ProjectionHandlers {
  readonly onRequestCreated: (event: RequestCreated) => Effect.Effect<void, SqlError>
  readonly onRequestUpdated: (event: RequestUpdated) => Effect.Effect<void, SqlError>
  readonly onRequestMetaDataCreated: (event: RequestMetaDataCreated) => Effect.Effect<void, SqlError>
  readonly onRequestSnapshotCreated: (event: RequestSnapshotCreated) => Effect.Effect<void, SqlError>
  readonly dispatch: (event: RequestProviderEvent) => Effect.Effect<void, SqlError>
}

export const makeDispatch = (handlers: Omit<ProjectionHandlers, "dispatch">) =>
  (event: RequestProviderEvent): Effect.Effect<void, SqlError> => {
    switch (event._tag) {
      case "RequestCreated": return handlers.onRequestCreated(event)
      case "RequestUpdated": return handlers.onRequestUpdated(event)
      case "RequestMetaDataCreated": return handlers.onRequestMetaDataCreated(event)
      case "RequestSnapshotCreated": return handlers.onRequestSnapshotCreated(event)
    }
  }

export class RequestProviderProjectionStore extends Context.Tag("RequestProviderProjectionStore")<
  RequestProviderProjectionStore,
  ProjectionHandlers
>() {}
