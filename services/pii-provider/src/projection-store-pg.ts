import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { makeDispatch, PIIProviderProjectionStore, type ProjectionHandlers } from "./projection-store.js"

const handlers: Omit<ProjectionHandlers, "dispatch"> = {
  onPIIRecordCreated: () => Effect.void,
  onPIIRecordStoredFromProfile: () => Effect.void,
  onPIIRecordUpdated: () => Effect.void,
  onPIIConsentUpdated: () => Effect.void,
  onPIIConsentWithdrawn: () => Effect.void,
  onPIISubjectRequestCreated: () => Effect.void,
  onPIISubjectRequestCompleted: () => Effect.void,
  onPIIErasureRequested: () => Effect.void,
  onPIIErasureCompleted: () => Effect.void,
  onPIIKeyRotated: () => Effect.void,
  onPIIRecordAccessed: () => Effect.void
}

export const PIIProviderProjectionStorePgLive = Layer.succeed(
  PIIProviderProjectionStore,
  { ...handlers, dispatch: makeDispatch(handlers) }
)
