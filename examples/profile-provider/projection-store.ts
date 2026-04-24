import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import type { SqlError } from "@effect/sql/SqlError"
import type {
  ProfileEvent,
  ProfileCreated,
  MergedDataProfile,
  SnapshotCreatedProfile,
  MetaDataCreated,
  PersonalDataExtracted,
  ProfileBranchForked,
  SnapshotPublishedProfile
} from "./contracts.js"

export interface ProjectionHandlers {
  readonly onProfileCreated: (event: ProfileCreated) => Effect.Effect<void, SqlError>
  readonly onMergedDataProfile: (event: MergedDataProfile) => Effect.Effect<void, SqlError>
  readonly onSnapshotCreatedProfile: (event: SnapshotCreatedProfile) => Effect.Effect<void, SqlError>
  readonly onMetaDataCreated: (event: MetaDataCreated) => Effect.Effect<void, SqlError>
  readonly onPersonalDataExtracted: (event: PersonalDataExtracted) => Effect.Effect<void, SqlError>
  readonly onProfileBranchForked: (event: ProfileBranchForked) => Effect.Effect<void, SqlError>
  readonly onSnapshotPublishedProfile: (event: SnapshotPublishedProfile) => Effect.Effect<void, SqlError>
  readonly dispatch: (event: ProfileEvent) => Effect.Effect<void, SqlError>
}

/** Build a dispatch function from per-event handlers. Exhaustive — adding a new event forces a handler. */
export const makeDispatch = (handlers: Omit<ProjectionHandlers, "dispatch">) =>
  (event: ProfileEvent): Effect.Effect<void, SqlError> => {
    switch (event._tag) {
      case "ProfileCreated": return handlers.onProfileCreated(event)
      case "MergedDataProfile": return handlers.onMergedDataProfile(event)
      case "SnapshotCreatedProfile": return handlers.onSnapshotCreatedProfile(event)
      case "MetaDataCreated": return handlers.onMetaDataCreated(event)
      case "PersonalDataExtracted": return handlers.onPersonalDataExtracted(event)
      case "ProfileBranchForked": return handlers.onProfileBranchForked(event)
      case "SnapshotPublishedProfile": return handlers.onSnapshotPublishedProfile(event)
    }
    const _exhaustive: never = event
    return _exhaustive
  }

export class ProfileProviderProjectionStore extends Context.Tag("ProfileProviderProjectionStore")<
  ProfileProviderProjectionStore,
  ProjectionHandlers
>() {}
