import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import type { SqlError } from "@effect/sql/SqlError"
import type {
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
}

export class ProfileProviderProjectionStore extends Context.Tag("ProfileProviderProjectionStore")<
  ProfileProviderProjectionStore,
  ProjectionHandlers
>() {}
