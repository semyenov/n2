import * as Effect from "effect/Effect"
import { EventLog } from "@effect/experimental"
import {
  MergedDataProfile,
  MetaDataCreated,
  PersonalDataExtracted,
  ProfileBranchForked,
  ProfileCreated,
  SnapshotCreatedProfile,
  SnapshotPublishedProfile
} from "./contracts.js"
import { ProfileProviderProjectionStore } from "./projection-store.js"
import { ProfileProviderEventGroup } from "./events.js"
import {
  makeProfileProviderEventMessage,
  startProfileEventPublish
} from "./workflows.js"

export const ProfileProviderProjectionLayer = EventLog.group(
  ProfileProviderEventGroup,
  (handlers) =>
    handlers
      .handle("ProfileCreated", ({ payload }) =>
        Effect.gen(function* () {
          const store = yield* ProfileProviderProjectionStore
          const message = makeProfileProviderEventMessage({
            profileId: payload.profileId as string,
            revision: payload.revision,
            eventType: "ProfileCreated",
            occurredAt: payload.createdAt.toJSON() as string,
            payload
          })
          yield* store.project(new ProfileCreated(payload), message)
          yield* startProfileEventPublish(message).pipe(Effect.asVoid)
        }).pipe(Effect.orDie)
      )
      .handle("MergedDataProfile", ({ payload }) =>
        Effect.gen(function* () {
          const store = yield* ProfileProviderProjectionStore
          const message = makeProfileProviderEventMessage({
            profileId: payload.profileId as string,
            revision: payload.revision,
            eventType: "MergedDataProfile",
            occurredAt: payload.mergedAt.toJSON() as string,
            payload
          })
          yield* store.project(new MergedDataProfile(payload), message)
          yield* startProfileEventPublish(message).pipe(Effect.asVoid)
        }).pipe(Effect.orDie)
      )
      .handle("SnapshotCreatedProfile", ({ payload }) =>
        Effect.gen(function* () {
          const store = yield* ProfileProviderProjectionStore
          const message = makeProfileProviderEventMessage({
            profileId: payload.profileId as string,
            revision: payload.revision,
            eventType: "SnapshotCreatedProfile",
            occurredAt: payload.createdAt.toJSON() as string,
            payload
          })
          yield* store.project(new SnapshotCreatedProfile(payload), message)
          yield* startProfileEventPublish(message).pipe(Effect.asVoid)
        }).pipe(Effect.orDie)
      )
      .handle("MetaDataCreated", ({ payload }) =>
        Effect.gen(function* () {
          const store = yield* ProfileProviderProjectionStore
          const message = makeProfileProviderEventMessage({
            profileId: payload.profileId as string,
            revision: payload.revision,
            eventType: "MetaDataCreated",
            occurredAt: payload.createdAt.toJSON() as string,
            payload
          })
          yield* store.project(new MetaDataCreated(payload), message)
          yield* startProfileEventPublish(message).pipe(Effect.asVoid)
        }).pipe(Effect.orDie)
      )
      .handle("PersonalDataExtracted", ({ payload }) =>
        Effect.gen(function* () {
          const store = yield* ProfileProviderProjectionStore
          const message = makeProfileProviderEventMessage({
            profileId: payload.profileId as string,
            revision: payload.revision,
            eventType: "PersonalDataExtracted",
            occurredAt: payload.extractedAt.toJSON() as string,
            payload
          })
          yield* store.project(new PersonalDataExtracted(payload), message)
          yield* startProfileEventPublish(message).pipe(Effect.asVoid)
        }).pipe(Effect.orDie)
      )
      .handle("ProfileBranchForked", ({ payload }) =>
        Effect.gen(function* () {
          const store = yield* ProfileProviderProjectionStore
          const message = makeProfileProviderEventMessage({
            profileId: payload.profileId as string,
            revision: payload.revision,
            eventType: "ProfileBranchForked",
            occurredAt: payload.createdAt.toJSON() as string,
            payload
          })
          yield* store.project(new ProfileBranchForked(payload), message)
          yield* startProfileEventPublish(message).pipe(Effect.asVoid)
        }).pipe(Effect.orDie)
      )
      .handle("SnapshotPublishedProfile", ({ payload }) =>
        Effect.gen(function* () {
          const store = yield* ProfileProviderProjectionStore
          const message = makeProfileProviderEventMessage({
            profileId: payload.profileId as string,
            revision: payload.revision,
            eventType: "SnapshotPublishedProfile",
            occurredAt: payload.publishedAt.toJSON() as string,
            payload
          })
          yield* store.project(new SnapshotPublishedProfile(payload), message)
          yield* startProfileEventPublish(message).pipe(Effect.asVoid)
        }).pipe(Effect.orDie)
      )
)
