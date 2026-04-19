import * as Effect from "effect/Effect"
import { EventLog } from "@effect/experimental"
import {
  type ProfileEvent,
  eventOccurredAt,
  ProfileCreated,
  MergedDataProfile,
  SnapshotCreatedProfile,
  MetaDataCreated,
  PersonalDataExtracted,
  ProfileBranchForked,
  SnapshotPublishedProfile
} from "./contracts.js"
import { ProfileProviderProjectionStore } from "./projection-store.js"
import { ProfileProviderOutbox } from "./outbox.js"
import { ProfileProviderEventGroup } from "./events.js"
import { makeProfileProviderEventMessage } from "./workflows.js"

const makeMessage = (event: ProfileEvent) =>
  makeProfileProviderEventMessage({
    profileId: event.profileId,
    revision: event.revision,
    eventType: event._tag,
    occurredAt: eventOccurredAt(event),
    payload: event
  })

export const ProfileProviderProjectionLayer = EventLog.group(
  ProfileProviderEventGroup,
  (handlers) =>
    handlers
      .handle("ProfileCreated", ({ payload }) =>
        Effect.gen(function* () {
          const store = yield* ProfileProviderProjectionStore
          const outbox = yield* ProfileProviderOutbox
          const event = new ProfileCreated(payload)
          yield* store.onProfileCreated(event)
          yield* outbox.enqueue(makeMessage(event))
        }).pipe(Effect.orDie)
      )
      .handle("MergedDataProfile", ({ payload }) =>
        Effect.gen(function* () {
          const store = yield* ProfileProviderProjectionStore
          const outbox = yield* ProfileProviderOutbox
          const event = new MergedDataProfile(payload)
          yield* store.onMergedDataProfile(event)
          yield* outbox.enqueue(makeMessage(event))
        }).pipe(Effect.orDie)
      )
      .handle("SnapshotCreatedProfile", ({ payload }) =>
        Effect.gen(function* () {
          const store = yield* ProfileProviderProjectionStore
          const outbox = yield* ProfileProviderOutbox
          const event = new SnapshotCreatedProfile(payload)
          yield* store.onSnapshotCreatedProfile(event)
          yield* outbox.enqueue(makeMessage(event))
        }).pipe(Effect.orDie)
      )
      .handle("MetaDataCreated", ({ payload }) =>
        Effect.gen(function* () {
          const store = yield* ProfileProviderProjectionStore
          const outbox = yield* ProfileProviderOutbox
          const event = new MetaDataCreated(payload)
          yield* store.onMetaDataCreated(event)
          yield* outbox.enqueue(makeMessage(event))
        }).pipe(Effect.orDie)
      )
      .handle("PersonalDataExtracted", ({ payload }) =>
        Effect.gen(function* () {
          const store = yield* ProfileProviderProjectionStore
          const outbox = yield* ProfileProviderOutbox
          const event = new PersonalDataExtracted(payload)
          yield* store.onPersonalDataExtracted(event)
          yield* outbox.enqueue(makeMessage(event))
        }).pipe(Effect.orDie)
      )
      .handle("ProfileBranchForked", ({ payload }) =>
        Effect.gen(function* () {
          const store = yield* ProfileProviderProjectionStore
          const outbox = yield* ProfileProviderOutbox
          const event = new ProfileBranchForked(payload)
          yield* store.onProfileBranchForked(event)
          yield* outbox.enqueue(makeMessage(event))
        }).pipe(Effect.orDie)
      )
      .handle("SnapshotPublishedProfile", ({ payload }) =>
        Effect.gen(function* () {
          const store = yield* ProfileProviderProjectionStore
          const outbox = yield* ProfileProviderOutbox
          const event = new SnapshotPublishedProfile(payload)
          yield* store.onSnapshotPublishedProfile(event)
          yield* outbox.enqueue(makeMessage(event))
        }).pipe(Effect.orDie)
      )
)
