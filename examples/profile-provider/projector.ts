import * as Effect from "effect/Effect"
import { EventLog } from "@effect/experimental"
import type { ProfileEvent } from "./contracts.js"
import {
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

/** Exhaustive, type-safe extraction of the business timestamp from each event. */
const occurredAt = (event: ProfileEvent): string => {
  switch (event._tag) {
    case "ProfileCreated": return String(event.createdAt.toJSON())
    case "MergedDataProfile": return String(event.mergedAt.toJSON())
    case "SnapshotCreatedProfile": return String(event.createdAt.toJSON())
    case "MetaDataCreated": return String(event.createdAt.toJSON())
    case "PersonalDataExtracted": return String(event.extractedAt.toJSON())
    case "ProfileBranchForked": return String(event.createdAt.toJSON())
    case "SnapshotPublishedProfile": return String(event.publishedAt.toJSON())
  }
}

const makeMessage = (event: ProfileEvent) =>
  makeProfileProviderEventMessage({
    profileId: event.profileId,
    revision: event.revision,
    eventType: event._tag,
    occurredAt: occurredAt(event),
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
