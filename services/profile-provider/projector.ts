import { EventLog } from "@effect/experimental"
import { wireProjectionHandler } from "@semyenov/n2/helpers"
import {
  type ProfileEvent,
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
    occurredAt: String(event.occurredAt.toJSON()),
    payload: event
  })

export const ProfileProviderProjectionLayer = EventLog.group(
  ProfileProviderEventGroup,
  (handlers) =>
    handlers
      .handle("ProfileCreated", wireProjectionHandler(ProfileProviderProjectionStore, ProfileProviderOutbox, ProfileCreated, "onProfileCreated", makeMessage))
      .handle("MergedDataProfile", wireProjectionHandler(ProfileProviderProjectionStore, ProfileProviderOutbox, MergedDataProfile, "onMergedDataProfile", makeMessage))
      .handle("SnapshotCreatedProfile", wireProjectionHandler(ProfileProviderProjectionStore, ProfileProviderOutbox, SnapshotCreatedProfile, "onSnapshotCreatedProfile", makeMessage))
      .handle("MetaDataCreated", wireProjectionHandler(ProfileProviderProjectionStore, ProfileProviderOutbox, MetaDataCreated, "onMetaDataCreated", makeMessage))
      .handle("PersonalDataExtracted", wireProjectionHandler(ProfileProviderProjectionStore, ProfileProviderOutbox, PersonalDataExtracted, "onPersonalDataExtracted", makeMessage))
      .handle("ProfileBranchForked", wireProjectionHandler(ProfileProviderProjectionStore, ProfileProviderOutbox, ProfileBranchForked, "onProfileBranchForked", makeMessage))
      .handle("SnapshotPublishedProfile", wireProjectionHandler(ProfileProviderProjectionStore, ProfileProviderOutbox, SnapshotPublishedProfile, "onSnapshotPublishedProfile", makeMessage))
)
