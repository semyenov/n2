/**
 * Profile provider EventGroup — bridges domain events to the @effect/experimental EventLog system.
 *
 * Payload schemas are derived from the TaggedClass definitions in contracts.ts
 * via `fieldsOf()`, which strips the `_tag` discriminant and produces a plain
 * Schema.Struct. This keeps contracts.ts as the single source of truth — any
 * field change there automatically flows to the EventGroup.
 *
 * ProfileProviderEventLogSchema is exported as a single instance shared by:
 *   - entity.ts  → EventLogApi.makeClient(ProfileProviderEventLogSchema)  (publish)
 *   - layers.ts  → EventLogApi.layer(ProfileProviderEventLogSchema)        (dispatch)
 * Both sides must use the same object reference so EventLog can match published
 * events to the correct dispatch watcher.
 */
import { EventGroup } from "@effect/experimental"
import * as EventLogApi from "@effect/experimental/EventLog"
import * as N2 from "../../src/framework/helpers/index.js"
import {
  ProfileCreated,
  MergedDataProfile,
  SnapshotCreatedProfile,
  MetaDataCreated,
  PersonalDataExtracted,
  ProfileBranchForked,
  SnapshotPublishedProfile
} from "./contracts.js"

export const ProfileProviderEventGroup = EventGroup.empty
  .add({
    tag: "ProfileCreated",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: N2.eventPayloadSchema(ProfileCreated)
  })
  .add({
    tag: "MergedDataProfile",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: N2.eventPayloadSchema(MergedDataProfile)
  })
  .add({
    tag: "SnapshotCreatedProfile",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: N2.eventPayloadSchema(SnapshotCreatedProfile)
  })
  .add({
    tag: "MetaDataCreated",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: N2.eventPayloadSchema(MetaDataCreated)
  })
  .add({
    tag: "PersonalDataExtracted",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: N2.eventPayloadSchema(PersonalDataExtracted)
  })
  .add({
    tag: "ProfileBranchForked",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: N2.eventPayloadSchema(ProfileBranchForked)
  })
  .add({
    tag: "SnapshotPublishedProfile",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: N2.eventPayloadSchema(SnapshotPublishedProfile)
  })

export const ProfileProviderEventLogSchema = EventLogApi.schema(ProfileProviderEventGroup)
