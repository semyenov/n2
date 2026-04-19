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
import * as Schema from "effect/Schema"
import { EventGroup } from "@effect/experimental"
import * as EventLogApi from "@effect/experimental/EventLog"
import {
  ProfileCreated,
  MergedDataProfile,
  SnapshotCreatedProfile,
  MetaDataCreated,
  PersonalDataExtracted,
  ProfileBranchForked,
  SnapshotPublishedProfile
} from "./contracts.js"

/** Extract payload fields from a TaggedClass (strips `_tag`), returning a Schema.Struct. */
const fieldsOf = <F extends Schema.Struct.Fields & { _tag: any }>(cls: { fields: F }) => {
  const { _tag: _, ...rest } = cls.fields
  return Schema.Struct(rest as { [K in Exclude<keyof F, "_tag">]: F[K] })
}

export const ProfileProviderEventGroup = EventGroup.empty
  .add({
    tag: "ProfileCreated",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: fieldsOf(ProfileCreated)
  })
  .add({
    tag: "MergedDataProfile",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: fieldsOf(MergedDataProfile)
  })
  .add({
    tag: "SnapshotCreatedProfile",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: fieldsOf(SnapshotCreatedProfile)
  })
  .add({
    tag: "MetaDataCreated",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: fieldsOf(MetaDataCreated)
  })
  .add({
    tag: "PersonalDataExtracted",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: fieldsOf(PersonalDataExtracted)
  })
  .add({
    tag: "ProfileBranchForked",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: fieldsOf(ProfileBranchForked)
  })
  .add({
    tag: "SnapshotPublishedProfile",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: fieldsOf(SnapshotPublishedProfile)
  })

export const ProfileProviderEventLogSchema = EventLogApi.schema(ProfileProviderEventGroup)
