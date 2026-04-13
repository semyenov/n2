/**
 * Profile provider EventGroup — bridges domain events to the @effect/experimental EventLog system.
 *
 * EventGroup.empty.add(...) defines each event's tag, primaryKey, and payload schema.
 * The primaryKey determines which EventLog stream an event belongs to (one per profileId).
 *
 * This is separate from contracts.ts because EventGroup uses plain Schema.Struct fields
 * rather than Schema.TaggedClass instances. The payloads are structurally identical —
 * just not the same class objects.
 *
 * MetadataScope and SnapshotType are imported from contracts.ts (not redefined here)
 * so both files stay in sync on a single source of truth.
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
import { MetadataScope, SnapshotType } from "./contracts.js"

export const ProfileProviderEventGroup = EventGroup.empty
  .add({
    tag: "ProfileCreated",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: Schema.Struct({
      profileId: Schema.String,
      ownerAgentId: Schema.String,
      branchId: Schema.String,
      schemaVersion: Schema.String,
      maskedProfileJson: Schema.String,
      createdAt: Schema.DateTimeUtc,
      createdBy: Schema.String,
      summary: Schema.String,
      revision: Schema.Number.pipe(Schema.int())
    })
  })
  .add({
    tag: "MergedDataProfile",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: Schema.Struct({
      profileId: Schema.String,
      branchId: Schema.String,
      schemaVersion: Schema.String,
      maskedProfileJson: Schema.String,
      mergedAt: Schema.DateTimeUtc,
      mergedBy: Schema.String,
      summary: Schema.String,
      sourceCount: Schema.Number.pipe(Schema.int()),
      revision: Schema.Number.pipe(Schema.int())
    })
  })
  .add({
    tag: "SnapshotCreatedProfile",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: Schema.Struct({
      profileId: Schema.String,
      branchId: Schema.String,
      snapshotId: Schema.String,
      snapshotType: SnapshotType,
      profileJson: Schema.String,
      metadataJson: Schema.String,
      schemaVersion: Schema.String,
      createdAt: Schema.DateTimeUtc,
      createdBy: Schema.String,
      summary: Schema.String,
      revision: Schema.Number.pipe(Schema.int())
    })
  })
  .add({
    tag: "MetaDataCreated",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: Schema.Struct({
      profileId: Schema.String,
      branchId: Schema.String,
      scope: MetadataScope,
      scopeId: Schema.String,
      metadataJson: Schema.String,
      schemaVersion: Schema.String,
      createdAt: Schema.DateTimeUtc,
      createdBy: Schema.String,
      revision: Schema.Number.pipe(Schema.int())
    })
  })
  .add({
    tag: "PersonalDataExtracted",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: Schema.Struct({
      profileId: Schema.String,
      branchId: Schema.String,
      scope: MetadataScope,
      scopeId: Schema.String,
      piiStorageKey: Schema.String,
      piiJson: Schema.String,
      jurisdiction: Schema.String,
      extractedAt: Schema.DateTimeUtc,
      extractedBy: Schema.String,
      revision: Schema.Number.pipe(Schema.int())
    })
  })
  .add({
    tag: "ProfileBranchForked",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: Schema.Struct({
      profileId: Schema.String,
      branchId: Schema.String,
      label: Schema.String,
      baseBranchId: Schema.String,
      baseRevision: Schema.Number.pipe(Schema.int()),
      baseSnapshotId: Schema.String,
      createdAt: Schema.DateTimeUtc,
      createdBy: Schema.String,
      summary: Schema.String,
      revision: Schema.Number.pipe(Schema.int())
    })
  })
  .add({
    tag: "SnapshotPublishedProfile",
    primaryKey: (p: { profileId: string }) => p.profileId,
    payload: Schema.Struct({
      profileId: Schema.String,
      snapshotId: Schema.String,
      strategyJson: Schema.String,
      publishedAt: Schema.DateTimeUtc,
      publishedBy: Schema.String,
      revision: Schema.Number.pipe(Schema.int())
    })
  })

export const ProfileProviderEventLogSchema = EventLogApi.schema(ProfileProviderEventGroup)
