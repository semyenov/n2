/**
 * Profile provider contracts — pure schemas, no framework helpers.
 *
 * Commands are Schema.TaggedRequest: they carry success/failure types and serve
 * as both the domain command and the RPC schema. No separate transport DTOs.
 *
 * Payload reuse: the Entity.make calls use `CommandClass.fields` as the Rpc payload
 * rather than re-declaring each field inline. This avoids duplication between the
 * TaggedRequest and the wire schema while keeping the two in sync.
 *
 * Read queries (GetProfile, GetProfileHistory) are included in the entity protocol
 * but excluded from ClusterSchema.Persisted — they produce no events and require
 * no journal writes.
 */
import * as Schema from "effect/Schema"
import { ClusterSchema, Entity } from "@effect/cluster"
import { Rpc } from "@effect/rpc"

export class SourceAsset extends Schema.Class<SourceAsset>("SourceAsset")({
  sourceId: Schema.String,
  kind: Schema.Literal("file", "url", "manual", "parsed"),
  uri: Schema.String,
  mediaType: Schema.String,
  storageKey: Schema.String,
  summary: Schema.String
}) {}

export class ProfileBranch extends Schema.Class<ProfileBranch>("ProfileBranch")({
  branchId: Schema.String,
  label: Schema.String,
  baseBranchId: Schema.String,
  baseRevision: Schema.Number.pipe(Schema.int()),
  baseSnapshotId: Schema.String,
  createdAt: Schema.DateTimeUtc,
  createdBy: Schema.String
}) {}

export class ProfileRevisionEntry extends Schema.Class<ProfileRevisionEntry>("ProfileRevisionEntry")({
  revision: Schema.Number.pipe(Schema.int()),
  branchId: Schema.String,
  eventType: Schema.String,
  summary: Schema.String,
  occurredAt: Schema.DateTimeUtc,
  actorId: Schema.String
}) {}

export const SnapshotType = Schema.Literal("AUTO", "MANUAL", "MIGRATION", "LLM")
export type SnapshotType = typeof SnapshotType.Type

export class ProfileSnapshot extends Schema.Class<ProfileSnapshot>("ProfileSnapshot")({
  snapshotId: Schema.String,
  branchId: Schema.String,
  revision: Schema.Number.pipe(Schema.int()),
  snapshotType: SnapshotType,
  profileJson: Schema.String,
  metadataJson: Schema.String,
  schemaVersion: Schema.String,
  summary: Schema.String,
  createdAt: Schema.DateTimeUtc,
  createdBy: Schema.String,
  published: Schema.Boolean,
  strategyJson: Schema.String
}) {}

export class ProfileCreated extends Schema.TaggedClass<ProfileCreated>()(
  "ProfileCreated",
  {
    profileId: Schema.String,
    ownerAgentId: Schema.String,
    branchId: Schema.String,
    schemaVersion: Schema.String,
    maskedProfileJson: Schema.String,
    createdAt: Schema.DateTimeUtc,
    createdBy: Schema.String,
    summary: Schema.String,
    revision: Schema.Number.pipe(Schema.int())
  }
) {}

export class MergedDataProfile extends Schema.TaggedClass<MergedDataProfile>()(
  "MergedDataProfile",
  {
    profileId: Schema.String,
    branchId: Schema.String,
    schemaVersion: Schema.String,
    maskedProfileJson: Schema.String,
    mergedAt: Schema.DateTimeUtc,
    mergedBy: Schema.String,
    summary: Schema.String,
    sourceCount: Schema.Number.pipe(Schema.int()),
    revision: Schema.Number.pipe(Schema.int())
  }
) {}

export class SnapshotCreatedProfile extends Schema.TaggedClass<SnapshotCreatedProfile>()(
  "SnapshotCreatedProfile",
  {
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
  }
) {}

export const MetadataScope = Schema.Literal("aggregate", "snapshot", "publish", "branch")
export type MetadataScope = typeof MetadataScope.Type

export class MetaDataCreated extends Schema.TaggedClass<MetaDataCreated>()(
  "MetaDataCreated",
  {
    profileId: Schema.String,
    branchId: Schema.String,
    scope: MetadataScope,
    scopeId: Schema.String,
    metadataJson: Schema.String,
    schemaVersion: Schema.String,
    createdAt: Schema.DateTimeUtc,
    createdBy: Schema.String,
    revision: Schema.Number.pipe(Schema.int())
  }
) {}

export class PersonalDataExtracted extends Schema.TaggedClass<PersonalDataExtracted>()(
  "PersonalDataExtracted",
  {
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
  }
) {}

export class ProfileBranchForked extends Schema.TaggedClass<ProfileBranchForked>()(
  "ProfileBranchForked",
  {
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
  }
) {}

export class SnapshotPublishedProfile extends Schema.TaggedClass<SnapshotPublishedProfile>()(
  "SnapshotPublishedProfile",
  {
    profileId: Schema.String,
    snapshotId: Schema.String,
    strategyJson: Schema.String,
    publishedAt: Schema.DateTimeUtc,
    publishedBy: Schema.String,
    revision: Schema.Number.pipe(Schema.int())
  }
) {}

export const ProfileEvent = Schema.Union(
  ProfileCreated,
  MergedDataProfile,
  SnapshotCreatedProfile,
  MetaDataCreated,
  PersonalDataExtracted,
  ProfileBranchForked,
  SnapshotPublishedProfile
)
export type ProfileEvent = typeof ProfileEvent.Type

export class ProfileError extends Schema.TaggedError<ProfileError>()(
  "ProfileError",
  { message: Schema.String }
) {}

export class ProfileNotFound extends Schema.TaggedError<ProfileNotFound>()(
  "ProfileNotFound",
  { profileId: Schema.String }
) {}

export class ProfileHistory extends Schema.Class<ProfileHistory>("ProfileHistory")({
  profileId: Schema.String,
  activeBranchId: Schema.String,
  currentRevision: Schema.Number.pipe(Schema.int()),
  publishedSnapshotId: Schema.String,
  branches: Schema.Array(ProfileBranch),
  revisions: Schema.Array(ProfileRevisionEntry),
  snapshots: Schema.Array(ProfileSnapshot)
}) {}

export const ProfileStatus = Schema.Literal("empty", "draft", "published")
export type ProfileStatus = typeof ProfileStatus.Type

export class ProfileState extends Schema.Class<ProfileState>("ProfileState")({
  status: ProfileStatus,
  profileId: Schema.String,
  ownerAgentId: Schema.String,
  activeBranchId: Schema.String,
  currentSchemaVersion: Schema.String,
  maskedProfileJson: Schema.String,
  latestMetadataJson: Schema.String,
  latestPiiStorageKey: Schema.String,
  piiJurisdiction: Schema.String,
  sourceAssets: Schema.Array(SourceAsset),
  branches: Schema.Array(ProfileBranch),
  revisions: Schema.Array(ProfileRevisionEntry),
  snapshots: Schema.Array(ProfileSnapshot),
  publishedSnapshotId: Schema.String,
  revision: Schema.Number.pipe(Schema.int())
}) {}

export const initialProfileState = new ProfileState({
  status: "empty",
  profileId: "",
  ownerAgentId: "",
  activeBranchId: "",
  currentSchemaVersion: "",
  maskedProfileJson: "{}",
  latestMetadataJson: "{}",
  latestPiiStorageKey: "",
  piiJurisdiction: "",
  sourceAssets: [],
  branches: [],
  revisions: [],
  snapshots: [],
  publishedSnapshotId: "",
  revision: 0
})

export class CommandResult extends Schema.Class<CommandResult>("CommandResult")({
  profileId: Schema.String,
  branchId: Schema.String,
  revision: Schema.Number.pipe(Schema.int())
}) {}

export class CreateProfile extends Schema.TaggedRequest<CreateProfile>("CreateProfile")(
  "CreateProfile",
  {
    failure: ProfileError,
    success: CommandResult,
    payload: {
      profileId: Schema.String,
      ownerAgentId: Schema.String,
      branchId: Schema.String,
      schemaVersion: Schema.String,
      maskedProfileJson: Schema.String,
      metadataJson: Schema.String,
      piiStorageKey: Schema.String,
      piiJson: Schema.String,
      piiJurisdiction: Schema.String,
      actorId: Schema.String,
      summary: Schema.String,
      sources: Schema.Array(SourceAsset)
    }
  }
) {}

export class MergeProfileData extends Schema.TaggedRequest<MergeProfileData>("MergeProfileData")(
  "MergeProfileData",
  {
    failure: ProfileError,
    success: CommandResult,
    payload: {
      profileId: Schema.String,
      branchId: Schema.String,
      schemaVersion: Schema.String,
      maskedProfileJson: Schema.String,
      metadataJson: Schema.String,
      piiStorageKey: Schema.String,
      piiJson: Schema.String,
      piiJurisdiction: Schema.String,
      actorId: Schema.String,
      summary: Schema.String,
      sources: Schema.Array(SourceAsset)
    }
  }
) {}

export class ForkProfileBranch extends Schema.TaggedRequest<ForkProfileBranch>("ForkProfileBranch")(
  "ForkProfileBranch",
  {
    failure: ProfileError,
    success: CommandResult,
    payload: {
      profileId: Schema.String,
      branchId: Schema.String,
      label: Schema.String,
      baseBranchId: Schema.String,
      baseRevision: Schema.Number.pipe(Schema.int()),
      baseSnapshotId: Schema.String,
      metadataJson: Schema.String,
      schemaVersion: Schema.String,
      actorId: Schema.String,
      summary: Schema.String
    }
  }
) {}

export class CreateProfileSnapshot extends Schema.TaggedRequest<CreateProfileSnapshot>("CreateProfileSnapshot")(
  "CreateProfileSnapshot",
  {
    failure: ProfileError,
    success: CommandResult,
    payload: {
      profileId: Schema.String,
      branchId: Schema.String,
      snapshotId: Schema.String,
      snapshotType: SnapshotType,
      schemaVersion: Schema.String,
      profileJson: Schema.String,
      metadataJson: Schema.String,
      piiStorageKey: Schema.String,
      piiJson: Schema.String,
      piiJurisdiction: Schema.String,
      actorId: Schema.String,
      summary: Schema.String
    }
  }
) {}

export class PublishProfileSnapshot extends Schema.TaggedRequest<PublishProfileSnapshot>("PublishProfileSnapshot")(
  "PublishProfileSnapshot",
  {
    failure: ProfileError,
    success: CommandResult,
    payload: {
      profileId: Schema.String,
      snapshotId: Schema.String,
      strategyJson: Schema.String,
      metadataJson: Schema.String,
      schemaVersion: Schema.String,
      actorId: Schema.String
    }
  }
) {}

export class GetProfile extends Schema.TaggedRequest<GetProfile>("GetProfile")(
  "GetProfile",
  {
    failure: ProfileNotFound,
    success: ProfileState,
    payload: { profileId: Schema.String }
  }
) {}

export class GetProfileHistory extends Schema.TaggedRequest<GetProfileHistory>("GetProfileHistory")(
  "GetProfileHistory",
  {
    failure: ProfileNotFound,
    success: ProfileHistory,
    payload: { profileId: Schema.String }
  }
) {}

export const ProfileCommandSchema = Schema.Union(
  CreateProfile,
  MergeProfileData,
  ForkProfileBranch,
  CreateProfileSnapshot,
  PublishProfileSnapshot,
  GetProfile,
  GetProfileHistory
)
export type ProfileCommand = typeof ProfileCommandSchema.Type

export const ProfileProviderEntity = Entity.make("ProfileProvider", [
  Rpc.make("CreateProfile", {
    payload: CreateProfile.fields,
    primaryKey: (p) => p.profileId,
    success: CommandResult,
    error: ProfileError
  }),
  Rpc.make("MergeProfileData", {
    payload: MergeProfileData.fields,
    primaryKey: (p) => p.profileId,
    success: CommandResult,
    error: ProfileError
  }),
  Rpc.make("ForkProfileBranch", {
    payload: ForkProfileBranch.fields,
    primaryKey: (p) => p.profileId,
    success: CommandResult,
    error: ProfileError
  }),
  Rpc.make("CreateProfileSnapshot", {
    payload: CreateProfileSnapshot.fields,
    primaryKey: (p) => p.profileId,
    success: CommandResult,
    error: ProfileError
  }),
  Rpc.make("PublishProfileSnapshot", {
    payload: PublishProfileSnapshot.fields,
    primaryKey: (p) => p.profileId,
    success: CommandResult,
    error: ProfileError
  }),
  Rpc.make("GetProfile", {
    payload: GetProfile.fields,
    primaryKey: (p) => p.profileId,
    success: ProfileState,
    error: ProfileNotFound
  }),
  Rpc.make("GetProfileHistory", {
    payload: GetProfileHistory.fields,
    primaryKey: (p) => p.profileId,
    success: ProfileHistory,
    error: ProfileNotFound
  })
]).annotateRpcs(ClusterSchema.Persisted, true)

export const ProfileProviderRpcs = ProfileProviderEntity.protocol
