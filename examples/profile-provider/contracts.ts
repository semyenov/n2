/**
 * Profile provider contracts — pure schemas, no framework helpers.
 *
 * Commands are Schema.TaggedRequest: they carry success/failure types and serve
 * as both the domain command and the RPC schema. No separate transport DTOs.
 *
 * The public RPC field names stay stable (`maskedProfileJson`, `profileJson`)
 * to avoid a wider API rename, but they now carry a typed ProfileDocument
 * instead of an opaque JSON string.
 */
import * as Schema from "effect/Schema"
import * as N2 from "../../src/framework/helpers/index.js"

const NonNegativeNumber = Schema.Number.pipe(Schema.greaterThanOrEqualTo(0))
const PositiveInt = Schema.Number.pipe(Schema.int(), Schema.greaterThanOrEqualTo(1))
const NonNegativeInt = Schema.Number.pipe(Schema.int(), Schema.greaterThanOrEqualTo(0))
const NonEmptyString = Schema.String.pipe(Schema.minLength(1))
const TwoCharString = Schema.String.pipe(Schema.minLength(2))
const E164PhoneNumber = Schema.String.pipe(Schema.pattern(/^\+[1-9]\d{1,14}$/))
const LooseObject = Schema.Record({ key: Schema.String, value: Schema.Unknown })
const ProfileId = Schema.UUID

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

export class PersonalInfo extends Schema.Class<PersonalInfo>("PersonalInfo")({
  first_name: TwoCharString,
  last_name: TwoCharString,
  gender: Schema.optional(Schema.String),
  birth_date: Schema.optional(Schema.String),
  citizenship: Schema.optional(Schema.String),
  residence: Schema.optional(Schema.String),
  relevant_position: NonEmptyString,
  relocation: Schema.optional(Schema.Boolean),
  photo_url: Schema.optional(Schema.String),
  is_employed: Schema.optional(Schema.Boolean)
}) {}

export class Contacts extends Schema.Class<Contacts>("Contacts")({
  phone: Schema.optional(Schema.NonEmptyArray(E164PhoneNumber)),
  email: Schema.optional(Schema.NonEmptyArray(Schema.String)),
  personal_website: Schema.optional(Schema.String),
  telegram: Schema.optional(Schema.String),
  linkedin: Schema.optional(Schema.String),
  github: Schema.optional(Schema.String),
  behance: Schema.optional(Schema.String),
  discord: Schema.optional(Schema.String),
  reddit: Schema.optional(Schema.String)
}) {}

export class SalaryExpectations extends Schema.Class<SalaryExpectations>("SalaryExpectations")({
  currency: Schema.String,
  amount_from: NonNegativeNumber,
  amount_to: Schema.optional(NonNegativeNumber)
}) {}

export class SkillEntry extends Schema.Class<SkillEntry>("SkillEntry")({
  name: TwoCharString,
  level: Schema.String,
  years_of_experience: Schema.optional(NonNegativeNumber)
}) {}

export class ToolProficiency extends Schema.Class<ToolProficiency>("ToolProficiency")({
  tool: NonEmptyString,
  level: Schema.String
}) {}

export class LanguageEntry extends Schema.Class<LanguageEntry>("LanguageEntry")({
  name: NonEmptyString,
  level: Schema.String
}) {}

export class EducationEntry extends Schema.Class<EducationEntry>("EducationEntry")({
  degree: Schema.String,
  field_of_study: Schema.String,
  institution: Schema.String,
  start_year: Schema.optional(NonNegativeInt),
  end_year: Schema.optional(NonNegativeInt),
  diploma_with_honors: Schema.optional(Schema.Boolean)
}) {}

export class OnlineCourse extends Schema.Class<OnlineCourse>("OnlineCourse")({
  title: NonEmptyString,
  platform: Schema.String,
  date_completed: Schema.optional(Schema.String)
}) {}

export class WorkExperienceEntry extends Schema.Class<WorkExperienceEntry>("WorkExperienceEntry")({
  position: NonEmptyString,
  company: NonEmptyString,
  about: Schema.optional(Schema.String),
  industry: Schema.optional(Schema.String),
  location: Schema.optional(Schema.String),
  start_date: Schema.String,
  job_format: Schema.optional(Schema.String),
  end_date: Schema.optionalWith(Schema.String, { nullable: true }),
  responsibilities: Schema.optional(Schema.Array(Schema.String)),
  achievements: Schema.optional(Schema.Array(Schema.String)),
  skills_used: Schema.optional(Schema.Array(Schema.String))
}) {}

export class PortfolioEntry extends Schema.Class<PortfolioEntry>("PortfolioEntry")({
  title: NonEmptyString,
  url: Schema.optional(Schema.String)
}) {}

export class UserData extends Schema.Class<UserData>("UserData")({
  personal_info: PersonalInfo,
  contacts: Schema.optional(Contacts),
  about: Schema.optional(Schema.String),
  achievements: Schema.optional(Schema.String),
  specialization: Schema.optional(Schema.Array(NonEmptyString)),
  salary_expectations: SalaryExpectations,
  employment_types: Schema.optional(Schema.Array(Schema.String)),
  work_schedule: Schema.optional(Schema.Array(Schema.String)),
  skills: Schema.Array(SkillEntry),
  tools_proficiency: Schema.optional(Schema.Array(ToolProficiency)),
  soft_skills: Schema.optional(Schema.Array(Schema.String)),
  languages: Schema.optional(Schema.Array(LanguageEntry)),
  education: Schema.Array(EducationEntry),
  online_courses: Schema.optional(Schema.Array(OnlineCourse)),
  work_experience: Schema.optional(Schema.Array(WorkExperienceEntry)),
  portfolio: Schema.optional(Schema.Array(PortfolioEntry))
}) {}

export class UserMatchingData extends Schema.Class<UserMatchingData>("UserMatchingData")({
  application_history: Schema.optional(Schema.Array(LooseObject)),
  interview_history: Schema.optional(Schema.Array(LooseObject)),
  feedback_history: Schema.optional(Schema.Array(LooseObject))
}) {}

export class UserMetaData extends Schema.Class<UserMetaData>("UserMetaData")({
  last_updated: Schema.optional(Schema.String),
  source_platform: Schema.optional(Schema.String),
  notes: Schema.optional(Schema.String),
  tags: Schema.optional(Schema.Array(Schema.String)),
  version: PositiveInt
}) {}

export class ProfileDocument extends Schema.Class<ProfileDocument>("ProfileDocument")({
  uuid: Schema.UUID,
  created_at: Schema.String,
  updated_at: Schema.optional(Schema.String),
  user_data: UserData,
  user_matching_data: Schema.optional(UserMatchingData),
  user_meta_data: UserMetaData
}) {}

export const SnapshotType = Schema.Literal("AUTO", "MANUAL", "MIGRATION", "LLM")
export type SnapshotType = typeof SnapshotType.Type

export class ProfileSnapshot extends Schema.Class<ProfileSnapshot>("ProfileSnapshot")({
  snapshotId: Schema.String,
  branchId: Schema.String,
  revision: Schema.Number.pipe(Schema.int()),
  snapshotType: SnapshotType,
  profileJson: ProfileDocument,
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
    profileId: ProfileId,
    ownerAgentId: Schema.String,
    branchId: Schema.String,
    schemaVersion: Schema.String,
    maskedProfileJson: ProfileDocument,
    createdAt: Schema.DateTimeUtc,
    createdBy: Schema.String,
    summary: Schema.String,
    revision: Schema.Number.pipe(Schema.int())
  }
) {}

export class MergedDataProfile extends Schema.TaggedClass<MergedDataProfile>()(
  "MergedDataProfile",
  {
    profileId: ProfileId,
    branchId: Schema.String,
    schemaVersion: Schema.String,
    maskedProfileJson: ProfileDocument,
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
    profileId: ProfileId,
    branchId: Schema.String,
    snapshotId: Schema.String,
    snapshotType: SnapshotType,
    profileJson: ProfileDocument,
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
    profileId: ProfileId,
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
    profileId: ProfileId,
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
    profileId: ProfileId,
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
    profileId: ProfileId,
    snapshotId: Schema.String,
    strategyJson: Schema.String,
    publishedAt: Schema.DateTimeUtc,
    publishedBy: Schema.String,
    revision: Schema.Number.pipe(Schema.int())
  }
) {}

const ProfileEvents = N2.defineEvents(
  ProfileCreated,
  MergedDataProfile,
  SnapshotCreatedProfile,
  MetaDataCreated,
  PersonalDataExtracted,
  ProfileBranchForked,
  SnapshotPublishedProfile
)

export const ProfileEvent = ProfileEvents.schema
export type ProfileEvent = typeof ProfileEvent.Type

/** Per-event metadata: which field holds the business timestamp. */
export const ProfileEventMeta: {
  readonly [K in ProfileEvent["_tag"]]: { readonly occurredAt: string }
} = {
  ProfileCreated: { occurredAt: "createdAt" },
  MergedDataProfile: { occurredAt: "mergedAt" },
  SnapshotCreatedProfile: { occurredAt: "createdAt" },
  MetaDataCreated: { occurredAt: "createdAt" },
  PersonalDataExtracted: { occurredAt: "extractedAt" },
  ProfileBranchForked: { occurredAt: "createdAt" },
  SnapshotPublishedProfile: { occurredAt: "publishedAt" }
}

/** Extract the business timestamp (as ISO string) from any profile event. */
export const eventOccurredAt = (event: ProfileEvent): string => {
  const record = event as unknown as Record<string, { toJSON(): unknown }>
  return String(record[ProfileEventMeta[event._tag].occurredAt]!.toJSON())
}

export class ProfileError extends Schema.TaggedError<ProfileError>()(
  "ProfileError",
  { message: Schema.String }
) {}

export class ProfileNotFound extends Schema.TaggedError<ProfileNotFound>()(
  "ProfileNotFound",
  { profileId: ProfileId }
) {}

export class ProfileHistory extends Schema.Class<ProfileHistory>("ProfileHistory")({
  profileId: ProfileId,
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
  maskedProfileJson: Schema.NullOr(ProfileDocument),
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
  maskedProfileJson: null,
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
  profileId: ProfileId,
  branchId: Schema.String,
  revision: Schema.Number.pipe(Schema.int())
}) {}

export class CreateProfile extends Schema.TaggedRequest<CreateProfile>("CreateProfile")(
  "CreateProfile",
  {
    failure: ProfileError,
    success: CommandResult,
    payload: {
      profileId: ProfileId,
      ownerAgentId: Schema.String,
      branchId: Schema.String,
      schemaVersion: Schema.String,
      maskedProfileJson: ProfileDocument,
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
      profileId: ProfileId,
      branchId: Schema.String,
      schemaVersion: Schema.String,
      maskedProfileJson: ProfileDocument,
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
      profileId: ProfileId,
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
      profileId: ProfileId,
      branchId: Schema.String,
      snapshotId: Schema.String,
      snapshotType: SnapshotType,
      schemaVersion: Schema.String,
      profileJson: ProfileDocument,
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
      profileId: ProfileId,
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
    payload: { profileId: ProfileId }
  }
) {}

export class GetProfileHistory extends Schema.TaggedRequest<GetProfileHistory>("GetProfileHistory")(
  "GetProfileHistory",
  {
    failure: ProfileNotFound,
    success: ProfileHistory,
    payload: { profileId: ProfileId }
  }
) {}

export const ProfileProviderCommands = N2.defineCommands(
  CreateProfile,
  MergeProfileData,
  ForkProfileBranch,
  CreateProfileSnapshot,
  PublishProfileSnapshot,
  GetProfile,
  GetProfileHistory
)

export const ProfileCommandSchema = ProfileProviderCommands.schema
export type ProfileCommand = typeof ProfileCommandSchema.Type

export const ProfileProviderEntity = ProfileProviderCommands.toPersistedEntity(
  "ProfileProvider",
  (p) => p.profileId
)
export const ProfileProviderRpcs = ProfileProviderEntity.protocol
