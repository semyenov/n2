import * as Schema from "effect/Schema"
import * as N2 from "@semyenov/n2/helpers"

const NonNegativeNumber = Schema.Number.pipe(Schema.greaterThanOrEqualTo(0))
const PositiveInt = Schema.Number.pipe(Schema.int(), Schema.greaterThanOrEqualTo(1))
const NonEmptyString = Schema.String.pipe(Schema.minLength(1))
const RequestId = Schema.UUID

export class SourceAsset extends Schema.Class<SourceAsset>("RequestSourceAsset")({
  sourceId: Schema.String,
  kind: Schema.Literal("file", "url", "manual", "parsed"),
  uri: Schema.String,
  mediaType: Schema.String,
  storageKey: Schema.String,
  summary: Schema.String
}) {}

export class HardSkillRequirement extends Schema.Class<HardSkillRequirement>("HardSkillRequirement")({
  name: NonEmptyString,
  level: Schema.String
}) {}

export class SoftSkillRequirement extends Schema.Class<SoftSkillRequirement>("SoftSkillRequirement")({
  name: NonEmptyString,
  required: Schema.Boolean
}) {}

export class LanguageRequirement extends Schema.Class<LanguageRequirement>("LanguageRequirement")({
  name: NonEmptyString,
  level: Schema.String,
  required: Schema.optional(Schema.Boolean)
}) {}

export class EducationRequirement extends Schema.Class<EducationRequirement>("EducationRequirement")({
  degree: Schema.String,
  field_of_study: Schema.String,
  institution: Schema.optional(Schema.String),
  required: Schema.optional(Schema.Boolean)
}) {}

export class CertificationRequirement extends Schema.Class<CertificationRequirement>("CertificationRequirement")({
  title: Schema.String,
  required: Schema.optional(Schema.Boolean)
}) {}

export class RequestRequirements extends Schema.Class<RequestRequirements>("RequestRequirements")({
  experience_years: NonNegativeNumber,
  hard_skills: Schema.Array(HardSkillRequirement),
  soft_skills: Schema.optional(Schema.Array(SoftSkillRequirement)),
  languages: Schema.optional(Schema.Array(LanguageRequirement)),
  education: Schema.optional(Schema.Array(EducationRequirement)),
  certifications: Schema.optional(Schema.Array(CertificationRequirement)),
  publications_required: Schema.optional(Schema.Boolean),
  open_source_contributions: Schema.optional(Schema.Boolean),
  team_leadership_experience: Schema.optional(Schema.Boolean),
  portfolio_required: Schema.optional(Schema.Boolean),
  media_appearances_preferred: Schema.optional(Schema.Boolean)
}) {}

export class SalaryOffer extends Schema.Class<SalaryOffer>("SalaryOffer")({
  currency: Schema.String,
  amount_from: NonNegativeNumber,
  amount_to: Schema.optional(NonNegativeNumber),
  bonus: Schema.optional(Schema.String)
}) {}

export class VacancyData extends Schema.Class<VacancyData>("VacancyData")({
  relevant_position: NonEmptyString,
  company: NonEmptyString,
  country: NonEmptyString,
  location: NonEmptyString,
  job_format: Schema.optional(Schema.String),
  industry: Schema.optional(Schema.String),
  description: Schema.optional(Schema.String),
  technology_stack: Schema.optional(Schema.Array(NonEmptyString)),
  responsibilities: Schema.optional(Schema.Array(NonEmptyString)),
  requirements: Schema.optional(RequestRequirements),
  salary_offer: Schema.optional(SalaryOffer),
  employment_types: Schema.optional(Schema.Array(Schema.String)),
  work_schedule: Schema.optional(Schema.Array(Schema.String)),
  benefits: Schema.optional(Schema.Array(Schema.String)),
  red_flags: Schema.optional(Schema.Array(Schema.String))
}) {}

export class VacancyMetaData extends Schema.Class<VacancyMetaData>("VacancyMetaData")({
  last_updated: Schema.optional(Schema.String),
  source_platform: Schema.optional(Schema.String),
  notes: Schema.optional(Schema.String),
  tags: Schema.optional(Schema.Array(Schema.String)),
  version: PositiveInt
}) {}

export class RequestDocument extends Schema.Class<RequestDocument>("RequestDocument")({
  id: PositiveInt,
  uuid: Schema.UUID,
  created_at: Schema.String,
  updated_at: Schema.optional(Schema.String),
  vacancy_data: VacancyData,
  vacancy_meta_data: VacancyMetaData
}) {}

export const SnapshotType = Schema.Literal("AUTO", "MANUAL", "MIGRATION", "LLM")
export type SnapshotType = typeof SnapshotType.Type

export class RequestSnapshot extends Schema.Class<RequestSnapshot>("RequestSnapshot")({
  snapshotId: Schema.String,
  revision: Schema.Number.pipe(Schema.int()),
  snapshotType: SnapshotType,
  requestJson: RequestDocument,
  metadataJson: Schema.String,
  schemaVersion: Schema.String,
  summary: Schema.String,
  createdAt: Schema.DateTimeUtc,
  createdBy: Schema.String
}) {}

export class RequestRevisionEntry extends Schema.Class<RequestRevisionEntry>("RequestRevisionEntry")({
  revision: Schema.Number.pipe(Schema.int()),
  eventType: Schema.String,
  summary: Schema.String,
  occurredAt: Schema.DateTimeUtc,
  actorId: Schema.String
}) {}

const RequestEventBase = {
  requestId: RequestId,
  occurredAt: Schema.DateTimeUtc,
  actorId: Schema.String,
  revision: Schema.Number.pipe(Schema.int())
}

export class RequestCreated extends Schema.TaggedClass<RequestCreated>()(
  "RequestCreated",
  {
    ...RequestEventBase,
    clientId: Schema.String,
    schemaVersion: Schema.String,
    requestJson: RequestDocument,
    summary: Schema.String,
    sourceCount: Schema.Number.pipe(Schema.int())
  }
) {}

export class RequestUpdated extends Schema.TaggedClass<RequestUpdated>()(
  "RequestUpdated",
  {
    ...RequestEventBase,
    schemaVersion: Schema.String,
    requestJson: RequestDocument,
    summary: Schema.String,
    sourceCount: Schema.Number.pipe(Schema.int())
  }
) {}

export const MetadataScope = Schema.Literal("aggregate", "snapshot")
export type MetadataScope = typeof MetadataScope.Type

export class RequestMetaDataCreated extends Schema.TaggedClass<RequestMetaDataCreated>()(
  "RequestMetaDataCreated",
  {
    ...RequestEventBase,
    scope: MetadataScope,
    scopeId: Schema.String,
    metadataJson: Schema.String,
    schemaVersion: Schema.String
  }
) {}

export class RequestSnapshotCreated extends Schema.TaggedClass<RequestSnapshotCreated>()(
  "RequestSnapshotCreated",
  {
    ...RequestEventBase,
    snapshotId: Schema.String,
    snapshotType: SnapshotType,
    requestJson: RequestDocument,
    metadataJson: Schema.String,
    schemaVersion: Schema.String,
    summary: Schema.String
  }
) {}

export const RequestProviderEvents = N2.defineEvents(
  RequestCreated,
  RequestUpdated,
  RequestMetaDataCreated,
  RequestSnapshotCreated
)

export const RequestProviderEvent = RequestProviderEvents.schema
export type RequestProviderEvent = typeof RequestProviderEvent.Type

export class RequestError extends Schema.TaggedError<RequestError>()(
  "RequestError",
  { message: Schema.String }
) {}

export class RequestNotFound extends Schema.TaggedError<RequestNotFound>()(
  "RequestNotFound",
  { requestId: RequestId }
) {}

export const RequestStatus = Schema.Literal("empty", "draft", "snapshotted")
export type RequestStatus = typeof RequestStatus.Type

export class RequestState extends Schema.Class<RequestState>("RequestState")({
  status: RequestStatus,
  requestId: Schema.String,
  clientId: Schema.String,
  currentSchemaVersion: Schema.String,
  requestJson: Schema.NullOr(RequestDocument),
  latestMetadataJson: Schema.String,
  sourceAssets: Schema.Array(SourceAsset),
  revisions: Schema.Array(RequestRevisionEntry),
  snapshots: Schema.Array(RequestSnapshot),
  latestSnapshotId: Schema.String,
  revision: Schema.Number.pipe(Schema.int())
}) {}

export const initialRequestState = new RequestState({
  status: "empty",
  requestId: "",
  clientId: "",
  currentSchemaVersion: "",
  requestJson: null,
  latestMetadataJson: "{}",
  sourceAssets: [],
  revisions: [],
  snapshots: [],
  latestSnapshotId: "",
  revision: 0
})

export class RequestHistory extends Schema.Class<RequestHistory>("RequestHistory")({
  requestId: RequestId,
  currentRevision: Schema.Number.pipe(Schema.int()),
  latestSnapshotId: Schema.String,
  revisions: Schema.Array(RequestRevisionEntry),
  snapshots: Schema.Array(RequestSnapshot)
}) {}

export class CommandResult extends Schema.Class<CommandResult>("RequestCommandResult")({
  requestId: RequestId,
  revision: Schema.Number.pipe(Schema.int()),
  latestSnapshotId: Schema.String
}) {}

export class CreateRequest extends Schema.TaggedRequest<CreateRequest>("CreateRequest")(
  "CreateRequest",
  {
    failure: RequestError,
    success: CommandResult,
    payload: {
      requestId: RequestId,
      clientId: Schema.String,
      schemaVersion: Schema.String,
      requestJson: RequestDocument,
      metadataJson: Schema.String,
      actorId: Schema.String,
      summary: Schema.String,
      sources: Schema.Array(SourceAsset)
    }
  }
) {}

export class UpdateRequest extends Schema.TaggedRequest<UpdateRequest>("UpdateRequest")(
  "UpdateRequest",
  {
    failure: RequestError,
    success: CommandResult,
    payload: {
      requestId: RequestId,
      schemaVersion: Schema.String,
      requestJson: RequestDocument,
      metadataJson: Schema.String,
      actorId: Schema.String,
      summary: Schema.String,
      sources: Schema.Array(SourceAsset)
    }
  }
) {}

export class CreateRequestSnapshot extends Schema.TaggedRequest<CreateRequestSnapshot>("CreateRequestSnapshot")(
  "CreateRequestSnapshot",
  {
    failure: RequestError,
    success: CommandResult,
    payload: {
      requestId: RequestId,
      snapshotId: Schema.String,
      snapshotType: SnapshotType,
      schemaVersion: Schema.String,
      requestJson: RequestDocument,
      metadataJson: Schema.String,
      actorId: Schema.String,
      summary: Schema.String
    }
  }
) {}

export class GetRequest extends Schema.TaggedRequest<GetRequest>("GetRequest")(
  "GetRequest",
  {
    failure: RequestNotFound,
    success: RequestState,
    payload: { requestId: RequestId }
  }
) {}

export class GetRequestHistory extends Schema.TaggedRequest<GetRequestHistory>("GetRequestHistory")(
  "GetRequestHistory",
  {
    failure: RequestNotFound,
    success: RequestHistory,
    payload: { requestId: RequestId }
  }
) {}

export const RequestProviderCommands = N2.defineCommands(
  CreateRequest,
  UpdateRequest,
  CreateRequestSnapshot,
  GetRequest,
  GetRequestHistory
)

export const RequestProviderCommand = RequestProviderCommands.schema
export type RequestProviderCommand = typeof RequestProviderCommand.Type

export const RequestProviderEntity = RequestProviderCommands.toPersistedEntity(
  "RequestProvider",
  (p) => p.requestId
)
export const RequestProviderRpcs = RequestProviderEntity.protocol
