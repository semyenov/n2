/**
 * Profile provider aggregate — pure business logic, no framework.
 *
 * Three plain functions implement the event-sourced state machine:
 *
 *   evolve : (state, event)   → state                    — pure fold, no effects
 *   decide : (state, command) → Effect<events, ProfileError>  — guards + domain rules
 *   handle : (state, command) → Effect<{events, state}>  — decide then evolve
 *
 * GetProfile and GetProfileHistory reach decide but return [] — they are read
 * queries handled directly by the entity layer and never run through this path.
 *
 * sourceAssets are merged in handle() (not evolve) because asset deduplication
 * is not event-sourced state — it is a side-effect of ingestion and does not
 * need to be replayed from the event log.
 *
 * Helper functions (hasMeaningfulPii, appendRevision, metadataEvent, piiEvent)
 * keep individual decide cases short and avoid parameter repetition.
 */
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import {
  type ProfileCommand,
  type ProfileEvent,
  type ProfileState,
  type MetadataScope,
  initialProfileState,
  ProfileError,
  ProfileCreated,
  MergedDataProfile,
  SnapshotCreatedProfile,
  MetaDataCreated,
  PersonalDataExtracted,
  ProfileBranchForked,
  SnapshotPublishedProfile,
  ProfileBranch,
  ProfileRevisionEntry,
  ProfileSnapshot,
  CreateProfile,
  MergeProfileData,
  ForkProfileBranch,
  CreateProfileSnapshot,
  PublishProfileSnapshot
} from "./contracts.js"

export { initialProfileState }

const hasMeaningfulPii = (storageKey: string, piiJson: string) =>
  storageKey.trim().length > 0 || piiJson.trim().length > 0

const appendRevision = (
  state: ProfileState,
  revision: number,
  branchId: string,
  eventType: string,
  summary: string,
  occurredAt: DateTime.Utc,
  actorId: string
) =>
  [
    ...state.revisions,
    new ProfileRevisionEntry({
      revision,
      branchId,
      eventType,
      summary,
      occurredAt,
      actorId
    })
  ]

const branchExists = (state: ProfileState, branchId: string) =>
  state.branches.some((branch) => branch.branchId === branchId)

const snapshotExists = (state: ProfileState, snapshotId: string) =>
  state.snapshots.some((snapshot) => snapshot.snapshotId === snapshotId)

const ensureProfileIdMatches = (
  profileId: string,
  candidateProfileId: string,
  fieldName: "maskedProfileJson" | "profileJson"
) =>
  candidateProfileId === profileId
    ? Effect.void
    : new ProfileError({
        message: `${fieldName}.uuid must match profileId "${profileId}"`
      })

const mergeSourceAssets = (current: ProfileState["sourceAssets"], incoming: ProfileState["sourceAssets"]) => {
  const next = new Map(current.map((asset) => [asset.sourceId, asset]))
  for (const asset of incoming) {
    next.set(asset.sourceId, asset)
  }
  return Array.from(next.values())
}

const nextRevision = (state: ProfileState, offset: number) => state.revision + offset + 1

const metadataEvent = (
  state: ProfileState,
  branchId: string,
  scope: MetadataScope,
  scopeId: string,
  metadataJson: string,
  schemaVersion: string,
  createdAt: DateTime.Utc,
  createdBy: string,
  offset: number
) =>
  new MetaDataCreated({
    profileId: state.profileId,
    branchId,
    scope,
    scopeId,
    metadataJson,
    schemaVersion,
    createdAt,
    createdBy,
    revision: nextRevision(state, offset)
  })

const piiEvent = (
  state: ProfileState,
  branchId: string,
  scope: MetadataScope,
  scopeId: string,
  piiStorageKey: string,
  piiJson: string,
  jurisdiction: string,
  extractedAt: DateTime.Utc,
  extractedBy: string,
  offset: number
) =>
  new PersonalDataExtracted({
    profileId: state.profileId,
    branchId,
    scope,
    scopeId,
    piiStorageKey,
    piiJson,
    jurisdiction,
    extractedAt,
    extractedBy,
    revision: nextRevision(state, offset)
  })

export const evolve = (state: ProfileState, event: ProfileEvent): ProfileState => {
  switch (event._tag) {
    case "ProfileCreated": {
      const branch = new ProfileBranch({
        branchId: event.branchId,
        label: "main",
        baseBranchId: "",
        baseRevision: 0,
        baseSnapshotId: "",
        createdAt: event.createdAt,
        createdBy: event.createdBy
      })
      return {
        ...state,
        status: "draft" as const,
        profileId: event.profileId,
        ownerAgentId: event.ownerAgentId,
        activeBranchId: event.branchId,
        currentSchemaVersion: event.schemaVersion,
        maskedProfileJson: event.maskedProfileJson,
        branches: [branch],
        revisions: appendRevision(
          state,
          event.revision,
          event.branchId,
          event._tag,
          event.summary,
          event.createdAt,
          event.createdBy
        ),
        revision: event.revision
      }
    }
    case "MergedDataProfile":
      return {
        ...state,
        status: "draft" as const,
        activeBranchId: event.branchId,
        currentSchemaVersion: event.schemaVersion,
        maskedProfileJson: event.maskedProfileJson,
        revisions: appendRevision(
          state,
          event.revision,
          event.branchId,
          event._tag,
          event.summary,
          event.mergedAt,
          event.mergedBy
        ),
        revision: event.revision
      }
    case "SnapshotCreatedProfile":
      return {
        ...state,
        activeBranchId: event.branchId,
        currentSchemaVersion: event.schemaVersion,
        snapshots: [
          ...state.snapshots,
          new ProfileSnapshot({
            snapshotId: event.snapshotId,
            branchId: event.branchId,
            revision: event.revision,
            snapshotType: event.snapshotType,
            profileJson: event.profileJson,
            metadataJson: event.metadataJson,
            schemaVersion: event.schemaVersion,
            summary: event.summary,
            createdAt: event.createdAt,
            createdBy: event.createdBy,
            published: false,
            strategyJson: ""
          })
        ],
        revisions: appendRevision(
          state,
          event.revision,
          event.branchId,
          event._tag,
          event.summary,
          event.createdAt,
          event.createdBy
        ),
        revision: event.revision
      }
    case "MetaDataCreated":
      return {
        ...state,
        latestMetadataJson: event.metadataJson,
        currentSchemaVersion: event.schemaVersion,
        activeBranchId: event.branchId,
        snapshots: state.snapshots.map((snapshot) =>
          event.scope === "snapshot" && snapshot.snapshotId === event.scopeId
            ? new ProfileSnapshot({ ...snapshot, metadataJson: event.metadataJson, schemaVersion: event.schemaVersion })
            : snapshot
        ),
        revisions: appendRevision(
          state,
          event.revision,
          event.branchId,
          event._tag,
          `${event.scope}:${event.scopeId}`,
          event.createdAt,
          event.createdBy
        ),
        revision: event.revision
      }
    case "PersonalDataExtracted":
      return {
        ...state,
        latestPiiStorageKey: event.piiStorageKey,
        piiJurisdiction: event.jurisdiction,
        activeBranchId: event.branchId,
        revisions: appendRevision(
          state,
          event.revision,
          event.branchId,
          event._tag,
          `${event.scope}:${event.scopeId}`,
          event.extractedAt,
          event.extractedBy
        ),
        revision: event.revision
      }
    case "ProfileBranchForked":
      return {
        ...state,
        activeBranchId: event.branchId,
        branches: [
          ...state.branches,
          new ProfileBranch({
            branchId: event.branchId,
            label: event.label,
            baseBranchId: event.baseBranchId,
            baseRevision: event.baseRevision,
            baseSnapshotId: event.baseSnapshotId,
            createdAt: event.createdAt,
            createdBy: event.createdBy
          })
        ],
        revisions: appendRevision(
          state,
          event.revision,
          event.branchId,
          event._tag,
          event.summary,
          event.createdAt,
          event.createdBy
        ),
        revision: event.revision
      }
    case "SnapshotPublishedProfile":
      return {
        ...state,
        status: "published" as const,
        publishedSnapshotId: event.snapshotId,
        snapshots: state.snapshots.map((snapshot) =>
          snapshot.snapshotId === event.snapshotId
            ? new ProfileSnapshot({ ...snapshot, published: true, strategyJson: event.strategyJson })
            : snapshot
        ),
        revisions: appendRevision(
          state,
          event.revision,
          state.activeBranchId,
          event._tag,
          event.snapshotId,
          event.publishedAt,
          event.publishedBy
        ),
        revision: event.revision
      }
  }
}

export const decide = (
  state: ProfileState,
  command: ProfileCommand
): Effect.Effect<ReadonlyArray<ProfileEvent>, ProfileError> => {
  switch (command._tag) {
    case "CreateProfile":
      return Effect.gen(function* () {
        if (state.status !== "empty") {
          return yield* new ProfileError({ message: "Profile already exists" })
        }
        yield* ensureProfileIdMatches(command.profileId, command.maskedProfileJson.uuid, "maskedProfileJson")
        const now = yield* DateTime.now
        const events: Array<ProfileEvent> = [
          new ProfileCreated({
            profileId: command.profileId,
            ownerAgentId: command.ownerAgentId,
            branchId: command.branchId,
            schemaVersion: command.schemaVersion,
            maskedProfileJson: command.maskedProfileJson,
            createdAt: now,
            createdBy: command.actorId,
            summary: command.summary,
            revision: nextRevision(state, 0)
          }),
          new MetaDataCreated({
            profileId: command.profileId,
            branchId: command.branchId,
            scope: "aggregate",
            scopeId: command.profileId,
            metadataJson: command.metadataJson,
            schemaVersion: command.schemaVersion,
            createdAt: now,
            createdBy: command.actorId,
            revision: nextRevision(state, 1)
          })
        ]
        if (hasMeaningfulPii(command.piiStorageKey, command.piiJson)) {
          events.push(new PersonalDataExtracted({
            profileId: command.profileId,
            branchId: command.branchId,
            scope: "aggregate",
            scopeId: command.profileId,
            piiStorageKey: command.piiStorageKey,
            piiJson: command.piiJson,
            jurisdiction: command.piiJurisdiction,
            extractedAt: now,
            extractedBy: command.actorId,
            revision: nextRevision(state, events.length)
          }))
        }
        return events
      })

    case "MergeProfileData":
      return Effect.gen(function* () {
        if (state.status === "empty") {
          return yield* new ProfileError({ message: "Profile does not exist" })
        }
        if (!branchExists(state, command.branchId)) {
          return yield* new ProfileError({ message: `Unknown branch "${command.branchId}"` })
        }
        yield* ensureProfileIdMatches(command.profileId, command.maskedProfileJson.uuid, "maskedProfileJson")
        const now = yield* DateTime.now
        const events: Array<ProfileEvent> = [
          new MergedDataProfile({
            profileId: command.profileId,
            branchId: command.branchId,
            schemaVersion: command.schemaVersion,
            maskedProfileJson: command.maskedProfileJson,
            mergedAt: now,
            mergedBy: command.actorId,
            summary: command.summary,
            sourceCount: command.sources.length,
            revision: nextRevision(state, 0)
          }),
          metadataEvent(
            state,
            command.branchId,
            "aggregate",
            command.profileId,
            command.metadataJson,
            command.schemaVersion,
            now,
            command.actorId,
            1
          )
        ]
        if (hasMeaningfulPii(command.piiStorageKey, command.piiJson)) {
          events.push(
            piiEvent(
              state,
              command.branchId,
              "aggregate",
              command.profileId,
              command.piiStorageKey,
              command.piiJson,
              command.piiJurisdiction,
              now,
              command.actorId,
              events.length
            )
          )
        }
        return events
      })

    case "ForkProfileBranch":
      return Effect.gen(function* () {
        if (state.status === "empty") {
          return yield* new ProfileError({ message: "Profile does not exist" })
        }
        if (branchExists(state, command.branchId)) {
          return yield* new ProfileError({ message: `Branch "${command.branchId}" already exists` })
        }
        if (command.baseBranchId !== "" && !branchExists(state, command.baseBranchId)) {
          return yield* new ProfileError({ message: `Unknown base branch "${command.baseBranchId}"` })
        }
        if (command.baseSnapshotId !== "" && !snapshotExists(state, command.baseSnapshotId)) {
          return yield* new ProfileError({ message: `Unknown base snapshot "${command.baseSnapshotId}"` })
        }
        const now = yield* DateTime.now
        return [
          new ProfileBranchForked({
            profileId: command.profileId,
            branchId: command.branchId,
            label: command.label,
            baseBranchId: command.baseBranchId,
            baseRevision: command.baseRevision,
            baseSnapshotId: command.baseSnapshotId,
            createdAt: now,
            createdBy: command.actorId,
            summary: command.summary,
            revision: nextRevision(state, 0)
          }),
          metadataEvent(
            state,
            command.branchId,
            "branch",
            command.branchId,
            command.metadataJson,
            command.schemaVersion,
            now,
            command.actorId,
            1
          )
        ]
      })

    case "CreateProfileSnapshot":
      return Effect.gen(function* () {
        if (state.status === "empty") {
          return yield* new ProfileError({ message: "Profile does not exist" })
        }
        if (!branchExists(state, command.branchId)) {
          return yield* new ProfileError({ message: `Unknown branch "${command.branchId}"` })
        }
        if (snapshotExists(state, command.snapshotId)) {
          return yield* new ProfileError({ message: `Snapshot "${command.snapshotId}" already exists` })
        }
        yield* ensureProfileIdMatches(command.profileId, command.profileJson.uuid, "profileJson")
        const now = yield* DateTime.now
        const events: Array<ProfileEvent> = [
          new SnapshotCreatedProfile({
            profileId: command.profileId,
            branchId: command.branchId,
            snapshotId: command.snapshotId,
            snapshotType: command.snapshotType,
            profileJson: command.profileJson,
            metadataJson: command.metadataJson,
            schemaVersion: command.schemaVersion,
            createdAt: now,
            createdBy: command.actorId,
            summary: command.summary,
            revision: nextRevision(state, 0)
          }),
          metadataEvent(
            state,
            command.branchId,
            "snapshot",
            command.snapshotId,
            command.metadataJson,
            command.schemaVersion,
            now,
            command.actorId,
            1
          )
        ]
        if (hasMeaningfulPii(command.piiStorageKey, command.piiJson)) {
          events.push(
            piiEvent(
              state,
              command.branchId,
              "snapshot",
              command.snapshotId,
              command.piiStorageKey,
              command.piiJson,
              command.piiJurisdiction,
              now,
              command.actorId,
              events.length
            )
          )
        }
        return events
      })

    case "PublishProfileSnapshot":
      return Effect.gen(function* () {
        if (state.status === "empty") {
          return yield* new ProfileError({ message: "Profile does not exist" })
        }
        if (!snapshotExists(state, command.snapshotId)) {
          return yield* new ProfileError({ message: `Unknown snapshot "${command.snapshotId}"` })
        }
        const now = yield* DateTime.now
        return [
          new SnapshotPublishedProfile({
            profileId: command.profileId,
            snapshotId: command.snapshotId,
            strategyJson: command.strategyJson,
            publishedAt: now,
            publishedBy: command.actorId,
            revision: nextRevision(state, 0)
          }),
          metadataEvent(
            state,
            state.activeBranchId,
            "publish",
            command.snapshotId,
            command.metadataJson,
            command.schemaVersion,
            now,
            command.actorId,
            1
          )
        ]
      })

    case "GetProfile":
    case "GetProfileHistory":
      return Effect.succeed([])
  }
}

export const handle = (state: ProfileState, command: ProfileCommand) =>
  Effect.gen(function* () {
    const events = yield* decide(state, command)
    let nextState = events.reduce(evolve, state)

    if (command._tag === "CreateProfile" || command._tag === "MergeProfileData") {
      nextState = {
        ...nextState,
        sourceAssets: mergeSourceAssets(nextState.sourceAssets, command.sources)
      }
    }

    return { events, state: nextState }
  })
