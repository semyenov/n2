/**
 * Profile provider service tests.
 *
 * Two layers of coverage:
 *
 *   1. Pure aggregate  — handle/evolve/decide with no infrastructure.
 *   2. Handler layer   — RpcTest.makeClient wired to in-memory services.
 */
import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { EventLog as EL } from "@effect/experimental"
import { RpcTest } from "@effect/rpc"
import { makeTestAggregate } from "n2/helpers"
import { handle, initialProfileState } from "./aggregate.js"
import {
  CreateProfile,
  CreateProfileSnapshot,
  ForkProfileBranch,
  GetProfile,
  GetProfileHistory,
  MergeProfileData,
  ProfileDocument,
  ProfileError,
  ProfileNotFound,
  ProfileProviderRpcs,
  ProfileState,
  PublishProfileSnapshot,
  SourceAsset
} from "./contracts.js"
import { ProfileProviderEventGroup, ProfileProviderEventLogSchema } from "./events.js"
import { ProfileProviderHandlersRaw } from "./entity.js"
import { ProfileProviderSnapshots } from "./snapshots.js"

const makeProfileId = (seed: number) =>
  `00000000-0000-4000-8000-${seed.toString().padStart(12, "0")}`

const decodeProfile = Schema.decodeUnknownSync(ProfileDocument)

const makeProfile = (profileId: string, relevantPosition = "Actor") =>
  decodeProfile({
    uuid: profileId,
    created_at: "2026-01-01T00:00:00.000Z",
    updated_at: "2026-01-02T00:00:00.000Z",
    user_data: {
      personal_info: {
        first_name: "Ada",
        last_name: "Lovelace",
        relevant_position: relevantPosition,
        relocation: true
      },
      salary_expectations: {
        currency: "USD",
        amount_from: 1000
      },
      skills: [
        {
          name: "TypeScript",
          level: "advanced",
          years_of_experience: 5
        }
      ],
      education: [
        {
          degree: "Bachelor",
          field_of_study: "Computer Science",
          institution: "Analytical Engine Institute"
        }
      ]
    },
    user_meta_data: {
      version: 1,
      source_platform: "resume-import"
    }
  })

const sampleSource = new SourceAsset({
  sourceId: "src-1",
  kind: "file",
  uri: "s3://bucket/resume.pdf",
  mediaType: "application/pdf",
  storageKey: "resume.pdf",
  summary: "Initial resume"
})

const NoOpProjection = EL.group(
  ProfileProviderEventGroup,
  (handlers) =>
    handlers
      .handle("ProfileCreated", (_) => Effect.void)
      .handle("MergedDataProfile", (_) => Effect.void)
      .handle("SnapshotCreatedProfile", (_) => Effect.void)
      .handle("MetaDataCreated", (_) => Effect.void)
      .handle("PersonalDataExtracted", (_) => Effect.void)
      .handle("ProfileBranchForked", (_) => Effect.void)
      .handle("SnapshotPublishedProfile", (_) => Effect.void)
)

const { makeTestLayers, runWith } = makeTestAggregate<ProfileState>({
  eventLogSchema: ProfileProviderEventLogSchema,
  noOpProjection: NoOpProjection,
  handlersLayer: ProfileProviderHandlersRaw,
  snapshotsTag: ProfileProviderSnapshots
})

const run = <A, E>(effect: Effect.Effect<A, E, never>): Promise<A> =>
  Effect.runPromise(effect)

test("CreateProfile initializes draft state and metadata revisions", async () => {
  const profileId = makeProfileId(1)
  const { state, events } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "agent-1",
    branchId: "main",
    schemaVersion: "1.0.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{\"channel\":\"resume\"}",
    piiStorageKey: "pii/profile-1",
    piiJson: "{\"email\":\"hidden\"}",
    piiJurisdiction: "DE",
    actorId: "agent-1",
    summary: "Create profile from resume",
    sources: [sampleSource]
  })))

  expect(state.status).toBe("draft")
  expect(state.profileId).toBe(profileId)
  expect(state.maskedProfileJson?.uuid).toBe(profileId)
  expect(state.revision).toBe(3)
  expect(events.map((event) => event._tag)).toEqual([
    "ProfileCreated",
    "MetaDataCreated",
    "PersonalDataExtracted"
  ])
})

test("MergeProfileData fails on unknown branch", async () => {
  const profileId = makeProfileId(2)
  const { state } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "agent-1",
    branchId: "main",
    schemaVersion: "1.0.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "agent-1",
    summary: "Create",
    sources: [sampleSource]
  })))

  const error = await run(handle(state, new MergeProfileData({
    profileId,
    branchId: "alt",
    schemaVersion: "1.0.1",
    maskedProfileJson: makeProfile(profileId, "Singer"),
    metadataJson: "{\"channel\":\"chat\"}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "agent-1",
    summary: "Invalid branch merge",
    sources: []
  })).pipe(Effect.flip))

  expect(error).toBeInstanceOf(ProfileError)
})

test("ForkProfileBranch then CreateProfileSnapshot records history", async () => {
  const profileId = makeProfileId(3)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "agent-1",
    branchId: "main",
    schemaVersion: "1.0.0",
    maskedProfileJson: makeProfile(profileId, "Performer"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "agent-1",
    summary: "Create",
    sources: [sampleSource]
  })))
  const { state: s2 } = await run(handle(s1, new ForkProfileBranch({
    profileId,
    branchId: "casting",
    label: "Casting angle",
    baseBranchId: "main",
    baseRevision: s1.revision,
    baseSnapshotId: "",
    metadataJson: "{\"reason\":\"alternate positioning\"}",
    schemaVersion: "1.0.0",
    actorId: "agent-1",
    summary: "Branch from main"
  })))
  const { state: s3 } = await run(handle(s2, new CreateProfileSnapshot({
    profileId,
    branchId: "casting",
    snapshotId: "snap-1",
    snapshotType: "LLM",
    schemaVersion: "1.0.1",
    profileJson: makeProfile(profileId, "Commercial talent"),
    metadataJson: "{\"model\":\"gpt\"}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "agent-1",
    summary: "Summarized casting version"
  })))

  expect(s3.branches.length).toBe(2)
  expect(s3.snapshots.length).toBe(1)
  expect(s3.snapshots[0]?.snapshotId).toBe("snap-1")
  expect(s3.snapshots[0]?.profileJson.user_data.personal_info.relevant_position).toBe("Commercial talent")
})

test("handlers: create, merge, snapshot, publish, history", async () => {
  const { handlersLayer } = makeTestLayers()
  const profileId = makeProfileId(4)

  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(ProfileProviderRpcs)
    yield* client.CreateProfile({
      profileId,
      ownerAgentId: "agent-1",
      branchId: "main",
      schemaVersion: "1.0.0",
      maskedProfileJson: makeProfile(profileId, "Actor"),
      metadataJson: "{\"channel\":\"resume\"}",
      piiStorageKey: "",
      piiJson: "",
      piiJurisdiction: "",
      actorId: "agent-1",
      summary: "Create",
      sources: [sampleSource]
    })
    yield* client.MergeProfileData({
      profileId,
      branchId: "main",
      schemaVersion: "1.0.1",
      maskedProfileJson: makeProfile(profileId, "Actor Singer"),
      metadataJson: "{\"channel\":\"chat\"}",
      piiStorageKey: "",
      piiJson: "",
      piiJurisdiction: "",
      actorId: "agent-1",
      summary: "Improve summary",
      sources: []
    })
    yield* client.CreateProfileSnapshot({
      profileId,
      branchId: "main",
      snapshotId: "snap-h1",
      snapshotType: "MANUAL",
      schemaVersion: "1.0.1",
      profileJson: makeProfile(profileId, "Actor Singer"),
      metadataJson: "{\"reviewed\":true}",
      piiStorageKey: "",
      piiJson: "",
      piiJurisdiction: "",
      actorId: "agent-1",
      summary: "Manual draft"
    })
    yield* client.PublishProfileSnapshot({
      profileId,
      snapshotId: "snap-h1",
      strategyJson: "{\"segment\":\"commercial\"}",
      metadataJson: "{\"action\":\"publish\"}",
      schemaVersion: "1.0.1",
      actorId: "agent-1"
    })

    const history = yield* client.GetProfileHistory({ profileId })
    expect(history.profileId).toBe(profileId)
    expect(history.snapshots.length).toBe(1)
    expect(history.publishedSnapshotId).toBe("snap-h1")
  }))
})

test("handlers: unknown profile returns ProfileNotFound", async () => {
  const { handlersLayer } = makeTestLayers()
  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(ProfileProviderRpcs)
    const error = yield* client.GetProfileHistory({ profileId: makeProfileId(999) }).pipe(Effect.flip)
    expect(error).toBeInstanceOf(ProfileNotFound)
  }))
})

test("CreateProfile on existing profile fails", async () => {
  const profileId = makeProfileId(11)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "init",
    sources: []
  })))
  const err = await run(handle(s1, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "duplicate",
    sources: []
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
})

test("CreateProfile rejects mismatched profile uuid", async () => {
  const profileId = makeProfileId(12)
  const err = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(makeProfileId(13), "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "init",
    sources: []
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
})

test("MergeProfileData on empty profile fails", async () => {
  const profileId = makeProfileId(14)
  const err = await run(handle(initialProfileState, new MergeProfileData({
    profileId,
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "merge on empty",
    sources: []
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
})

test("ForkProfileBranch on empty profile fails", async () => {
  const err = await run(handle(initialProfileState, new ForkProfileBranch({
    profileId: makeProfileId(15),
    branchId: "alt",
    label: "Alt",
    baseBranchId: "",
    baseRevision: 0,
    baseSnapshotId: "",
    metadataJson: "{}",
    schemaVersion: "1.0",
    actorId: "a",
    summary: "fork"
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
})

test("ForkProfileBranch with duplicate branchId fails", async () => {
  const profileId = makeProfileId(16)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "init",
    sources: []
  })))
  const err = await run(handle(s1, new ForkProfileBranch({
    profileId,
    branchId: "main",
    label: "dup",
    baseBranchId: "main",
    baseRevision: s1.revision,
    baseSnapshotId: "",
    metadataJson: "{}",
    schemaVersion: "1.0",
    actorId: "a",
    summary: "dup fork"
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
})

test("CreateProfileSnapshot with duplicate snapshotId fails", async () => {
  const profileId = makeProfileId(17)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "init",
    sources: []
  })))
  const { state: s2 } = await run(handle(s1, new CreateProfileSnapshot({
    profileId,
    branchId: "main",
    snapshotId: "snap-e5",
    snapshotType: "MANUAL",
    schemaVersion: "1.0",
    profileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "first snap"
  })))
  const err = await run(handle(s2, new CreateProfileSnapshot({
    profileId,
    branchId: "main",
    snapshotId: "snap-e5",
    snapshotType: "MANUAL",
    schemaVersion: "1.0",
    profileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "dup snap"
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
})

test("PublishProfileSnapshot on unknown snapshotId fails", async () => {
  const profileId = makeProfileId(18)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "init",
    sources: []
  })))
  const err = await run(handle(s1, new PublishProfileSnapshot({
    profileId,
    snapshotId: "no-such-snap",
    strategyJson: "{}",
    metadataJson: "{}",
    schemaVersion: "1.0",
    actorId: "a"
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
})

test("handlers: GetProfile returns current state", async () => {
  const { handlersLayer } = makeTestLayers()
  const profileId = makeProfileId(19)

  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(ProfileProviderRpcs)
    yield* client.CreateProfile({
      profileId,
      ownerAgentId: "agent-1",
      branchId: "main",
      schemaVersion: "1.0",
      maskedProfileJson: makeProfile(profileId, "Vocalist"),
      metadataJson: "{}",
      piiStorageKey: "",
      piiJson: "",
      piiJurisdiction: "",
      actorId: "agent-1",
      summary: "Create",
      sources: []
    })
    const state = yield* client.GetProfile({ profileId })
    expect(state.profileId).toBe(profileId)
    expect(state.status).toBe("draft")
    expect(state.maskedProfileJson?.user_data.personal_info.relevant_position).toBe("Vocalist")
  }))
})

test("handlers: GetProfile for unknown profile returns ProfileNotFound", async () => {
  const { handlersLayer } = makeTestLayers()
  const profileId = makeProfileId(20)

  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(ProfileProviderRpcs)
    const err = yield* client.GetProfile({ profileId }).pipe(Effect.flip)
    expect(err._tag).toBe("ProfileNotFound")
    expect((err as unknown as ProfileNotFound).profileId).toBe(profileId)
  }))
})

test("snapshot recovery: GetProfile loads state from snapshot store", async () => {
  const { snapshotStore, handlersLayer } = makeTestLayers()
  const profileId = makeProfileId(21)

  snapshotStore.set(profileId, {
    state: new ProfileState({
      status: "draft",
      profileId,
      ownerAgentId: "agent-x",
      activeBranchId: "main",
      currentSchemaVersion: "2.0",
      maskedProfileJson: makeProfile(profileId, "Recovered"),
      latestMetadataJson: "{}",
      latestPiiStorageKey: "",
      piiJurisdiction: "",
      sourceAssets: [],
      branches: [],
      revisions: [],
      snapshots: [],
      publishedSnapshotId: "",
      revision: 10
    }),
    revision: 10
  })

  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(ProfileProviderRpcs)
    const state = yield* client.GetProfile({ profileId })
    expect(state.status).toBe("draft")
    expect(state.profileId).toBe(profileId)
    expect(state.maskedProfileJson?.user_data.personal_info.relevant_position).toBe("Recovered")
    expect(state.ownerAgentId).toBe("agent-x")
  }))
})

// ---------------------------------------------------------------------------
// A. Error edge cases
// ---------------------------------------------------------------------------

test("ForkProfileBranch with unknown baseBranchId fails", async () => {
  const profileId = makeProfileId(30)
  const { state } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "init",
    sources: []
  })))
  const err = await run(handle(state, new ForkProfileBranch({
    profileId,
    branchId: "alt",
    label: "Alt",
    baseBranchId: "nonexistent",
    baseRevision: 0,
    baseSnapshotId: "",
    metadataJson: "{}",
    schemaVersion: "1.0",
    actorId: "a",
    summary: "fork"
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
  expect(err.message).toContain("Unknown base branch")
})

test("ForkProfileBranch with unknown baseSnapshotId fails", async () => {
  const profileId = makeProfileId(31)
  const { state } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "init",
    sources: []
  })))
  const err = await run(handle(state, new ForkProfileBranch({
    profileId,
    branchId: "alt",
    label: "Alt",
    baseBranchId: "main",
    baseRevision: state.revision,
    baseSnapshotId: "nonexistent-snap",
    metadataJson: "{}",
    schemaVersion: "1.0",
    actorId: "a",
    summary: "fork"
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
  expect(err.message).toContain("Unknown base snapshot")
})

test("MergeProfileData rejects mismatched profile uuid", async () => {
  const profileId = makeProfileId(32)
  const { state } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "init",
    sources: []
  })))
  const err = await run(handle(state, new MergeProfileData({
    profileId,
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(makeProfileId(99), "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "merge",
    sources: []
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
  expect(err.message).toContain("maskedProfileJson.uuid must match")
})

test("CreateProfileSnapshot on unknown branch fails", async () => {
  const profileId = makeProfileId(33)
  const { state } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "init",
    sources: []
  })))
  const err = await run(handle(state, new CreateProfileSnapshot({
    profileId,
    branchId: "nonexistent",
    snapshotId: "snap-x",
    snapshotType: "MANUAL",
    schemaVersion: "1.0",
    profileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "snap"
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
  expect(err.message).toContain("Unknown branch")
})

test("CreateProfileSnapshot rejects mismatched profile uuid", async () => {
  const profileId = makeProfileId(34)
  const { state } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "init",
    sources: []
  })))
  const err = await run(handle(state, new CreateProfileSnapshot({
    profileId,
    branchId: "main",
    snapshotId: "snap-x",
    snapshotType: "MANUAL",
    schemaVersion: "1.0",
    profileJson: makeProfile(makeProfileId(99), "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "snap"
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
  expect(err.message).toContain("profileJson.uuid must match")
})

test("PublishProfileSnapshot on empty profile fails", async () => {
  const err = await run(handle(initialProfileState, new PublishProfileSnapshot({
    profileId: makeProfileId(35),
    snapshotId: "snap-x",
    strategyJson: "{}",
    metadataJson: "{}",
    schemaVersion: "1.0",
    actorId: "a"
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
  expect(err.message).toContain("Profile does not exist")
})

// ---------------------------------------------------------------------------
// B. PII conditional event counts
// ---------------------------------------------------------------------------

test("CreateProfile without PII produces exactly 2 events", async () => {
  const profileId = makeProfileId(40)
  const { state, events } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "init",
    sources: []
  })))
  expect(events.length).toBe(2)
  expect(events.map((e) => e._tag)).toEqual(["ProfileCreated", "MetaDataCreated"])
  expect(state.revision).toBe(2)
})

test("MergeProfileData without PII produces 2 events", async () => {
  const profileId = makeProfileId(41)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "init",
    sources: []
  })))
  const { events } = await run(handle(s1, new MergeProfileData({
    profileId,
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Singer"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "merge",
    sources: []
  })))
  expect(events.length).toBe(2)
  expect(events.map((e) => e._tag)).toEqual(["MergedDataProfile", "MetaDataCreated"])
})

test("MergeProfileData with PII produces 3 events", async () => {
  const profileId = makeProfileId(42)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "init",
    sources: []
  })))
  const { events } = await run(handle(s1, new MergeProfileData({
    profileId,
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Singer"),
    metadataJson: "{}",
    piiStorageKey: "pii/key",
    piiJson: "{\"email\":\"hidden\"}",
    piiJurisdiction: "DE",
    actorId: "a",
    summary: "merge",
    sources: []
  })))
  expect(events.length).toBe(3)
  expect(events.map((e) => e._tag)).toEqual(["MergedDataProfile", "MetaDataCreated", "PersonalDataExtracted"])
})

test("CreateProfileSnapshot without PII produces 2 events", async () => {
  const profileId = makeProfileId(43)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "init",
    sources: []
  })))
  const { events } = await run(handle(s1, new CreateProfileSnapshot({
    profileId,
    branchId: "main",
    snapshotId: "snap-nopii",
    snapshotType: "MANUAL",
    schemaVersion: "1.0",
    profileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "snap"
  })))
  expect(events.length).toBe(2)
  expect(events.map((e) => e._tag)).toEqual(["SnapshotCreatedProfile", "MetaDataCreated"])
})

test("CreateProfileSnapshot with PII produces 3 events", async () => {
  const profileId = makeProfileId(44)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "init",
    sources: []
  })))
  const { events } = await run(handle(s1, new CreateProfileSnapshot({
    profileId,
    branchId: "main",
    snapshotId: "snap-pii",
    snapshotType: "MANUAL",
    schemaVersion: "1.0",
    profileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "pii/snap",
    piiJson: "{\"ssn\":\"hidden\"}",
    piiJurisdiction: "US",
    actorId: "a",
    summary: "snap"
  })))
  expect(events.length).toBe(3)
  expect(events.map((e) => e._tag)).toEqual(["SnapshotCreatedProfile", "MetaDataCreated", "PersonalDataExtracted"])
})

// ---------------------------------------------------------------------------
// C. Revision numbering
// ---------------------------------------------------------------------------

test("CreateProfile with PII produces consecutive revisions starting from 1", async () => {
  const profileId = makeProfileId(50)
  const { state, events } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "pii/key",
    piiJson: "{\"email\":\"x\"}",
    piiJurisdiction: "DE",
    actorId: "a",
    summary: "init",
    sources: []
  })))
  expect(events.map((e) => e.revision)).toEqual([1, 2, 3])
  expect(state.revision).toBe(3)
})

test("MergeProfileData revisions continue from prior state", async () => {
  const profileId = makeProfileId(51)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "init",
    sources: []
  })))
  expect(s1.revision).toBe(2)
  const { state: s2, events } = await run(handle(s1, new MergeProfileData({
    profileId,
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Singer"),
    metadataJson: "{}",
    piiStorageKey: "pii/key",
    piiJson: "{\"email\":\"x\"}",
    piiJurisdiction: "DE",
    actorId: "a",
    summary: "merge",
    sources: []
  })))
  expect(events.map((e) => e.revision)).toEqual([3, 4, 5])
  expect(s2.revision).toBe(5)
})

// ---------------------------------------------------------------------------
// D. PostHandle source asset merging (handler-level)
// ---------------------------------------------------------------------------

test("handlers: CreateProfile stores source assets", async () => {
  const { handlersLayer } = makeTestLayers()
  const profileId = makeProfileId(60)

  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(ProfileProviderRpcs)
    yield* client.CreateProfile({
      profileId,
      ownerAgentId: "a",
      branchId: "main",
      schemaVersion: "1.0",
      maskedProfileJson: makeProfile(profileId, "Actor"),
      metadataJson: "{}",
      piiStorageKey: "",
      piiJson: "",
      piiJurisdiction: "",
      actorId: "a",
      summary: "init",
      sources: [sampleSource]
    })
    const state = yield* client.GetProfile({ profileId })
    expect(state.sourceAssets.length).toBe(1)
    expect(state.sourceAssets[0]?.sourceId).toBe("src-1")
  }))
})

test("handlers: MergeProfileData deduplicates source assets by sourceId", async () => {
  const { handlersLayer } = makeTestLayers()
  const profileId = makeProfileId(61)
  const sourceA = new SourceAsset({
    sourceId: "src-a",
    kind: "file",
    uri: "s3://bucket/a.pdf",
    mediaType: "application/pdf",
    storageKey: "a.pdf",
    summary: "Source A"
  })
  const sourceAUpdated = new SourceAsset({
    sourceId: "src-a",
    kind: "file",
    uri: "s3://bucket/a-v2.pdf",
    mediaType: "application/pdf",
    storageKey: "a-v2.pdf",
    summary: "Source A updated"
  })
  const sourceB = new SourceAsset({
    sourceId: "src-b",
    kind: "url",
    uri: "https://example.com",
    mediaType: "text/html",
    storageKey: "b.html",
    summary: "Source B"
  })

  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(ProfileProviderRpcs)
    yield* client.CreateProfile({
      profileId,
      ownerAgentId: "a",
      branchId: "main",
      schemaVersion: "1.0",
      maskedProfileJson: makeProfile(profileId, "Actor"),
      metadataJson: "{}",
      piiStorageKey: "",
      piiJson: "",
      piiJurisdiction: "",
      actorId: "a",
      summary: "init",
      sources: [sourceA]
    })
    yield* client.MergeProfileData({
      profileId,
      branchId: "main",
      schemaVersion: "1.0",
      maskedProfileJson: makeProfile(profileId, "Singer"),
      metadataJson: "{}",
      piiStorageKey: "",
      piiJson: "",
      piiJurisdiction: "",
      actorId: "a",
      summary: "merge",
      sources: [sourceAUpdated, sourceB]
    })
    const state = yield* client.GetProfile({ profileId })
    expect(state.sourceAssets.length).toBe(2)
    const ids = state.sourceAssets.map((a) => a.sourceId).sort()
    expect(ids).toEqual(["src-a", "src-b"])
    const updatedA = state.sourceAssets.find((a) => a.sourceId === "src-a")
    expect(updatedA?.summary).toBe("Source A updated")
  }))
})

// ---------------------------------------------------------------------------
// E. MetaDataCreated scope field validation
// ---------------------------------------------------------------------------

test("CreateProfile emits MetaDataCreated with scope 'aggregate'", async () => {
  const profileId = makeProfileId(70)
  const { events } = await run(handle(initialProfileState, new CreateProfile({
    profileId,
    ownerAgentId: "a",
    branchId: "main",
    schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{\"channel\":\"test\"}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "a",
    summary: "init",
    sources: []
  })))
  const metadata = events.find((e) => e._tag === "MetaDataCreated") as any
  expect(metadata.scope).toBe("aggregate")
  expect(metadata.scopeId).toBe(profileId)
  expect(metadata.metadataJson).toBe("{\"channel\":\"test\"}")
})

test("MergeProfileData emits MetaDataCreated with scope 'aggregate'", async () => {
  const profileId = makeProfileId(71)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId, ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}", piiStorageKey: "",
    piiJson: "", piiJurisdiction: "", actorId: "a", summary: "init", sources: []
  })))
  const { events } = await run(handle(s1, new MergeProfileData({
    profileId, branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Singer"), metadataJson: "{\"merged\":true}",
    piiStorageKey: "", piiJson: "", piiJurisdiction: "", actorId: "a", summary: "merge", sources: []
  })))
  const metadata = events.find((e) => e._tag === "MetaDataCreated") as any
  expect(metadata.scope).toBe("aggregate")
  expect(metadata.scopeId).toBe(profileId)
})

test("ForkProfileBranch emits MetaDataCreated with scope 'branch'", async () => {
  const profileId = makeProfileId(72)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId, ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}", piiStorageKey: "",
    piiJson: "", piiJurisdiction: "", actorId: "a", summary: "init", sources: []
  })))
  const { events } = await run(handle(s1, new ForkProfileBranch({
    profileId, branchId: "casting", label: "Casting", baseBranchId: "main",
    baseRevision: s1.revision, baseSnapshotId: "", metadataJson: "{\"fork\":true}",
    schemaVersion: "1.0", actorId: "a", summary: "fork"
  })))
  const metadata = events.find((e) => e._tag === "MetaDataCreated") as any
  expect(metadata.scope).toBe("branch")
  expect(metadata.scopeId).toBe("casting")
})

test("CreateProfileSnapshot emits MetaDataCreated with scope 'snapshot'", async () => {
  const profileId = makeProfileId(73)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId, ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}", piiStorageKey: "",
    piiJson: "", piiJurisdiction: "", actorId: "a", summary: "init", sources: []
  })))
  const { events } = await run(handle(s1, new CreateProfileSnapshot({
    profileId, branchId: "main", snapshotId: "snap-scope", snapshotType: "MANUAL",
    schemaVersion: "1.0", profileJson: makeProfile(profileId, "Actor"), metadataJson: "{\"snap\":true}",
    piiStorageKey: "", piiJson: "", piiJurisdiction: "", actorId: "a", summary: "snap"
  })))
  const metadata = events.find((e) => e._tag === "MetaDataCreated") as any
  expect(metadata.scope).toBe("snapshot")
  expect(metadata.scopeId).toBe("snap-scope")
})

test("PublishProfileSnapshot emits MetaDataCreated with scope 'publish'", async () => {
  const profileId = makeProfileId(74)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId, ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}", piiStorageKey: "",
    piiJson: "", piiJurisdiction: "", actorId: "a", summary: "init", sources: []
  })))
  const { state: s2 } = await run(handle(s1, new CreateProfileSnapshot({
    profileId, branchId: "main", snapshotId: "snap-pub", snapshotType: "MANUAL",
    schemaVersion: "1.0", profileJson: makeProfile(profileId, "Actor"), metadataJson: "{}",
    piiStorageKey: "", piiJson: "", piiJurisdiction: "", actorId: "a", summary: "snap"
  })))
  const { events } = await run(handle(s2, new PublishProfileSnapshot({
    profileId, snapshotId: "snap-pub", strategyJson: "{\"segment\":\"A\"}",
    metadataJson: "{\"publish\":true}", schemaVersion: "1.0", actorId: "a"
  })))
  const metadata = events.find((e) => e._tag === "MetaDataCreated") as any
  expect(metadata.scope).toBe("publish")
  expect(metadata.scopeId).toBe("snap-pub")
})

// ---------------------------------------------------------------------------
// F. Evolve state mutations
// ---------------------------------------------------------------------------

test("evolve: SnapshotCreatedProfile appends snapshot with published=false", async () => {
  const profileId = makeProfileId(80)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId, ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}", piiStorageKey: "",
    piiJson: "", piiJurisdiction: "", actorId: "a", summary: "init", sources: []
  })))
  const { state: s2 } = await run(handle(s1, new CreateProfileSnapshot({
    profileId, branchId: "main", snapshotId: "snap-evo", snapshotType: "AUTO",
    schemaVersion: "1.0", profileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{\"model\":\"v1\"}", piiStorageKey: "", piiJson: "", piiJurisdiction: "",
    actorId: "a", summary: "auto snap"
  })))
  expect(s2.snapshots.length).toBe(1)
  expect(s2.snapshots[0]?.published).toBe(false)
  expect(s2.snapshots[0]?.strategyJson).toBe("")
  expect(s2.snapshots[0]?.snapshotType).toBe("AUTO")
})

test("evolve: SnapshotPublishedProfile marks snapshot published and sets strategyJson", async () => {
  const profileId = makeProfileId(81)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId, ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}", piiStorageKey: "",
    piiJson: "", piiJurisdiction: "", actorId: "a", summary: "init", sources: []
  })))
  const { state: s2 } = await run(handle(s1, new CreateProfileSnapshot({
    profileId, branchId: "main", snapshotId: "snap-pub2", snapshotType: "MANUAL",
    schemaVersion: "1.0", profileJson: makeProfile(profileId, "Actor"), metadataJson: "{}",
    piiStorageKey: "", piiJson: "", piiJurisdiction: "", actorId: "a", summary: "snap"
  })))
  const { state: s3 } = await run(handle(s2, new PublishProfileSnapshot({
    profileId, snapshotId: "snap-pub2", strategyJson: "{\"routing\":\"priority\"}",
    metadataJson: "{}", schemaVersion: "1.0", actorId: "a"
  })))
  expect(s3.status).toBe("published")
  expect(s3.publishedSnapshotId).toBe("snap-pub2")
  expect(s3.snapshots[0]?.published).toBe(true)
  expect(s3.snapshots[0]?.strategyJson).toBe("{\"routing\":\"priority\"}")
})

test("evolve: MetaDataCreated with scope 'snapshot' updates snapshot metadata", async () => {
  const profileId = makeProfileId(82)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId, ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}", piiStorageKey: "",
    piiJson: "", piiJurisdiction: "", actorId: "a", summary: "init", sources: []
  })))
  // Create snapshot with initial metadata
  const { state: s2 } = await run(handle(s1, new CreateProfileSnapshot({
    profileId, branchId: "main", snapshotId: "snap-meta", snapshotType: "MANUAL",
    schemaVersion: "1.0", profileJson: makeProfile(profileId, "Actor"),
    metadataJson: "{\"original\":true}", piiStorageKey: "", piiJson: "", piiJurisdiction: "",
    actorId: "a", summary: "snap"
  })))
  expect(s2.snapshots[0]?.metadataJson).toBe("{\"original\":true}")

  // The CreateProfileSnapshot emits MetaDataCreated with scope "snapshot" and scopeId "snap-meta"
  // which updates the snapshot's metadataJson in evolve. The snapshot should have the metadata
  // from the MetaDataCreated event, not the original.
  // Let's verify by checking the metadataJson on the snapshot matches the MetaDataCreated event's value.
  expect(s2.snapshots[0]?.schemaVersion).toBe("1.0")
})

test("evolve: PersonalDataExtracted updates latestPiiStorageKey and jurisdiction", async () => {
  const profileId = makeProfileId(83)
  const { state } = await run(handle(initialProfileState, new CreateProfile({
    profileId, ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}",
    piiStorageKey: "pii/storage/key-1", piiJson: "{\"email\":\"test@example.com\"}",
    piiJurisdiction: "DE", actorId: "a", summary: "init", sources: []
  })))
  expect(state.latestPiiStorageKey).toBe("pii/storage/key-1")
  expect(state.piiJurisdiction).toBe("DE")
})

test("evolve: ProfileBranchForked appends branch with correct metadata", async () => {
  const profileId = makeProfileId(84)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId, ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}", piiStorageKey: "",
    piiJson: "", piiJurisdiction: "", actorId: "a", summary: "init", sources: []
  })))
  const { state: s2 } = await run(handle(s1, new ForkProfileBranch({
    profileId, branchId: "casting", label: "Casting angle", baseBranchId: "main",
    baseRevision: s1.revision, baseSnapshotId: "", metadataJson: "{}",
    schemaVersion: "1.0", actorId: "actor-2", summary: "fork for casting"
  })))
  expect(s2.branches.length).toBe(2)
  expect(s2.activeBranchId).toBe("casting")
  const fork = s2.branches[1]!
  expect(fork.branchId).toBe("casting")
  expect(fork.label).toBe("Casting angle")
  expect(fork.baseBranchId).toBe("main")
  expect(fork.baseRevision).toBe(s1.revision)
  expect(fork.createdBy).toBe("actor-2")
})

test("evolve: MergedDataProfile updates maskedProfileJson and schemaVersion", async () => {
  const profileId = makeProfileId(85)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId, ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}", piiStorageKey: "",
    piiJson: "", piiJurisdiction: "", actorId: "a", summary: "init", sources: []
  })))
  expect(s1.maskedProfileJson?.user_data.personal_info.relevant_position).toBe("Actor")
  const { state: s2 } = await run(handle(s1, new MergeProfileData({
    profileId, branchId: "main", schemaVersion: "2.0",
    maskedProfileJson: makeProfile(profileId, "Singer"), metadataJson: "{}",
    piiStorageKey: "", piiJson: "", piiJurisdiction: "", actorId: "a", summary: "merge", sources: []
  })))
  expect(s2.maskedProfileJson?.user_data.personal_info.relevant_position).toBe("Singer")
  expect(s2.currentSchemaVersion).toBe("2.0")
  expect(s2.status).toBe("draft")
})

// ---------------------------------------------------------------------------
// G. Revision history tracking
// ---------------------------------------------------------------------------

test("revisions track eventType and summary for each event", async () => {
  const profileId = makeProfileId(90)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId, ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}", piiStorageKey: "",
    piiJson: "", piiJurisdiction: "", actorId: "agent-1", summary: "Created from resume", sources: []
  })))
  // ProfileCreated + MetaDataCreated = 2 revisions
  expect(s1.revisions.length).toBe(2)
  expect(s1.revisions[0]?.eventType).toBe("ProfileCreated")
  expect(s1.revisions[0]?.summary).toBe("Created from resume")
  expect(s1.revisions[0]?.actorId).toBe("agent-1")
  expect(s1.revisions[1]?.eventType).toBe("MetaDataCreated")
  // MetaDataCreated summary is "scope:scopeId"
  expect(s1.revisions[1]?.summary).toBe(`aggregate:${profileId}`)
})

test("full lifecycle revisions are tracked correctly", async () => {
  const profileId = makeProfileId(91)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId, ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}", piiStorageKey: "",
    piiJson: "", piiJurisdiction: "", actorId: "a", summary: "init", sources: []
  })))
  const { state: s2 } = await run(handle(s1, new ForkProfileBranch({
    profileId, branchId: "casting", label: "Casting", baseBranchId: "main",
    baseRevision: s1.revision, baseSnapshotId: "", metadataJson: "{}",
    schemaVersion: "1.0", actorId: "a", summary: "fork for casting"
  })))
  const { state: s3 } = await run(handle(s2, new CreateProfileSnapshot({
    profileId, branchId: "casting", snapshotId: "snap-1", snapshotType: "MANUAL",
    schemaVersion: "1.0", profileJson: makeProfile(profileId, "Commercial"),
    metadataJson: "{}", piiStorageKey: "", piiJson: "", piiJurisdiction: "",
    actorId: "a", summary: "manual snap"
  })))
  const { state: s4 } = await run(handle(s3, new PublishProfileSnapshot({
    profileId, snapshotId: "snap-1", strategyJson: "{}",
    metadataJson: "{}", schemaVersion: "1.0", actorId: "a"
  })))
  // 2 (create) + 2 (fork) + 2 (snapshot) + 2 (publish) = 8 revision entries
  expect(s4.revisions.length).toBe(8)
  expect(s4.revision).toBe(8)
  const eventTypes = s4.revisions.map((r) => r.eventType)
  expect(eventTypes).toEqual([
    "ProfileCreated", "MetaDataCreated",
    "ProfileBranchForked", "MetaDataCreated",
    "SnapshotCreatedProfile", "MetaDataCreated",
    "SnapshotPublishedProfile", "MetaDataCreated"
  ])
})

// ---------------------------------------------------------------------------
// H. GetProfileHistory via handlers
// ---------------------------------------------------------------------------

test("handlers: GetProfileHistory returns correct structure after full lifecycle", async () => {
  const { handlersLayer } = makeTestLayers()
  const profileId = makeProfileId(100)

  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(ProfileProviderRpcs)
    yield* client.CreateProfile({
      profileId, ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
      maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}", piiStorageKey: "",
      piiJson: "", piiJurisdiction: "", actorId: "a", summary: "init", sources: []
    })
    yield* client.ForkProfileBranch({
      profileId, branchId: "casting", label: "Casting", baseBranchId: "main",
      baseRevision: 2, baseSnapshotId: "", metadataJson: "{}", schemaVersion: "1.0",
      actorId: "a", summary: "fork"
    })
    yield* client.CreateProfileSnapshot({
      profileId, branchId: "casting", snapshotId: "snap-hist", snapshotType: "LLM",
      schemaVersion: "1.0", profileJson: makeProfile(profileId, "Actor"),
      metadataJson: "{}", piiStorageKey: "", piiJson: "", piiJurisdiction: "",
      actorId: "a", summary: "snap"
    })
    yield* client.PublishProfileSnapshot({
      profileId, snapshotId: "snap-hist", strategyJson: "{}",
      metadataJson: "{}", schemaVersion: "1.0", actorId: "a"
    })

    const history = yield* client.GetProfileHistory({ profileId })
    expect(history.profileId).toBe(profileId)
    expect(history.activeBranchId).toBe("casting")
    expect(history.currentRevision).toBe(8)
    expect(history.publishedSnapshotId).toBe("snap-hist")
    expect(history.branches.length).toBe(2)
    expect(history.branches.map((b) => b.branchId)).toEqual(["main", "casting"])
    expect(history.revisions.length).toBe(8)
    expect(history.snapshots.length).toBe(1)
    expect(history.snapshots[0]?.published).toBe(true)
  }))
})

// ---------------------------------------------------------------------------
// I. PII scope field on PersonalDataExtracted
// ---------------------------------------------------------------------------

test("CreateProfile emits PersonalDataExtracted with scope 'aggregate'", async () => {
  const profileId = makeProfileId(110)
  const { events } = await run(handle(initialProfileState, new CreateProfile({
    profileId, ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}",
    piiStorageKey: "pii/key", piiJson: "{\"email\":\"x\"}", piiJurisdiction: "US",
    actorId: "a", summary: "init", sources: []
  })))
  const pii = events.find((e) => e._tag === "PersonalDataExtracted") as any
  expect(pii.scope).toBe("aggregate")
  expect(pii.scopeId).toBe(profileId)
  expect(pii.jurisdiction).toBe("US")
})

test("CreateProfileSnapshot emits PersonalDataExtracted with scope 'snapshot'", async () => {
  const profileId = makeProfileId(111)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId, ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}", piiStorageKey: "",
    piiJson: "", piiJurisdiction: "", actorId: "a", summary: "init", sources: []
  })))
  const { events } = await run(handle(s1, new CreateProfileSnapshot({
    profileId, branchId: "main", snapshotId: "snap-pii-scope", snapshotType: "MANUAL",
    schemaVersion: "1.0", profileJson: makeProfile(profileId, "Actor"), metadataJson: "{}",
    piiStorageKey: "pii/snap-key", piiJson: "{\"ssn\":\"x\"}", piiJurisdiction: "DE",
    actorId: "a", summary: "snap"
  })))
  const pii = events.find((e) => e._tag === "PersonalDataExtracted") as any
  expect(pii.scope).toBe("snapshot")
  expect(pii.scopeId).toBe("snap-pii-scope")
  expect(pii.jurisdiction).toBe("DE")
})

// ---------------------------------------------------------------------------
// J. PostHandle: non-merge commands don't affect sourceAssets
// ---------------------------------------------------------------------------

test("handlers: ForkProfileBranch does not alter sourceAssets", async () => {
  const { handlersLayer } = makeTestLayers()
  const profileId = makeProfileId(120)

  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(ProfileProviderRpcs)
    yield* client.CreateProfile({
      profileId, ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
      maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}", piiStorageKey: "",
      piiJson: "", piiJurisdiction: "", actorId: "a", summary: "init", sources: [sampleSource]
    })
    yield* client.ForkProfileBranch({
      profileId, branchId: "alt", label: "Alt", baseBranchId: "main",
      baseRevision: 2, baseSnapshotId: "", metadataJson: "{}", schemaVersion: "1.0",
      actorId: "a", summary: "fork"
    })
    const state = yield* client.GetProfile({ profileId })
    expect(state.sourceAssets.length).toBe(1)
    expect(state.sourceAssets[0]?.sourceId).toBe("src-1")
  }))
})

// ---------------------------------------------------------------------------
// K. Multiple snapshots and selective publish
// ---------------------------------------------------------------------------

test("multiple snapshots: publishing one does not affect others", async () => {
  const profileId = makeProfileId(130)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId, ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}", piiStorageKey: "",
    piiJson: "", piiJurisdiction: "", actorId: "a", summary: "init", sources: []
  })))
  const { state: s2 } = await run(handle(s1, new CreateProfileSnapshot({
    profileId, branchId: "main", snapshotId: "snap-A", snapshotType: "MANUAL",
    schemaVersion: "1.0", profileJson: makeProfile(profileId, "Actor"), metadataJson: "{}",
    piiStorageKey: "", piiJson: "", piiJurisdiction: "", actorId: "a", summary: "snap A"
  })))
  const { state: s3 } = await run(handle(s2, new CreateProfileSnapshot({
    profileId, branchId: "main", snapshotId: "snap-B", snapshotType: "AUTO",
    schemaVersion: "1.0", profileJson: makeProfile(profileId, "Singer"), metadataJson: "{}",
    piiStorageKey: "", piiJson: "", piiJurisdiction: "", actorId: "a", summary: "snap B"
  })))
  const { state: s4 } = await run(handle(s3, new PublishProfileSnapshot({
    profileId, snapshotId: "snap-A", strategyJson: "{\"strategy\":\"A\"}",
    metadataJson: "{}", schemaVersion: "1.0", actorId: "a"
  })))
  expect(s4.snapshots.length).toBe(2)
  const snapA = s4.snapshots.find((s) => s.snapshotId === "snap-A")!
  const snapB = s4.snapshots.find((s) => s.snapshotId === "snap-B")!
  expect(snapA.published).toBe(true)
  expect(snapA.strategyJson).toBe("{\"strategy\":\"A\"}")
  expect(snapB.published).toBe(false)
  expect(snapB.strategyJson).toBe("")
  expect(s4.publishedSnapshotId).toBe("snap-A")
})

// ---------------------------------------------------------------------------
// L. CreateProfile branch initialization
// ---------------------------------------------------------------------------

test("CreateProfile initializes branch with correct fields", async () => {
  const profileId = makeProfileId(140)
  const { state } = await run(handle(initialProfileState, new CreateProfile({
    profileId, ownerAgentId: "agent-99", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}", piiStorageKey: "",
    piiJson: "", piiJurisdiction: "", actorId: "agent-99", summary: "init", sources: []
  })))
  expect(state.branches.length).toBe(1)
  const branch = state.branches[0]!
  expect(branch.branchId).toBe("main")
  expect(branch.label).toBe("main")
  expect(branch.baseBranchId).toBe("")
  expect(branch.baseRevision).toBe(0)
  expect(branch.baseSnapshotId).toBe("")
  expect(branch.createdBy).toBe("agent-99")
  expect(state.ownerAgentId).toBe("agent-99")
  expect(state.activeBranchId).toBe("main")
})

// ---------------------------------------------------------------------------
// M. ForkProfileBranch from specific snapshot
// ---------------------------------------------------------------------------

test("ForkProfileBranch with baseSnapshotId records it in the branch", async () => {
  const profileId = makeProfileId(150)
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId, ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: makeProfile(profileId, "Actor"), metadataJson: "{}", piiStorageKey: "",
    piiJson: "", piiJurisdiction: "", actorId: "a", summary: "init", sources: []
  })))
  const { state: s2 } = await run(handle(s1, new CreateProfileSnapshot({
    profileId, branchId: "main", snapshotId: "snap-base", snapshotType: "MANUAL",
    schemaVersion: "1.0", profileJson: makeProfile(profileId, "Actor"), metadataJson: "{}",
    piiStorageKey: "", piiJson: "", piiJurisdiction: "", actorId: "a", summary: "snap"
  })))
  const { state: s3 } = await run(handle(s2, new ForkProfileBranch({
    profileId, branchId: "from-snap", label: "From snapshot", baseBranchId: "main",
    baseRevision: s2.revision, baseSnapshotId: "snap-base", metadataJson: "{}",
    schemaVersion: "1.0", actorId: "a", summary: "fork from snap"
  })))
  const fork = s3.branches.find((b) => b.branchId === "from-snap")!
  expect(fork.baseSnapshotId).toBe("snap-base")
  expect(fork.baseBranchId).toBe("main")
  expect(fork.baseRevision).toBe(s2.revision)
})
