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
import { makeTestAggregate } from "../../src/framework/helpers/TestAggregate.js"
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
