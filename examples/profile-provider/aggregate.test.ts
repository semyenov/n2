/**
 * Profile provider service tests.
 *
 * Two layers of coverage:
 *
 *   1. Pure aggregate  — handle/evolve/decide with no infrastructure.
 *   2. Handler layer   — RpcTest.makeClient wired to in-memory services.
 *
 * No PostgreSQL required: EventJournal.layerMemory and an in-memory Map
 * for snapshots replace all SQL dependencies.
 *
 * RpcTest.makeClient(group) creates a direct-call client: it bypasses HTTP and
 * calls handler functions from the Layer in-process. Requires Scope (from
 * Effect.scoped) + Rpc.ToHandler<Rpcs> (from handlersLayer).
 */
import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as ExpEventJournal from "@effect/experimental/EventJournal"
import { EventLog as EL } from "@effect/experimental"
import { Identity } from "@effect/experimental/EventLog"
import * as EventLogApi from "@effect/experimental/EventLog"
import { RpcTest } from "@effect/rpc"
import { handle, initialProfileState } from "./aggregate.js"
import {
  CreateProfile,
  CreateProfileSnapshot,
  ForkProfileBranch,
  GetProfile,
  GetProfileHistory,
  MergeProfileData,
  ProfileError,
  ProfileNotFound,
  ProfileProviderRpcs,
  ProfileState,
  PublishProfileSnapshot,
  SourceAsset
} from "./contracts.js"
import { ProfileProviderEventGroup, ProfileProviderEventLogSchema } from "./events.js"
import { ProfileProviderHandlersRaw } from "./entity.js"
import { ProfileProviderSnapshots, type SnapshotEntry } from "./snapshots.js"

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

const testJournalLayer = ExpEventJournal.layerMemory
const testIdentityLayer = Layer.succeed(Identity, Identity.makeRandom())

const testEventLogLayer = EventLogApi.layer(ProfileProviderEventLogSchema).pipe(
  Layer.provide(NoOpProjection),
  Layer.provide(Layer.merge(testJournalLayer, testIdentityLayer))
)

const makeTestLayers = () => {
  const snapshotStore = new Map<string, SnapshotEntry>()
  const snapshotsLayer = Layer.succeed(ProfileProviderSnapshots, {
    load: (profileId: string) => Effect.succeed(Option.fromNullable(snapshotStore.get(profileId))),
    save: (profileId: string, state: SnapshotEntry["state"], revision: number) =>
      Effect.sync(() => {
        snapshotStore.set(profileId, { state, revision })
      })
  })

  return {
    snapshotStore,
    handlersLayer: Layer.provide(ProfileProviderHandlersRaw, Layer.mergeAll(
      testJournalLayer,
      testIdentityLayer,
      testEventLogLayer,
      snapshotsLayer
    ))
  }
}

const run = <A, E>(effect: Effect.Effect<A, E, never>): Promise<A> =>
  Effect.runPromise(effect)

const runWith = <A>(
  handlersLayer: Layer.Layer<any, any, never>,
  program: Effect.Effect<A, any, any>
): Promise<A> =>
  Effect.runPromise(
    Effect.scoped(program).pipe(
      Effect.provide(handlersLayer)
    ) as Effect.Effect<A, never, never>
  )

test("CreateProfile initializes draft state and metadata revisions", async () => {
  const { state, events } = await run(handle(initialProfileState, new CreateProfile({
    profileId: "profile-1",
    ownerAgentId: "agent-1",
    branchId: "main",
    schemaVersion: "1.0.0",
    maskedProfileJson: "{\"headline\":\"Actor\"}",
    metadataJson: "{\"channel\":\"resume\"}",
    piiStorageKey: "pii/profile-1",
    piiJson: "{\"email\":\"hidden\"}",
    piiJurisdiction: "DE",
    actorId: "agent-1",
    summary: "Create profile from resume",
    sources: [sampleSource]
  })))

  expect(state.status).toBe("draft")
  expect(state.profileId).toBe("profile-1")
  expect(state.sourceAssets.length).toBe(1)
  expect(state.revision).toBe(3)
  expect(events.map((event) => event._tag)).toEqual([
    "ProfileCreated",
    "MetaDataCreated",
    "PersonalDataExtracted"
  ])
})

test("MergeProfileData fails on unknown branch", async () => {
  const { state } = await run(handle(initialProfileState, new CreateProfile({
    profileId: "profile-2",
    ownerAgentId: "agent-1",
    branchId: "main",
    schemaVersion: "1.0.0",
    maskedProfileJson: "{}",
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "agent-1",
    summary: "Create",
    sources: [sampleSource]
  })))

  const error = await run(handle(state, new MergeProfileData({
    profileId: "profile-2",
    branchId: "alt",
    schemaVersion: "1.0.1",
    maskedProfileJson: "{\"headline\":\"Singer\"}",
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
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId: "profile-3",
    ownerAgentId: "agent-1",
    branchId: "main",
    schemaVersion: "1.0.0",
    maskedProfileJson: "{\"headline\":\"Performer\"}",
    metadataJson: "{}",
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "agent-1",
    summary: "Create",
    sources: [sampleSource]
  })))
  const { state: s2 } = await run(handle(s1, new ForkProfileBranch({
    profileId: "profile-3",
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
    profileId: "profile-3",
    branchId: "casting",
    snapshotId: "snap-1",
    snapshotType: "LLM",
    schemaVersion: "1.0.1",
    profileJson: "{\"headline\":\"Commercial talent\"}",
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
})

test("handlers: create, merge, snapshot, publish, history", async () => {
  const { handlersLayer } = makeTestLayers()
  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(ProfileProviderRpcs)
    yield* client.CreateProfile({
      profileId: "profile-h1",
      ownerAgentId: "agent-1",
      branchId: "main",
      schemaVersion: "1.0.0",
      maskedProfileJson: "{\"headline\":\"Actor\"}",
      metadataJson: "{\"channel\":\"resume\"}",
      piiStorageKey: "",
      piiJson: "",
      piiJurisdiction: "",
      actorId: "agent-1",
      summary: "Create",
      sources: [sampleSource]
    })
    yield* client.MergeProfileData({
      profileId: "profile-h1",
      branchId: "main",
      schemaVersion: "1.0.1",
      maskedProfileJson: "{\"headline\":\"Actor Singer\"}",
      metadataJson: "{\"channel\":\"chat\"}",
      piiStorageKey: "",
      piiJson: "",
      piiJurisdiction: "",
      actorId: "agent-1",
      summary: "Improve summary",
      sources: []
    })
    yield* client.CreateProfileSnapshot({
      profileId: "profile-h1",
      branchId: "main",
      snapshotId: "snap-h1",
      snapshotType: "MANUAL",
      schemaVersion: "1.0.1",
      profileJson: "{\"headline\":\"Actor Singer\"}",
      metadataJson: "{\"reviewed\":true}",
      piiStorageKey: "",
      piiJson: "",
      piiJurisdiction: "",
      actorId: "agent-1",
      summary: "Manual draft"
    })
    yield* client.PublishProfileSnapshot({
      profileId: "profile-h1",
      snapshotId: "snap-h1",
      strategyJson: "{\"segment\":\"commercial\"}",
      metadataJson: "{\"action\":\"publish\"}",
      schemaVersion: "1.0.1",
      actorId: "agent-1"
    })

    const history = yield* client.GetProfileHistory({ profileId: "profile-h1" })
    expect(history.profileId).toBe("profile-h1")
    expect(history.snapshots.length).toBe(1)
    expect(history.publishedSnapshotId).toBe("snap-h1")
  }))
})

test("handlers: unknown profile returns ProfileNotFound", async () => {
  const { handlersLayer } = makeTestLayers()
  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(ProfileProviderRpcs)
    const error = yield* client.GetProfileHistory({ profileId: "missing" }).pipe(Effect.flip)
    expect(error).toBeInstanceOf(ProfileNotFound)
  }))
})

// ---------------------------------------------------------------------------
// Pure aggregate: error guards
// ---------------------------------------------------------------------------

test("CreateProfile on existing profile fails", async () => {
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId: "e-1", ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: "{}", metadataJson: "{}", piiStorageKey: "", piiJson: "",
    piiJurisdiction: "", actorId: "a", summary: "init", sources: []
  })))
  const err = await run(handle(s1, new CreateProfile({
    profileId: "e-1", ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: "{}", metadataJson: "{}", piiStorageKey: "", piiJson: "",
    piiJurisdiction: "", actorId: "a", summary: "duplicate", sources: []
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
})

test("MergeProfileData on empty profile fails", async () => {
  const err = await run(handle(initialProfileState, new MergeProfileData({
    profileId: "e-2", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: "{}", metadataJson: "{}", piiStorageKey: "", piiJson: "",
    piiJurisdiction: "", actorId: "a", summary: "merge on empty", sources: []
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
})

test("ForkProfileBranch on empty profile fails", async () => {
  const err = await run(handle(initialProfileState, new ForkProfileBranch({
    profileId: "e-3", branchId: "alt", label: "Alt", baseBranchId: "", baseRevision: 0,
    baseSnapshotId: "", metadataJson: "{}", schemaVersion: "1.0", actorId: "a", summary: "fork"
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
})

test("ForkProfileBranch with duplicate branchId fails", async () => {
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId: "e-4", ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: "{}", metadataJson: "{}", piiStorageKey: "", piiJson: "",
    piiJurisdiction: "", actorId: "a", summary: "init", sources: []
  })))
  const err = await run(handle(s1, new ForkProfileBranch({
    profileId: "e-4", branchId: "main", label: "dup", baseBranchId: "main", baseRevision: s1.revision,
    baseSnapshotId: "", metadataJson: "{}", schemaVersion: "1.0", actorId: "a", summary: "dup fork"
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
})

test("CreateProfileSnapshot with duplicate snapshotId fails", async () => {
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId: "e-5", ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: "{}", metadataJson: "{}", piiStorageKey: "", piiJson: "",
    piiJurisdiction: "", actorId: "a", summary: "init", sources: []
  })))
  const { state: s2 } = await run(handle(s1, new CreateProfileSnapshot({
    profileId: "e-5", branchId: "main", snapshotId: "snap-e5", snapshotType: "MANUAL",
    schemaVersion: "1.0", profileJson: "{}", metadataJson: "{}", piiStorageKey: "", piiJson: "",
    piiJurisdiction: "", actorId: "a", summary: "first snap"
  })))
  const err = await run(handle(s2, new CreateProfileSnapshot({
    profileId: "e-5", branchId: "main", snapshotId: "snap-e5", snapshotType: "MANUAL",
    schemaVersion: "1.0", profileJson: "{}", metadataJson: "{}", piiStorageKey: "", piiJson: "",
    piiJurisdiction: "", actorId: "a", summary: "dup snap"
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
})

test("PublishProfileSnapshot on unknown snapshotId fails", async () => {
  const { state: s1 } = await run(handle(initialProfileState, new CreateProfile({
    profileId: "e-6", ownerAgentId: "a", branchId: "main", schemaVersion: "1.0",
    maskedProfileJson: "{}", metadataJson: "{}", piiStorageKey: "", piiJson: "",
    piiJurisdiction: "", actorId: "a", summary: "init", sources: []
  })))
  const err = await run(handle(s1, new PublishProfileSnapshot({
    profileId: "e-6", snapshotId: "no-such-snap", strategyJson: "{}",
    metadataJson: "{}", schemaVersion: "1.0", actorId: "a"
  })).pipe(Effect.flip))
  expect(err._tag).toBe("ProfileError")
})

// ---------------------------------------------------------------------------
// Handler integration: GetProfile + snapshot recovery
// ---------------------------------------------------------------------------

test("handlers: GetProfile returns current state", async () => {
  const { handlersLayer } = makeTestLayers()
  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(ProfileProviderRpcs)
    yield* client.CreateProfile({
      profileId: "h-get-1", ownerAgentId: "agent-1", branchId: "main", schemaVersion: "1.0",
      maskedProfileJson: "{\"headline\":\"Vocalist\"}", metadataJson: "{}", piiStorageKey: "",
      piiJson: "", piiJurisdiction: "", actorId: "agent-1", summary: "Create", sources: []
    })
    const state = yield* client.GetProfile({ profileId: "h-get-1" })
    expect(state.profileId).toBe("h-get-1")
    expect(state.status).toBe("draft")
    expect(state.maskedProfileJson).toBe("{\"headline\":\"Vocalist\"}")
  }))
})

test("handlers: GetProfile for unknown profile returns ProfileNotFound", async () => {
  const { handlersLayer } = makeTestLayers()
  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(ProfileProviderRpcs)
    const err = yield* client.GetProfile({ profileId: "no-profile" }).pipe(Effect.flip)
    expect(err._tag).toBe("ProfileNotFound")
    expect((err as unknown as ProfileNotFound).profileId).toBe("no-profile")
  }))
})

test("snapshot recovery: GetProfile loads state from snapshot store", async () => {
  const { snapshotStore, handlersLayer } = makeTestLayers()

  // Simulate a server restart: pre-seed snapshot store with a known state.
  // The handler's SynchronizedRef starts empty — it must fall back to the snapshot.
  snapshotStore.set("snap-recovery-1", {
    state: new ProfileState({
      status: "draft",
      profileId: "snap-recovery-1",
      ownerAgentId: "agent-x",
      activeBranchId: "main",
      currentSchemaVersion: "2.0",
      maskedProfileJson: "{\"headline\":\"Recovered\"}",
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
    const state = yield* client.GetProfile({ profileId: "snap-recovery-1" })
    expect(state.status).toBe("draft")
    expect(state.profileId).toBe("snap-recovery-1")
    expect(state.maskedProfileJson).toBe("{\"headline\":\"Recovered\"}")
    expect(state.ownerAgentId).toBe("agent-x")
  }))
})
