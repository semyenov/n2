import { makeFetchClient } from "@semyenov/n2/helpers"
import { ProfileProviderRpcs } from "../services/profile-provider/src/contracts.js"
import { RequestProviderRpcs } from "../services/request-provider/src/contracts.js"

const profileBaseUrl = process.env.PROFILE_PROVIDER_BASE_URL ?? "http://127.0.0.1:4100"
const requestBaseUrl = process.env.REQUEST_PROVIDER_BASE_URL ?? "http://127.0.0.1:4110"

const profileClient = makeFetchClient(ProfileProviderRpcs, `${profileBaseUrl}/rpc/profile-provider`)
const requestClient = makeFetchClient(RequestProviderRpcs, `${requestBaseUrl}/rpc/request-provider`)

const makeUuid = () => crypto.randomUUID()

const assert = (condition: unknown, message: string) => {
  if (!condition) throw new Error(message)
}

const assertEqual = <Actual, Expected extends Actual>(
  actual: Actual,
  expected: Expected,
  message: string
) => {
  if (actual !== expected) {
    throw new Error(`${message}: expected ${String(expected)}, got ${String(actual)}`)
  }
}

const assertArrayEqual = (
  actual: ReadonlyArray<string | number>,
  expected: ReadonlyArray<string | number>,
  message: string
) => {
  assert(
    actual.length === expected.length && actual.every((value, index) => value === expected[index]),
    `${message}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`
  )
}

const assertRejectsWithTag = async (
  name: string,
  action: () => Promise<unknown>,
  expectedTag: string
) => {
  try {
    await action()
  } catch (error) {
    const tag = typeof error === "object" && error !== null && "_tag" in error
      ? String((error as { readonly _tag: unknown })._tag)
      : ""
    assertEqual(tag, expectedTag, `${name} returned wrong error tag`)
    console.log(`${name}: ${expectedTag} ok`)
    return
  }

  throw new Error(`${name}: expected ${expectedTag}`)
}

const waitForHealth = async (name: string, url: string) => {
  const deadline = Date.now() + 30_000
  let lastError = ""

  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${url}/health`)
      if (response.ok) {
        console.log(`${name}: health ok`)
        return
      }
      lastError = `HTTP ${response.status}`
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error)
    }

    await Bun.sleep(1_000)
  }

  throw new Error(`${name}: health check failed: ${lastError}`)
}

const makeSource = (kind: "profile" | "request") => ({
  sourceId: `${kind}-source-1`,
  kind: "manual" as const,
  uri: `manual://${kind}/smoke-test`,
  mediaType: "application/json",
  storageKey: `${kind}-smoke-test.json`,
  summary: `${kind} smoke-test source`
})

const makeExtraSource = (kind: "profile" | "request") => ({
  sourceId: `${kind}-source-2`,
  kind: "url" as const,
  uri: `https://example.test/${kind}/smoke-test`,
  mediaType: "text/html",
  storageKey: `${kind}-smoke-test.html`,
  summary: `${kind} secondary source`
})

const json = (value: unknown) => JSON.stringify(value)

const makeProfileDocument = (profileId: string, position = "Senior TypeScript Engineer") => ({
  uuid: profileId,
  created_at: "2026-01-01T00:00:00.000Z",
  updated_at: "2026-01-02T00:00:00.000Z",
  user_data: {
    personal_info: {
      first_name: "Ada",
      last_name: "Lovelace",
      relevant_position: position,
      citizenship: "GB",
      residence: "Berlin",
      relocation: true
    },
    contacts: {
      phone: ["+4915123456789"] as [string, ...string[]],
      email: ["ada@example.test"] as [string, ...string[]],
      github: "ada-lovelace"
    },
    about: "Builds reliable TypeScript systems around Effect and distributed workflows.",
    salary_expectations: {
      currency: "USD",
      amount_from: 1000,
      amount_to: 1600
    },
    employment_types: ["full_time", "contract"],
    work_schedule: ["remote", "flexible"],
    skills: [
      {
        name: "TypeScript",
        level: "advanced",
        years_of_experience: 5
      },
      {
        name: "Effect",
        level: "advanced",
        years_of_experience: 3
      }
    ],
    tools_proficiency: [
      { tool: "Bun", level: "advanced" },
      { tool: "PostgreSQL", level: "intermediate" }
    ],
    soft_skills: ["written communication", "systems thinking"],
    languages: [
      { name: "English", level: "C2" },
      { name: "German", level: "B1" }
    ],
    education: [
      {
        degree: "Bachelor",
        field_of_study: "Computer Science",
        institution: "Analytical Engine Institute",
        start_year: 2018,
        end_year: 2022,
        diploma_with_honors: true
      }
    ],
    online_courses: [
      { title: "Distributed Systems with TypeScript", platform: "Smoke Test Academy" }
    ],
    work_experience: [
      {
        position: "Platform Engineer",
        company: "Analytical Engine Labs",
        start_date: "2022-01-01",
        end_date: null,
        job_format: "remote",
        responsibilities: ["Designed event-sourced services", "Maintained observability pipelines"],
        skills_used: ["TypeScript", "Effect", "PostgreSQL"]
      }
    ],
    portfolio: [
      { title: "Workflow orchestration demo", url: "https://example.test/portfolio/workflows" }
    ]
  },
  user_matching_data: {
    application_history: [{ status: "screened", source: "smoke-test" }],
    feedback_history: [{ rating: 5, note: "strong distributed systems background" }]
  },
  user_meta_data: {
    version: 1,
    source_platform: "smoke-test",
    tags: ["effect", "typescript", "distributed-systems"]
  }
})

const makeRequestDocument = (requestId: string, position = "Senior TypeScript Engineer") => ({
  id: Math.floor(Math.random() * 1_000_000),
  uuid: requestId,
  created_at: "2026-01-01T00:00:00.000Z",
  updated_at: "2026-01-02T00:00:00.000Z",
  vacancy_data: {
    relevant_position: position,
    company: "Acme",
    country: "Germany",
    location: "Berlin",
    job_format: "remote",
    industry: "SaaS",
    description: "Own event-sourced service boundaries and operational tooling.",
    technology_stack: ["TypeScript", "Effect"],
    requirements: {
      experience_years: 5,
      hard_skills: [
        { name: "TypeScript", level: "advanced" },
        { name: "Effect", level: "advanced" }
      ],
      soft_skills: [
        { name: "cross-team communication", required: true },
        { name: "mentoring", required: false }
      ],
      languages: [
        { name: "English", level: "C1", required: true },
        { name: "German", level: "B1" }
      ],
      education: [
        { degree: "Bachelor", field_of_study: "Computer Science", required: false }
      ],
      certifications: [
        { title: "Cloud architecture", required: false }
      ],
      open_source_contributions: true,
      team_leadership_experience: false,
      portfolio_required: true
    },
    salary_offer: {
      currency: "EUR",
      amount_from: 7000,
      amount_to: 9000,
      bonus: "annual performance bonus"
    },
    responsibilities: ["Build service APIs", "Improve event replay tooling"],
    employment_types: ["full_time"],
    work_schedule: ["remote"],
    benefits: ["equipment budget", "conference budget"],
    red_flags: ["legacy-only role"]
  },
  vacancy_meta_data: {
    source_platform: "smoke-test",
    version: 1,
    tags: ["effect", "platform"]
  }
})

const testProfileProvider = async () => {
  const profileId = makeUuid()
  const profileJson = makeProfileDocument(profileId)
  const mergedProfileJson = makeProfileDocument(profileId, "Principal Effect Engineer")
  const branchProfileJson = makeProfileDocument(profileId, "Principal Platform Engineer")
  const source = makeSource("profile")
  const extraSource = makeExtraSource("profile")
  const snapshotId = `snapshot-${profileId}`

  const created = await profileClient.CreateProfile({
    profileId,
    ownerAgentId: "smoke-test-agent",
    branchId: "main",
    schemaVersion: "1.0.0",
    maskedProfileJson: profileJson,
    metadataJson: json({ source: "smoke-test", stage: "create" }),
    piiStorageKey: `pii/profile/${profileId}/create.json`,
    piiJson: json({ email: "ada@example.test", phone: "+4915123456789" }),
    piiJurisdiction: "DE",
    actorId: "smoke-test-agent",
    summary: "Create profile from smoke test",
    sources: [source]
  })

  assertEqual(created.profileId, profileId, "profile-provider: CreateProfile returned wrong profileId")
  assertEqual(created.revision, 3, "profile-provider: CreateProfile should produce revision 3 with PII")

  const merged = await profileClient.MergeProfileData({
    profileId,
    branchId: "main",
    schemaVersion: "1.1.0",
    maskedProfileJson: mergedProfileJson,
    metadataJson: json({ source: "smoke-test", stage: "merge", confidence: 0.98 }),
    piiStorageKey: "",
    piiJson: "",
    piiJurisdiction: "",
    actorId: "smoke-test-agent",
    summary: "Merge richer profile data from smoke test",
    sources: [
      { ...source, summary: "profile smoke-test source updated by merge" },
      extraSource
    ]
  })

  assertEqual(merged.revision, 5, "profile-provider: MergeProfileData should advance to revision 5")

  const forked = await profileClient.ForkProfileBranch({
    profileId,
    branchId: "review",
    label: "Review branch",
    baseBranchId: "main",
    baseRevision: merged.revision,
    baseSnapshotId: "",
    metadataJson: json({ source: "smoke-test", stage: "branch" }),
    schemaVersion: "1.1.0",
    actorId: "smoke-test-agent",
    summary: "Fork review branch from smoke test"
  })

  assertEqual(forked.branchId, "review", "profile-provider: ForkProfileBranch returned wrong active branch")
  assertEqual(forked.revision, 7, "profile-provider: ForkProfileBranch should advance to revision 7")

  const snapshotted = await profileClient.CreateProfileSnapshot({
    profileId,
    branchId: "review",
    snapshotId,
    snapshotType: "MANUAL",
    schemaVersion: "1.2.0",
    profileJson: branchProfileJson,
    metadataJson: json({ source: "smoke-test", stage: "snapshot", reviewer: "qa" }),
    piiStorageKey: `pii/profile/${profileId}/snapshot.json`,
    piiJson: json({ passport: "masked", tax_id: "masked" }),
    piiJurisdiction: "DE",
    actorId: "smoke-test-agent",
    summary: "Create profile snapshot from smoke test"
  })

  assertEqual(snapshotted.revision, 10, "profile-provider: CreateProfileSnapshot should advance to revision 10 with PII")

  const published = await profileClient.PublishProfileSnapshot({
    profileId,
    snapshotId,
    strategyJson: json({ audience: "platform", priority: "high" }),
    metadataJson: json({ source: "smoke-test", stage: "publish" }),
    schemaVersion: "1.2.0",
    actorId: "smoke-test-agent"
  })

  assertEqual(published.revision, 12, "profile-provider: PublishProfileSnapshot should advance to revision 12")

  const profile = await profileClient.GetProfile({ profileId })
  assertEqual(profile.status, "published", "profile-provider: GetProfile should return published state")
  assertEqual(profile.activeBranchId, "review", "profile-provider: GetProfile should return review as active branch")
  assertEqual(profile.revision, 12, "profile-provider: GetProfile returned wrong revision")
  assertEqual(profile.maskedProfileJson?.uuid, profileId, "profile-provider: GetProfile returned wrong profile document")
  assertEqual(
    profile.maskedProfileJson?.user_data.personal_info.relevant_position,
    "Principal Effect Engineer",
    "profile-provider: MergeProfileData did not update the current document"
  )
  assertEqual(profile.latestPiiStorageKey, `pii/profile/${profileId}/snapshot.json`, "profile-provider: latest PII key not updated")
  assertEqual(profile.piiJurisdiction, "DE", "profile-provider: PII jurisdiction not updated")
  assertEqual(profile.sourceAssets.length, 2, "profile-provider: source assets should be deduplicated and merged")
  assertEqual(profile.sourceAssets[0]?.summary, "profile smoke-test source updated by merge", "profile-provider: duplicate source should be overwritten")

  const history = await profileClient.GetProfileHistory({ profileId })
  assertEqual(history.currentRevision, 12, "profile-provider: history returned wrong current revision")
  assertEqual(history.publishedSnapshotId, snapshotId, "profile-provider: history returned wrong published snapshot")
  assertArrayEqual(
    history.branches.map((branch) => branch.branchId),
    ["main", "review"],
    "profile-provider: history returned wrong branches"
  )
  assertArrayEqual(
    history.revisions.map((entry) => entry.eventType),
    [
      "ProfileCreated",
      "MetaDataCreated",
      "PersonalDataExtracted",
      "MergedDataProfile",
      "MetaDataCreated",
      "ProfileBranchForked",
      "MetaDataCreated",
      "SnapshotCreatedProfile",
      "MetaDataCreated",
      "PersonalDataExtracted",
      "SnapshotPublishedProfile",
      "MetaDataCreated"
    ],
    "profile-provider: history returned wrong event sequence"
  )
  assertEqual(history.snapshots.length, 1, "profile-provider: GetProfileHistory should include one snapshot")
  assertEqual(history.snapshots[0]?.published, true, "profile-provider: snapshot should be published")
  assertEqual(
    history.snapshots[0]?.profileJson.user_data.personal_info.relevant_position,
    "Principal Platform Engineer",
    "profile-provider: snapshot should preserve the snapshot document"
  )

  await assertRejectsWithTag(
    "profile-provider: unknown profile read",
    () => profileClient.GetProfile({ profileId: makeUuid() }),
    "ProfileNotFound"
  )

  console.log(`profile-provider: complex lifecycle ok (${profileId})`)
}

const testRequestProvider = async () => {
  const requestId = makeUuid()
  const requestJson = makeRequestDocument(requestId)
  const updatedRequestJson = makeRequestDocument(requestId, "Principal Effect Engineer")
  const snapshotRequestJson = makeRequestDocument(requestId, "Staff Platform Engineer")
  const finalRequestJson = makeRequestDocument(requestId, "Staff Distributed Systems Engineer")
  const source = makeSource("request")
  const extraSource = makeExtraSource("request")
  const firstSnapshotId = `snapshot-${requestId}-manual`
  const secondSnapshotId = `snapshot-${requestId}-llm`

  const created = await requestClient.CreateRequest({
    requestId,
    clientId: "smoke-test-client",
    schemaVersion: "1.0.0",
    requestJson,
    metadataJson: json({ source: "smoke-test", stage: "create" }),
    actorId: "smoke-test-client",
    summary: "Create request from smoke test",
    sources: [source]
  })

  assertEqual(created.requestId, requestId, "request-provider: CreateRequest returned wrong requestId")
  assertEqual(created.revision, 2, "request-provider: CreateRequest should produce revision 2")

  const updated = await requestClient.UpdateRequest({
    requestId,
    schemaVersion: "1.1.0",
    requestJson: updatedRequestJson,
    metadataJson: json({ source: "smoke-test", stage: "update", score: 0.91 }),
    actorId: "smoke-test-client",
    summary: "Update request from smoke test",
    sources: [
      { ...source, summary: "request smoke-test source updated by update" },
      extraSource
    ]
  })

  assertEqual(updated.revision, 4, "request-provider: UpdateRequest should advance to revision 4")

  const snapshotted = await requestClient.CreateRequestSnapshot({
    requestId,
    snapshotId: firstSnapshotId,
    snapshotType: "MANUAL",
    schemaVersion: "1.2.0",
    requestJson: snapshotRequestJson,
    metadataJson: json({ source: "smoke-test", stage: "manual-snapshot" }),
    actorId: "smoke-test-client",
    summary: "Create request snapshot from smoke test"
  })

  assertEqual(snapshotted.revision, 6, "request-provider: CreateRequestSnapshot should advance to revision 6")
  assertEqual(snapshotted.latestSnapshotId, firstSnapshotId, "request-provider: first snapshot id not returned")

  const updatedAfterSnapshot = await requestClient.UpdateRequest({
    requestId,
    schemaVersion: "1.3.0",
    requestJson: finalRequestJson,
    metadataJson: json({ source: "smoke-test", stage: "post-snapshot-update" }),
    actorId: "smoke-test-client",
    summary: "Update request after first snapshot from smoke test",
    sources: []
  })

  assertEqual(updatedAfterSnapshot.revision, 8, "request-provider: second UpdateRequest should advance to revision 8")

  const secondSnapshot = await requestClient.CreateRequestSnapshot({
    requestId,
    snapshotId: secondSnapshotId,
    snapshotType: "LLM",
    schemaVersion: "1.4.0",
    requestJson: finalRequestJson,
    metadataJson: json({ source: "smoke-test", stage: "llm-snapshot" }),
    actorId: "smoke-test-client",
    summary: "Create LLM request snapshot from smoke test"
  })

  assertEqual(secondSnapshot.revision, 10, "request-provider: second snapshot should advance to revision 10")
  assertEqual(secondSnapshot.latestSnapshotId, secondSnapshotId, "request-provider: second snapshot id not returned")

  const request = await requestClient.GetRequest({ requestId })
  assertEqual(request.status, "snapshotted", "request-provider: GetRequest should return snapshotted state")
  assertEqual(request.revision, 10, "request-provider: GetRequest returned wrong revision")
  assertEqual(request.currentSchemaVersion, "1.4.0", "request-provider: GetRequest returned wrong schema version")
  assertEqual(request.requestJson?.uuid, requestId, "request-provider: GetRequest returned wrong request document")
  assertEqual(
    request.requestJson?.vacancy_data.relevant_position,
    "Staff Distributed Systems Engineer",
    "request-provider: final request document not preserved"
  )
  assertEqual(request.sourceAssets.length, 2, "request-provider: source assets should be deduplicated and merged")
  assertEqual(request.sourceAssets[0]?.summary, "request smoke-test source updated by update", "request-provider: duplicate source should be overwritten")

  const history = await requestClient.GetRequestHistory({ requestId })
  assertEqual(history.currentRevision, 10, "request-provider: history returned wrong current revision")
  assertEqual(history.latestSnapshotId, secondSnapshotId, "request-provider: history returned wrong latest snapshot")
  assertArrayEqual(
    history.revisions.map((entry) => entry.eventType),
    [
      "RequestCreated",
      "RequestMetaDataCreated",
      "RequestUpdated",
      "RequestMetaDataCreated",
      "RequestSnapshotCreated",
      "RequestMetaDataCreated",
      "RequestUpdated",
      "RequestMetaDataCreated",
      "RequestSnapshotCreated",
      "RequestMetaDataCreated"
    ],
    "request-provider: history returned wrong event sequence"
  )
  assertEqual(history.snapshots.length, 2, "request-provider: GetRequestHistory should include two snapshots")
  assertArrayEqual(
    history.snapshots.map((snapshot) => snapshot.snapshotId),
    [firstSnapshotId, secondSnapshotId],
    "request-provider: history returned wrong snapshot order"
  )

  await assertRejectsWithTag(
    "request-provider: unknown request read",
    () => requestClient.GetRequest({ requestId: makeUuid() }),
    "RequestNotFound"
  )

  console.log(`request-provider: complex lifecycle ok (${requestId})`)
}

await waitForHealth("profile-provider", profileBaseUrl)
await waitForHealth("request-provider", requestBaseUrl)
await testProfileProvider()
await testRequestProvider()

console.log("services: smoke tests passed")
