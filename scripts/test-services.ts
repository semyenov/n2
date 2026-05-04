import * as DateTime from "effect/DateTime"
import { makeFetchClient } from "@semyenov/n2/helpers"
import { ProfileProviderRpcs } from "../services/profile-provider/src/contracts/commands.js"
import { RequestProviderRpcs } from "../services/request-provider/src/contracts/commands.js"
import { PIIProviderRpcs } from "../services/pii-provider/src/contracts/commands.js"

const profileBaseUrl = process.env.PROFILE_PROVIDER_BASE_URL ?? "http://127.0.0.1:4100"
const requestBaseUrl = process.env.REQUEST_PROVIDER_BASE_URL ?? "http://127.0.0.1:4110"
const piiBaseUrl = process.env.PII_PROVIDER_BASE_URL ?? "http://127.0.0.1:4120"

const profileClient = makeFetchClient(ProfileProviderRpcs, `${profileBaseUrl}/rpc/profile-provider`)
const requestClient = makeFetchClient(RequestProviderRpcs, `${requestBaseUrl}/rpc/request-provider`)
const piiClient = makeFetchClient(PIIProviderRpcs, `${piiBaseUrl}/rpc/pii-provider`)

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

const runStep = async <A>(
  name: string,
  action: () => Promise<A>
) => {
  console.log(`${name}: start`)
  const result = await action()
  console.log(`${name}: ok`)
  return result
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

  console.log(`${name}: waiting for health at ${url}/health`)

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
const sqlString = (value: string) => `'${value.replaceAll("'", "''")}'`

const queryPostgresScalar = async (query: string) => {
  const process = Bun.spawn([
    "docker",
    "compose",
    "exec",
    "-T",
    "postgres",
    "psql",
    "-U",
    "n2",
    "-d",
    "n2",
    "-t",
    "-A",
    "-c",
    query
  ], {
    stdout: "pipe",
    stderr: "pipe"
  })
  const [stdout, stderr, code] = await Promise.all([
    new Response(process.stdout).text(),
    new Response(process.stderr).text(),
    process.exited
  ])

  if (code !== 0) {
    throw new Error(`Postgres query failed (${code}): ${stderr.trim()}`)
  }

  return stdout.trim()
}

const verifyEventJournalCount = async (
  name: string,
  table: string,
  primaryKey: string,
  expectedCount: number
) => {
  const count = Number(await runStep(`${name}: verify event journal entries`, () =>
    queryPostgresScalar(`
      SELECT count(*)
      FROM ${table}
      WHERE primary_key = ${sqlString(primaryKey)}
    `)
  ))
  assertEqual(count, expectedCount, `${name}: event journal entry count mismatch`)
}

const makeProfileDocument = (profileId: string, position = "Senior TypeScript Engineer") => ({
  uuid: profileId,
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-02T00:00:00.000Z",
  userData: {
    personalInfo: {
      firstName: "Ada",
      lastName: "Lovelace",
      relevantPosition: position,
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
    salaryExpectations: {
      currency: "USD",
      amountFrom: 1000,
      amountTo: 1600
    },
    employmentTypes: ["full_time", "contract"],
    workSchedule: ["remote", "flexible"],
    skills: [
      {
        name: "TypeScript",
        level: "advanced",
        yearsOfExperience: 5
      },
      {
        name: "Effect",
        level: "advanced",
        yearsOfExperience: 3
      }
    ],
    toolsProficiency: [
      { tool: "Bun", level: "advanced" },
      { tool: "PostgreSQL", level: "intermediate" }
    ],
    softSkills: ["written communication", "systems thinking"],
    languages: [
      { name: "English", level: "C2" },
      { name: "German", level: "B1" }
    ],
    education: [
      {
        degree: "Bachelor",
        fieldOfStudy: "Computer Science",
        institution: "Analytical Engine Institute",
        startYear: 2018,
        endYear: 2022,
        diplomaWithHonors: true
      }
    ],
    onlineCourses: [
      { title: "Distributed Systems with TypeScript", platform: "Smoke Test Academy" }
    ],
    workExperience: [
      {
        position: "Platform Engineer",
        company: "Analytical Engine Labs",
        startDate: "2022-01-01",
        jobFormat: "remote",
        responsibilities: ["Designed event-sourced services", "Maintained observability pipelines"],
        skillsUsed: ["TypeScript", "Effect", "PostgreSQL"]
      }
    ],
    portfolio: [
      { title: "Workflow orchestration demo", url: "https://example.test/portfolio/workflows" }
    ]
  },
  userMatchingData: {
    applicationHistory: [{ status: "screened", source: "smoke-test" }],
    feedbackHistory: [{ rating: 5, note: "strong distributed systems background" }]
  },
  userMetaData: {
    version: 1,
    sourcePlatform: "smoke-test",
    tags: ["effect", "typescript", "distributed-systems"]
  }
})

const makeRequestDocument = (requestId: string, position = "Senior TypeScript Engineer") => ({
  id: Math.floor(Math.random() * 1_000_000),
  uuid: requestId,
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-02T00:00:00.000Z",
  vacancyData: {
    relevantPosition: position,
    company: "Acme",
    country: "Germany",
    location: "Berlin",
    jobFormat: "remote",
    industry: "SaaS",
    description: "Own event-sourced service boundaries and operational tooling.",
    technologyStack: ["TypeScript", "Effect"],
    requirements: {
      experienceYears: 5,
      hardSkills: [
        { name: "TypeScript", level: "advanced" },
        { name: "Effect", level: "advanced" }
      ],
      softSkills: [
        { name: "cross-team communication", required: true },
        { name: "mentoring", required: false }
      ],
      languages: [
        { name: "English", level: "C1", required: true },
        { name: "German", level: "B1" }
      ],
      education: [
        { degree: "Bachelor", fieldOfStudy: "Computer Science", required: false }
      ],
      certifications: [
        { title: "Cloud architecture", required: false }
      ],
      openSourceContributions: true,
      teamLeadershipExperience: false,
      portfolioRequired: true
    },
    salaryOffer: {
      currency: "EUR",
      amountFrom: 7000,
      amountTo: 9000,
      bonus: "annual performance bonus"
    },
    responsibilities: ["Build service APIs", "Improve event replay tooling"],
    employmentTypes: ["full_time"],
    workSchedule: ["remote"],
    benefits: ["equipment budget", "conference budget"],
    redFlags: ["legacy-only role"]
  },
  vacancyMetaData: {
    sourcePlatform: "smoke-test",
    version: 1,
    tags: ["effect", "platform"]
  }
})

const makePIIRecord = (recordId: string, profileId: string, actorId: string) => ({
  id: recordId,
  schemaVersion: "1.0.0",
  entityReference: {
    entityId: profileId,
    entityType: "AGGREGATE" as const,
    entityVersion: 1
  },
  jurisdiction: {
    countryCode: "RU",
    applicableLaws: ["152-FZ" as const],
    dataResidency: "RU"
  },
  personalIdentity: {
    fullName: {
      firstName: "Ada",
      lastName: "Lovelace"
    },
    citizenship: ["RU"]
  },
  contactData: {
    phones: [{ number: "+79001234567", type: "MOBILE" as const, verified: true }],
    emails: [{ address: "ada@example.test", type: "WORK" as const, verified: true }]
  },
  consent: {
    given: true,
    givenAt: DateTime.unsafeMake("2026-01-01T00:00:00.000Z"),
    consentVersion: "1.0",
    purposes: ["PROFILE_MATCHING" as const],
    withdrawalRequested: false
  },
  dataSubjectRequests: [],
  retention: {
    policyId: "retention-ru-standard",
    retentionPeriodDays: 1095,
    legalHold: false
  },
  audit: {
    createdAt: DateTime.unsafeMake("2026-01-01T00:00:00.000Z"),
    createdBy: actorId,
    accessCount: 0
  },
  extractionInfo: {
    extractedFields: ["userData.personalInfo.firstName", "userData.contacts.email"],
    extractionMethod: "LLM_DETECTION" as const,
    confidenceScores: { fullName: 0.99, emails: 0.98 }
  },
  status: "ACTIVE" as const
})

const verifyProfilePostgresProjection = async (
  profileId: string,
  snapshotId: string,
  latestPiiStorageKey: string
) => {
  const escapedProfileId = sqlString(profileId)
  const escapedSnapshotId = sqlString(snapshotId)

  const profileState = await runStep("profile-provider: verify Postgres profile projection", () =>
    queryPostgresScalar(`
      SELECT concat_ws(
        '|',
        status,
        current_revision::text,
        active_branch_id,
        current_schema_version,
        latest_pii_storage_key,
        pii_jurisdiction,
        published_snapshot_id
      )
      FROM profile_provider_profiles_read
      WHERE profile_id = ${escapedProfileId}
    `)
  )
  assertEqual(
    profileState,
    ["published", "12", "review", "1.2.0", latestPiiStorageKey, "DE", snapshotId].join("|"),
    "profile-provider: Postgres profile projection mismatch"
  )

  const snapshotState = await runStep("profile-provider: verify Postgres snapshot projection", () =>
    queryPostgresScalar(`
      SELECT concat_ws(
        '|',
        published::text,
        revision::text,
        snapshot_type,
        schema_version,
        profile_json::jsonb #>> '{userData,personalInfo,relevantPosition}',
        strategy_json::jsonb ->> 'priority'
      )
      FROM profile_provider_snapshots_read
      WHERE snapshot_id = ${escapedSnapshotId}
    `)
  )
  assertEqual(
    snapshotState,
    "true|8|MANUAL|1.2.0|Principal Platform Engineer|high",
    "profile-provider: Postgres snapshot projection mismatch"
  )

  await verifyEventJournalCount("profile-provider", "profile_provider_event_journal", profileId, 12)
}

const verifyRequestPostgresProjection = async (
  requestId: string,
  firstSnapshotId: string,
  secondSnapshotId: string
) => {
  const escapedRequestId = sqlString(requestId)

  const requestState = await runStep("request-provider: verify Postgres request projection", () =>
    queryPostgresScalar(`
      SELECT concat_ws(
        '|',
        status,
        current_revision::text,
        current_schema_version,
        latest_snapshot_id,
        request_json::jsonb #>> '{vacancyData,relevantPosition}'
      )
      FROM request_provider_requests_read
      WHERE request_id = ${escapedRequestId}
    `)
  )
  assertEqual(
    requestState,
    ["snapshotted", "9", "1.4.0", secondSnapshotId, "Staff Distributed Systems Engineer"].join("|"),
    "request-provider: Postgres request projection mismatch"
  )

  const snapshotState = await runStep("request-provider: verify Postgres snapshot projections", () =>
    queryPostgresScalar(`
      SELECT string_agg(
        snapshot_id || ':' ||
        snapshot_type || ':' ||
        revision::text || ':' ||
        (request_json::jsonb #>> '{vacancyData,relevantPosition}'),
        ','
        ORDER BY revision
      )
      FROM request_provider_snapshots_read
      WHERE request_id = ${escapedRequestId}
    `)
  )
  assertEqual(
    snapshotState,
    [
      `${firstSnapshotId}:MANUAL:5:Staff Platform Engineer`,
      `${secondSnapshotId}:LLM:9:Staff Distributed Systems Engineer`
    ].join(","),
    "request-provider: Postgres snapshot projection mismatch"
  )

  await verifyEventJournalCount("request-provider", "request_provider_event_journal", requestId, 10)
}

const verifyPIIPostgresProjection = async (
  storageKey: string,
  subjectRequestId: string,
  erasureRequestId: string
) => {
  const escapedStorageKey = sqlString(storageKey)
  const escapedSubjectRequestId = sqlString(subjectRequestId)
  const escapedErasureRequestId = sqlString(erasureRequestId)

  const recordState = await runStep("pii-provider: verify Postgres record projection", () =>
    queryPostgresScalar(`
      SELECT status || ':' || revision
      FROM pii_provider_records
      WHERE storage_key = ${escapedStorageKey}
    `)
  )
  assertEqual(recordState, "DELETED:14", "pii-provider: Postgres record projection mismatch")

  const auditCount = Number(await runStep("pii-provider: verify Postgres audit projection", () =>
    queryPostgresScalar(`
      SELECT count(*)
      FROM pii_provider_audit_log
      WHERE storage_key = ${escapedStorageKey}
    `)
  ))
  assertEqual(auditCount, 14, "pii-provider: Postgres audit projection mismatch")

  const subjectRequestStatus = await runStep("pii-provider: verify Postgres access request projection", () =>
    queryPostgresScalar(`
      SELECT status
      FROM pii_provider_subject_requests
      WHERE request_id = ${escapedSubjectRequestId}
        AND storage_key = ${escapedStorageKey}
    `)
  )
  assertEqual(subjectRequestStatus, "COMPLETED", "pii-provider: Postgres subject request projection mismatch")

  const erasureRequestStatus = await runStep("pii-provider: verify Postgres erasure request projection", () =>
    queryPostgresScalar(`
      SELECT status
      FROM pii_provider_subject_requests
      WHERE request_id = ${escapedErasureRequestId}
        AND storage_key = ${escapedStorageKey}
    `)
  )
  assertEqual(erasureRequestStatus, "COMPLETED", "pii-provider: Postgres erasure request projection mismatch")

  const requestTypes = await runStep("pii-provider: verify Postgres subject request types", () =>
    queryPostgresScalar(`
      SELECT string_agg(request_type || ':' || status, ',' ORDER BY request_type)
      FROM pii_provider_subject_requests
      WHERE storage_key = ${escapedStorageKey}
    `)
  )
  assertEqual(
    requestTypes,
    "ACCESS:COMPLETED,ERASURE:COMPLETED",
    "pii-provider: Postgres subject request type mismatch"
  )

  await verifyEventJournalCount("pii-provider", "pii_provider_event_journal", storageKey, 14)
}

const testProfileProvider = async () => {
  const profileId = makeUuid()
  const profileJson = makeProfileDocument(profileId)
  const mergedProfileJson = makeProfileDocument(profileId, "Principal Effect Engineer")
  const branchProfileJson = makeProfileDocument(profileId, "Principal Platform Engineer")
  const source = makeSource("profile")
  const extraSource = makeExtraSource("profile")
  const snapshotId = `snapshot-${profileId}`

  console.log(`profile-provider: lifecycle start (${profileId})`)

  const created = await runStep("profile-provider: CreateProfile", () =>
    profileClient.CreateProfile({
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
  )

  assertEqual(created.profileId, profileId, "profile-provider: CreateProfile returned wrong profileId")
  assertEqual(created.revision, 3, "profile-provider: CreateProfile should produce revision 3 with PII")

  const merged = await runStep("profile-provider: MergeProfileData", () =>
    profileClient.MergeProfileData({
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
  )

  assertEqual(merged.revision, 5, "profile-provider: MergeProfileData should advance to revision 5")

  const forked = await runStep("profile-provider: ForkProfileBranch", () =>
    profileClient.ForkProfileBranch({
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
  )

  assertEqual(forked.branchId, "review", "profile-provider: ForkProfileBranch returned wrong active branch")
  assertEqual(forked.revision, 7, "profile-provider: ForkProfileBranch should advance to revision 7")

  const snapshotted = await runStep("profile-provider: CreateProfileSnapshot", () =>
    profileClient.CreateProfileSnapshot({
      profileId,
      branchId: "review",
      snapshotId,
      snapshotType: "MANUAL",
      schemaVersion: "1.2.0",
      profileJson: branchProfileJson,
      metadataJson: json({ source: "smoke-test", stage: "snapshot", reviewer: "qa" }),
      piiStorageKey: `pii/profile/${profileId}/snapshot.json`,
      piiJson: json({ passport: "masked", taxId: "masked" }),
      piiJurisdiction: "DE",
      actorId: "smoke-test-agent",
      summary: "Create profile snapshot from smoke test"
    })
  )

  assertEqual(snapshotted.revision, 10, "profile-provider: CreateProfileSnapshot should advance to revision 10 with PII")

  const published = await runStep("profile-provider: PublishProfileSnapshot", () =>
    profileClient.PublishProfileSnapshot({
      profileId,
      snapshotId,
      strategyJson: json({ audience: "platform", priority: "high" }),
      metadataJson: json({ source: "smoke-test", stage: "publish" }),
      schemaVersion: "1.2.0",
      actorId: "smoke-test-agent"
    })
  )

  assertEqual(published.revision, 12, "profile-provider: PublishProfileSnapshot should advance to revision 12")

  const profile = await runStep("profile-provider: GetProfile", () =>
    profileClient.GetProfile({ profileId })
  )
  assertEqual(profile.status, "published", "profile-provider: GetProfile should return published state")
  assertEqual(profile.activeBranchId, "review", "profile-provider: GetProfile should return review as active branch")
  assertEqual(profile.revision, 12, "profile-provider: GetProfile returned wrong revision")
  assertEqual(profile.maskedProfileJson?.uuid, profileId, "profile-provider: GetProfile returned wrong profile document")
  assertEqual(
    profile.maskedProfileJson?.userData.personalInfo.relevantPosition,
    "Principal Effect Engineer",
    "profile-provider: MergeProfileData did not update the current document"
  )
  assertEqual(profile.latestPiiStorageKey, `pii/profile/${profileId}/snapshot.json`, "profile-provider: latest PII key not updated")
  assertEqual(profile.piiJurisdiction, "DE", "profile-provider: PII jurisdiction not updated")
  assertEqual(profile.sourceAssets.length, 2, "profile-provider: source assets should be deduplicated and merged")
  assertEqual(profile.sourceAssets[0]?.summary, "profile smoke-test source updated by merge", "profile-provider: duplicate source should be overwritten")

  const history = await runStep("profile-provider: GetProfileHistory", () =>
    profileClient.GetProfileHistory({ profileId })
  )
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
    history.snapshots[0]?.profileJson.userData.personalInfo.relevantPosition,
    "Principal Platform Engineer",
    "profile-provider: snapshot should preserve the snapshot document"
  )

  await verifyProfilePostgresProjection(
    profileId,
    snapshotId,
    `pii/profile/${profileId}/snapshot.json`
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

  console.log(`request-provider: lifecycle start (${requestId})`)

  const created = await runStep("request-provider: CreateRequest", () =>
    requestClient.CreateRequest({
      requestId,
      clientId: "smoke-test-client",
      schemaVersion: "1.0.0",
      requestJson,
      metadataJson: json({ source: "smoke-test", stage: "create" }),
      actorId: "smoke-test-client",
      summary: "Create request from smoke test",
      sources: [source]
    })
  )

  assertEqual(created.requestId, requestId, "request-provider: CreateRequest returned wrong requestId")
  assertEqual(created.revision, 2, "request-provider: CreateRequest should produce revision 2")

  const updated = await runStep("request-provider: UpdateRequest", () =>
    requestClient.UpdateRequest({
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
  )

  assertEqual(updated.revision, 4, "request-provider: UpdateRequest should advance to revision 4")

  const snapshotted = await runStep("request-provider: CreateRequestSnapshot manual", () =>
    requestClient.CreateRequestSnapshot({
      requestId,
      snapshotId: firstSnapshotId,
      snapshotType: "MANUAL",
      schemaVersion: "1.2.0",
      requestJson: snapshotRequestJson,
      metadataJson: json({ source: "smoke-test", stage: "manual-snapshot" }),
      actorId: "smoke-test-client",
      summary: "Create request snapshot from smoke test"
    })
  )

  assertEqual(snapshotted.revision, 6, "request-provider: CreateRequestSnapshot should advance to revision 6")
  assertEqual(snapshotted.latestSnapshotId, firstSnapshotId, "request-provider: first snapshot id not returned")

  const updatedAfterSnapshot = await runStep("request-provider: UpdateRequest after snapshot", () =>
    requestClient.UpdateRequest({
      requestId,
      schemaVersion: "1.3.0",
      requestJson: finalRequestJson,
      metadataJson: json({ source: "smoke-test", stage: "post-snapshot-update" }),
      actorId: "smoke-test-client",
      summary: "Update request after first snapshot from smoke test",
      sources: []
    })
  )

  assertEqual(updatedAfterSnapshot.revision, 8, "request-provider: second UpdateRequest should advance to revision 8")

  const secondSnapshot = await runStep("request-provider: CreateRequestSnapshot LLM", () =>
    requestClient.CreateRequestSnapshot({
      requestId,
      snapshotId: secondSnapshotId,
      snapshotType: "LLM",
      schemaVersion: "1.4.0",
      requestJson: finalRequestJson,
      metadataJson: json({ source: "smoke-test", stage: "llm-snapshot" }),
      actorId: "smoke-test-client",
      summary: "Create LLM request snapshot from smoke test"
    })
  )

  assertEqual(secondSnapshot.revision, 10, "request-provider: second snapshot should advance to revision 10")
  assertEqual(secondSnapshot.latestSnapshotId, secondSnapshotId, "request-provider: second snapshot id not returned")

  const request = await runStep("request-provider: GetRequest", () =>
    requestClient.GetRequest({ requestId })
  )
  assertEqual(request.status, "snapshotted", "request-provider: GetRequest should return snapshotted state")
  assertEqual(request.revision, 10, "request-provider: GetRequest returned wrong revision")
  assertEqual(request.currentSchemaVersion, "1.4.0", "request-provider: GetRequest returned wrong schema version")
  assertEqual(request.requestJson?.uuid, requestId, "request-provider: GetRequest returned wrong request document")
  assertEqual(
    request.requestJson?.vacancyData.relevantPosition,
    "Staff Distributed Systems Engineer",
    "request-provider: final request document not preserved"
  )
  assertEqual(request.sourceAssets.length, 2, "request-provider: source assets should be deduplicated and merged")
  assertEqual(request.sourceAssets[0]?.summary, "request smoke-test source updated by update", "request-provider: duplicate source should be overwritten")

  const history = await runStep("request-provider: GetRequestHistory", () =>
    requestClient.GetRequestHistory({ requestId })
  )
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

  await verifyRequestPostgresProjection(requestId, firstSnapshotId, secondSnapshotId)

  await assertRejectsWithTag(
    "request-provider: unknown request read",
    () => requestClient.GetRequest({ requestId: makeUuid() }),
    "RequestNotFound"
  )

  console.log(`request-provider: complex lifecycle ok (${requestId})`)
}

const testPIIProvider = async () => {
  const profileId = makeUuid()
  const recordId = makeUuid()
  const actorId = makeUuid()
  const subjectRequestId = makeUuid()
  const erasureRequestId = makeUuid()
  const record = makePIIRecord(recordId, profileId, actorId)
  const storageKey = `aggregate:${profileId}:1`

  console.log(`pii-provider: lifecycle start (${storageKey})`)

  const created = await runStep("pii-provider: CreatePIIRecord", () =>
    piiClient.CreatePIIRecord({
      record,
      actorId: actorId,
      summary: "Create PII record from smoke test"
    })
  )

  assertEqual(created.storageKey, storageKey, "pii-provider: CreatePIIRecord returned wrong storage key")
  assertEqual(created.status, "ACTIVE", "pii-provider: CreatePIIRecord returned wrong status")

  const pii = await runStep("pii-provider: GetPIIRecord", () =>
    piiClient.GetPIIRecord({
      storageKey: storageKey,
      actorId: actorId,
      actorType: "USER",
      purpose: "SMOKE_TEST_READ"
    })
  )
  assertEqual(pii.id, recordId, "pii-provider: GetPIIRecord returned wrong record id")
  assertEqual(
    pii.personalIdentity?.fullName?.firstName,
    "Ada",
    "pii-provider: GetPIIRecord did not decrypt personal identity"
  )
  assertEqual(pii.encryption.algorithm, "AES-256-GCM", "pii-provider: encryption metadata missing")
  assertEqual(pii.audit.accessCount, 1, "pii-provider: audited read should increment access count")

  const piiByEntity = await runStep("pii-provider: GetPIIRecordByEntity", () =>
    piiClient.GetPIIRecordByEntity({
      entityReference: record.entityReference,
      actorId: actorId,
      actorType: "SYSTEM",
      purpose: "SMOKE_TEST_ENTITY_READ"
    })
  )
  assertEqual(piiByEntity.id, recordId, "pii-provider: GetPIIRecordByEntity returned wrong record id")
  assertEqual(piiByEntity.audit.accessCount, 2, "pii-provider: entity read should increment access count")

  const patched = await runStep("pii-provider: PatchPIIRecord", () =>
    piiClient.PatchPIIRecord({
      storageKey: storageKey,
      personalData: {
        personalIdentity: {
          fullName: {
            firstName: "Ada",
            lastName: "Byron",
            middleName: "Augusta"
          },
          citizenship: ["RU", "GB"]
        },
        contactData: {
          phones: [{ number: "+79007654321", type: "MOBILE", verified: true }],
          emails: [{ address: "ada.byron@example.test", type: "PERSONAL", verified: true }],
          messengers: [{ platform: "TELEGRAM", identifier: "@ada_byron" }]
        },
        socialProfiles: [
          { platform: "GITHUB", profileUrl: "https://github.com/ada-byron", username: "ada-byron" }
        ]
      },
      actorId: actorId,
      reason: "Patch smoke-test personal data"
    })
  )
  assertEqual(patched.revision, 4, "pii-provider: PatchPIIRecord should advance revision after two reads")

  const patchedRead = await runStep("pii-provider: GetPIIRecord patched", () =>
    piiClient.GetPIIRecord({
      storageKey: storageKey,
      actorId: actorId,
      actorType: "USER",
      purpose: "SMOKE_TEST_PATCH_VERIFY"
    })
  )
  assertEqual(
    patchedRead.personalIdentity?.fullName?.middleName,
    "Augusta",
    "pii-provider: PatchPIIRecord did not update decrypted identity"
  )
  assertEqual(
    patchedRead.contactData?.emails?.[0]?.address,
    "ada.byron@example.test",
    "pii-provider: PatchPIIRecord did not update decrypted contact data"
  )
  assertEqual(patchedRead.audit.accessCount, 3, "pii-provider: patched read should increment access count")

  const consent = await runStep("pii-provider: UpdatePIIConsent", () =>
    piiClient.UpdatePIIConsent({
      storageKey: storageKey,
      given: true,
      purposes: ["PROFILE_MATCHING", "COMMUNICATION"],
      consentVersion: "2.0",
      actorId: actorId
    })
  )
  assertEqual(consent.revision, 6, "pii-provider: UpdatePIIConsent should advance revision after patch verification")

  const access = await runStep("pii-provider: RecordPIIAccess", () =>
    piiClient.RecordPIIAccess({
      storageKey: storageKey,
      actorId: actorId,
      actorType: "USER",
      purpose: "PROFILE_VIEW",
      success: true
    })
  )
  assertEqual(access.revision, 7, "pii-provider: RecordPIIAccess should advance revision after consent update")

  const withdrawn = await runStep("pii-provider: WithdrawPIIConsent", () =>
    piiClient.WithdrawPIIConsent({
      storageKey: storageKey,
      reason: "smoke-test consent withdrawal",
      retainForLegal: true,
      actorId: actorId
    })
  )
  assertEqual(withdrawn.revision, 8, "pii-provider: WithdrawPIIConsent should advance revision after manual access")

  const subjectCreated = await runStep("pii-provider: CreateSubjectRequest", () =>
    piiClient.CreateSubjectRequest({
      storageKey: storageKey,
      requestId: subjectRequestId,
      requestType: "ACCESS",
      notes: "smoke-test access export",
      actorId: actorId
    })
  )
  assertEqual(subjectCreated.revision, 9, "pii-provider: CreateSubjectRequest should advance revision after withdrawal")

  const subjectCompleted = await runStep("pii-provider: CompleteSubjectRequest", () =>
    piiClient.CompleteSubjectRequest({
      storageKey: storageKey,
      requestId: subjectRequestId,
      status: "COMPLETED",
      notes: "smoke-test access export completed",
      actorId: actorId
    })
  )
  assertEqual(subjectCompleted.revision, 10, "pii-provider: CompleteSubjectRequest should advance revision")

  const rotated = await runStep("pii-provider: RotatePIIKey", () =>
    piiClient.RotatePIIKey({
      storageKey: storageKey,
      reason: "smoke-test key rotation",
      actorId: actorId
    })
  )
  assertEqual(rotated.revision, 11, "pii-provider: RotatePIIKey should advance revision")

  const failedAccess = await runStep("pii-provider: RecordPIIAccess failed", () =>
    piiClient.RecordPIIAccess({
      storageKey: storageKey,
      actorId: actorId,
      actorType: "SYSTEM",
      purpose: "SMOKE_TEST_FAILED_READ",
      success: false,
      failureReason: "simulated authorization denial"
    })
  )
  assertEqual(failedAccess.revision, 12, "pii-provider: failed RecordPIIAccess should be audited")

  const audit = await runStep("pii-provider: GetPIIAuditLog", () =>
    piiClient.GetPIIAuditLog({ storageKey: storageKey })
  )
  assertEqual(audit.total, 12, "pii-provider: audit log should include all pre-erasure operations")
  assertArrayEqual(
    audit.auditEntries.map((entry) => entry.action),
    [
      "CREATE",
      "READ",
      "READ",
      "UPDATE",
      "READ",
      "UPDATE",
      "READ",
      "UPDATE",
      "UPDATE",
      "UPDATE",
      "UPDATE",
      "READ"
    ],
    "pii-provider: audit log returned wrong action sequence"
  )
  assertEqual(
    audit.auditEntries.at(-1)?.failureReason,
    "simulated authorization denial",
    "pii-provider: failed access audit should preserve failure reason"
  )

  const history = await runStep("pii-provider: GetPIIHistory", () =>
    piiClient.GetPIIHistory({ storageKey: storageKey })
  )
  assertEqual(history.currentRevision, 12, "pii-provider: history should reflect pre-erasure revision")
  assertEqual(history.requests.length, 1, "pii-provider: history should include completed access request")
  assertEqual(history.requests[0]?.status, "COMPLETED", "pii-provider: history should mark access request completed")
  assertArrayEqual(
    history.revisions.map((entry) => entry.eventType),
    [
      "PIIRecordCreated",
      "PIIRecordAccessed",
      "PIIRecordAccessed",
      "PIIRecordUpdated",
      "PIIRecordAccessed",
      "PIIConsentUpdated",
      "PIIRecordAccessed",
      "PIIConsentWithdrawn",
      "PIISubjectRequestCreated",
      "PIISubjectRequestCompleted",
      "PIIKeyRotated",
      "PIIRecordAccessed"
    ],
    "pii-provider: history returned wrong pre-erasure event sequence"
  )

  const erasure = await runStep("pii-provider: RequestPIIErasure", () =>
    piiClient.RequestPIIErasure({
      storageKey: storageKey,
      requestId: erasureRequestId,
      reason: "smoke-test erasure",
      immediate: true,
      actorId: actorId
    })
  )
  assertEqual(erasure.status, "PENDING_DELETION", "pii-provider: erasure request should mark pending deletion")
  assertEqual(erasure.revision, 13, "pii-provider: erasure request should advance revision after pre-erasure lifecycle")

  const deleted = await runStep("pii-provider: CompletePIIErasure", () =>
    piiClient.CompletePIIErasure({
      storageKey: storageKey,
      requestId: erasureRequestId,
      actorId: actorId
    })
  )
  assertEqual(deleted.status, "DELETED", "pii-provider: erasure completion should delete the record")
  assertEqual(deleted.revision, 14, "pii-provider: erasure completion should advance revision after request")

  await assertRejectsWithTag(
    "pii-provider: deleted record read",
    () => piiClient.GetPIIRecord({
      storageKey: storageKey,
      actorId: actorId,
      actorType: "USER",
      purpose: "SMOKE_TEST_READ_DELETED"
    }),
    "PIINotFound"
  )
  await verifyPIIPostgresProjection(storageKey, subjectRequestId, erasureRequestId)

  console.log(`pii-provider: encrypted storage lifecycle ok (${storageKey})`)
}

console.log("services: smoke tests starting")
console.log(`profile-provider: ${profileBaseUrl}`)
console.log(`request-provider: ${requestBaseUrl}`)
console.log(`pii-provider: ${piiBaseUrl}`)

await waitForHealth("profile-provider", profileBaseUrl)
await waitForHealth("request-provider", requestBaseUrl)
await waitForHealth("pii-provider", piiBaseUrl)
await testProfileProvider()
await testRequestProvider()
await testPIIProvider()

console.log("services: smoke tests passed")
