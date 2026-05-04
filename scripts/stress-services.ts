import * as DateTime from "effect/DateTime"
import { makeFetchClient } from "@semyenov/n2/helpers"
import { PIIProviderRpcs } from "../services/pii-provider/src/contracts/commands.js"
import { ProfileProviderRpcs } from "../services/profile-provider/src/contracts/commands.js"
import { RequestProviderRpcs } from "../services/request-provider/src/contracts/commands.js"

const profileBaseUrl = process.env.PROFILE_PROVIDER_BASE_URL ?? "http://127.0.0.1:4100"
const requestBaseUrl = process.env.REQUEST_PROVIDER_BASE_URL ?? "http://127.0.0.1:4110"
const piiBaseUrl = process.env.PII_PROVIDER_BASE_URL ?? "http://127.0.0.1:4120"

const profileClient = makeFetchClient(ProfileProviderRpcs, `${profileBaseUrl}/rpc/profile-provider`)
const requestClient = makeFetchClient(RequestProviderRpcs, `${requestBaseUrl}/rpc/request-provider`)
const piiClient = makeFetchClient(PIIProviderRpcs, `${piiBaseUrl}/rpc/pii-provider`)

const allServices = ["profile-provider", "request-provider", "pii-provider"] as const
type ServiceName = typeof allServices[number]

interface WorkUnit {
  readonly service: ServiceName
  readonly index: number
}

interface Measurement {
  readonly operation: string
  readonly durationMs: number
  readonly ok: boolean
}

interface Failure {
  readonly service: ServiceName
  readonly index: number
  readonly error: string
}

const serviceAliases: Record<string, ServiceName> = {
  profile: "profile-provider",
  "profile-provider": "profile-provider",
  request: "request-provider",
  "request-provider": "request-provider",
  pii: "pii-provider",
  "pii-provider": "pii-provider"
}

const parsePositiveInt = (name: string, defaultValue: number) => {
  const raw = process.env[name]
  if (raw === undefined || raw.trim() === "") return defaultValue
  const value = Number(raw)
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer, got ${raw}`)
  }
  return value
}

const parseServices = (): ReadonlyArray<ServiceName> => {
  const raw = process.env.STRESS_SERVICES ?? "all"
  if (raw.trim() === "" || raw.trim() === "all") return allServices

  const services = raw
    .split(",")
    .map((item) => item.trim())
    .filter((item) => item.length > 0)
    .map((item) => {
      const service = serviceAliases[item]
      if (service === undefined) {
        throw new Error(`STRESS_SERVICES contains unknown service "${item}"`)
      }
      return service
    })

  return [...new Set(services)]
}

const iterations = parsePositiveInt("STRESS_ITERATIONS", 25)
const concurrency = parsePositiveInt("STRESS_CONCURRENCY", 5)
const healthTimeoutMs = parsePositiveInt("STRESS_HEALTH_TIMEOUT_MS", 30_000)
const services = parseServices()
const measurements: Array<Measurement> = []
const failures: Array<Failure> = []

const json = (value: unknown) => JSON.stringify(value)

const makeUuid = () => crypto.randomUUID()

const formatMs = (value: number) => `${value.toFixed(1)}ms`

const formatRate = (value: number) => `${value.toFixed(1)}/s`

const errorMessage = (error: unknown) =>
  error instanceof Error ? error.stack ?? error.message : String(error)

const percentile = (values: ReadonlyArray<number>, fraction: number) => {
  if (values.length === 0) return 0
  const index = Math.min(values.length - 1, Math.ceil(values.length * fraction) - 1)
  return values[index] ?? 0
}

const record = async <A>(operation: string, run: () => Promise<A>) => {
  const start = performance.now()
  try {
    const result = await run()
    measurements.push({ operation, durationMs: performance.now() - start, ok: true })
    return result
  } catch (error) {
    measurements.push({ operation, durationMs: performance.now() - start, ok: false })
    throw error
  }
}

const waitForHealth = async (name: ServiceName, baseUrl: string) => {
  const deadline = Date.now() + healthTimeoutMs
  let lastError = ""

  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${baseUrl}/health`)
      if (response.ok) return
      lastError = `HTTP ${response.status}`
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error)
    }

    await Bun.sleep(1_000)
  }

  throw new Error(`${name}: health check failed: ${lastError}`)
}

const makeSource = (kind: "profile" | "request", index: number) => ({
  sourceId: `${kind}-stress-source-${index}`,
  kind: "manual" as const,
  uri: `manual://${kind}/stress/${index}`,
  mediaType: "application/json",
  storageKey: `${kind}/stress/${index}.json`,
  summary: `${kind} stress source ${index}`
})

const makeProfileDocument = (profileId: string, index: number, position: string) => ({
  uuid: profileId,
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-02T00:00:00.000Z",
  userData: {
    personalInfo: {
      firstName: "Load",
      lastName: "Tester",
      relevantPosition: position,
      citizenship: "GB",
      residence: "Berlin",
      relocation: true
    },
    salaryExpectations: {
      currency: "USD",
      amountFrom: 1000 + index,
      amountTo: 1600 + index
    },
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
    education: [
      {
        degree: "Bachelor",
        fieldOfStudy: "Computer Science",
        institution: "Stress Test University",
        startYear: 2018,
        endYear: 2022
      }
    ]
  },
  userMatchingData: {
    applicationHistory: [{ status: "generated", source: "stress-test" }]
  },
  userMetaData: {
    version: 1,
    sourcePlatform: "stress-test",
    tags: ["stress", "profile-provider"]
  }
})

const makeRequestDocument = (requestId: string, index: number, position: string) => ({
  id: index + 1,
  uuid: requestId,
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-02T00:00:00.000Z",
  vacancyData: {
    relevantPosition: position,
    company: "Stress Test Co",
    country: "Germany",
    location: "Berlin",
    jobFormat: "remote",
    industry: "SaaS",
    description: "Exercise service lifecycle load.",
    technologyStack: ["TypeScript", "Effect"],
    requirements: {
      experienceYears: 5,
      hardSkills: [
        { name: "TypeScript", level: "advanced" },
        { name: "Effect", level: "advanced" }
      ],
      softSkills: [
        { name: "communication", required: true }
      ],
      languages: [
        { name: "English", level: "C1", required: true }
      ]
    },
    salaryOffer: {
      currency: "EUR",
      amountFrom: 7000 + index,
      amountTo: 9000 + index
    },
    responsibilities: ["Exercise service APIs"],
    employmentTypes: ["full_time"],
    workSchedule: ["remote"]
  },
  vacancyMetaData: {
    sourcePlatform: "stress-test",
    version: 1,
    tags: ["stress", "request-provider"]
  }
})

const makePIIRecord = (recordId: string, profileId: string, actorId: string, index: number) => ({
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
      firstName: "Stress",
      lastName: `Subject ${index}`
    },
    citizenship: ["RU"]
  },
  contactData: {
    phones: [{ number: `+7900${String(index).padStart(7, "0")}`, type: "MOBILE" as const, verified: true }],
    emails: [{ address: `stress-pii-${index}@example.test`, type: "WORK" as const, verified: true }]
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
    policyId: "stress-retention-ru",
    retentionPeriodDays: 1095,
    legalHold: false
  },
  audit: {
    createdAt: DateTime.unsafeMake("2026-01-01T00:00:00.000Z"),
    createdBy: actorId,
    accessCount: 0
  },
  extractionInfo: {
    extractedFields: ["personalIdentity.fullName", "contactData.emails"],
    extractionMethod: "MANUAL" as const,
    confidenceScores: { fullName: 1, emails: 1 }
  },
  status: "ACTIVE" as const
})

const stressProfileProvider = async (index: number) => {
  const profileId = makeUuid()
  const source = makeSource("profile", index)
  const createdDocument = makeProfileDocument(profileId, index, "Senior TypeScript Engineer")
  const mergedDocument = makeProfileDocument(profileId, index, "Principal Effect Engineer")
  const snapshotDocument = makeProfileDocument(profileId, index, "Staff Platform Engineer")
  const snapshotId = `stress-profile-snapshot-${profileId}`
  const createPiiStorageKey = `pii/profile/${profileId}/stress-create.json`
  const snapshotPiiStorageKey = `pii/profile/${profileId}/stress-snapshot.json`

  await record("profile-provider CreateProfile", () =>
    profileClient.CreateProfile({
      profileId,
      ownerAgentId: "stress-agent",
      branchId: "main",
      schemaVersion: "1.0.0",
      maskedProfileJson: createdDocument,
      metadataJson: json({ source: "stress-test", iteration: index, stage: "create" }),
      piiStorageKey: createPiiStorageKey,
      piiJson: json({ email: `stress-profile-${index}@example.test`, phone: `+4915100${String(index).padStart(4, "0")}` }),
      piiJurisdiction: "DE",
      actorId: "stress-agent",
      summary: "Create profile from stress test",
      sources: [source]
    })
  )

  await record("profile-provider MergeProfileData", () =>
    profileClient.MergeProfileData({
      profileId,
      branchId: "main",
      schemaVersion: "1.1.0",
      maskedProfileJson: mergedDocument,
      metadataJson: json({ source: "stress-test", iteration: index, stage: "merge" }),
      piiStorageKey: "",
      piiJson: "",
      piiJurisdiction: "",
      actorId: "stress-agent",
      summary: "Merge profile from stress test",
      sources: [{ ...source, summary: "profile stress source updated" }]
    })
  )

  await record("profile-provider CreateProfileSnapshot", () =>
    profileClient.CreateProfileSnapshot({
      profileId,
      branchId: "main",
      snapshotId,
      snapshotType: "MANUAL",
      schemaVersion: "1.2.0",
      profileJson: snapshotDocument,
      metadataJson: json({ source: "stress-test", iteration: index, stage: "snapshot" }),
      piiStorageKey: snapshotPiiStorageKey,
      piiJson: json({ passport: `stress-passport-${index}`, taxId: `stress-tax-${index}` }),
      piiJurisdiction: "DE",
      actorId: "stress-agent",
      summary: "Create profile snapshot from stress test"
    })
  )

  await record("profile-provider PublishProfileSnapshot", () =>
    profileClient.PublishProfileSnapshot({
      profileId,
      snapshotId,
      strategyJson: json({ source: "stress-test", iteration: index, visibility: "internal" }),
      metadataJson: json({ source: "stress-test", iteration: index, stage: "publish" }),
      schemaVersion: "1.2.0",
      actorId: "stress-agent"
    })
  )

  const profile = await record("profile-provider GetProfile", () =>
    profileClient.GetProfile({ profileId })
  )

  if (profile.profileId !== profileId || profile.status === "empty") {
    throw new Error(`profile-provider returned invalid profile state for ${profileId}`)
  }
  if (
    profile.status !== "published" ||
    profile.latestPiiStorageKey !== snapshotPiiStorageKey ||
    profile.piiJurisdiction !== "DE" ||
    profile.publishedSnapshotId !== snapshotId
  ) {
    throw new Error(`profile-provider returned incomplete published profile state for ${profileId}`)
  }

  await record("profile-provider GetProfileHistory", () =>
    profileClient.GetProfileHistory({ profileId })
  )
}

const stressRequestProvider = async (index: number) => {
  const requestId = makeUuid()
  const source = makeSource("request", index)
  const createdDocument = makeRequestDocument(requestId, index, "Senior TypeScript Engineer")
  const updatedDocument = makeRequestDocument(requestId, index, "Principal Effect Engineer")
  const snapshotDocument = makeRequestDocument(requestId, index, "Staff Platform Engineer")
  const snapshotId = `stress-request-snapshot-${requestId}`

  await record("request-provider CreateRequest", () =>
    requestClient.CreateRequest({
      requestId,
      clientId: "stress-client",
      schemaVersion: "1.0.0",
      requestJson: createdDocument,
      metadataJson: json({ source: "stress-test", iteration: index, stage: "create" }),
      actorId: "stress-client",
      summary: "Create request from stress test",
      sources: [source]
    })
  )

  await record("request-provider UpdateRequest", () =>
    requestClient.UpdateRequest({
      requestId,
      schemaVersion: "1.1.0",
      requestJson: updatedDocument,
      metadataJson: json({ source: "stress-test", iteration: index, stage: "update" }),
      actorId: "stress-client",
      summary: "Update request from stress test",
      sources: [{ ...source, summary: "request stress source updated" }]
    })
  )

  await record("request-provider CreateRequestSnapshot", () =>
    requestClient.CreateRequestSnapshot({
      requestId,
      snapshotId,
      snapshotType: "MANUAL",
      schemaVersion: "1.2.0",
      requestJson: snapshotDocument,
      metadataJson: json({ source: "stress-test", iteration: index, stage: "snapshot" }),
      actorId: "stress-client",
      summary: "Create request snapshot from stress test"
    })
  )

  const request = await record("request-provider GetRequest", () =>
    requestClient.GetRequest({ requestId })
  )

  if (request.requestId !== requestId || request.status === "empty") {
    throw new Error(`request-provider returned invalid request state for ${requestId}`)
  }

  await record("request-provider GetRequestHistory", () =>
    requestClient.GetRequestHistory({ requestId })
  )
}

const stressPIIProvider = async (index: number) => {
  const profileId = makeUuid()
  const recordId = makeUuid()
  const actorId = makeUuid()
  const piiRecord = makePIIRecord(recordId, profileId, actorId, index)
  const storageKey = `aggregate:${profileId}:1`

  const created = await record("pii-provider CreatePIIRecord", () =>
    piiClient.CreatePIIRecord({
      record: piiRecord,
      actorId,
      summary: "Create PII record from stress test"
    })
  )

  if (created.storageKey !== storageKey || created.status !== "ACTIVE") {
    throw new Error(`pii-provider returned invalid created state for ${storageKey}`)
  }

  const pii = await record("pii-provider GetPIIRecord", () =>
    piiClient.GetPIIRecord({
      storageKey,
      actorId,
      actorType: "USER",
      purpose: "STRESS_TEST_READ"
    })
  )

  if (pii.id !== recordId || pii.personalIdentity?.fullName?.firstName !== "Stress") {
    throw new Error(`pii-provider returned invalid decrypted record for ${storageKey}`)
  }

  const consent = await record("pii-provider UpdatePIIConsent", () =>
    piiClient.UpdatePIIConsent({
      storageKey,
      given: true,
      purposes: ["PROFILE_MATCHING", "COMMUNICATION"],
      consentVersion: "2.0",
      actorId
    })
  )

  if (consent.status !== "ACTIVE") {
    throw new Error(`pii-provider returned invalid consent state for ${storageKey}`)
  }

  await record("pii-provider RecordPIIAccess", () =>
    piiClient.RecordPIIAccess({
      storageKey,
      actorId,
      actorType: "USER",
      purpose: "STRESS_TEST_PROFILE_VIEW",
      success: true
    })
  )

  const audit = await record("pii-provider GetPIIAuditLog", () =>
    piiClient.GetPIIAuditLog({ storageKey })
  )

  if (audit.total < 4) {
    throw new Error(`pii-provider returned incomplete audit log for ${storageKey}`)
  }

  const history = await record("pii-provider GetPIIHistory", () =>
    piiClient.GetPIIHistory({ storageKey })
  )

  if (history.currentRevision < 4) {
    throw new Error(`pii-provider returned incomplete history for ${storageKey}`)
  }
}

const runUnit = (unit: WorkUnit) => {
  switch (unit.service) {
    case "profile-provider":
      return stressProfileProvider(unit.index)
    case "request-provider":
      return stressRequestProvider(unit.index)
    case "pii-provider":
      return stressPIIProvider(unit.index)
  }
}

const summarize = (startedAt: number) => {
  const elapsedSeconds = (performance.now() - startedAt) / 1000
  const successful = measurements.filter((item) => item.ok)
  const failed = measurements.filter((item) => !item.ok)
  const byOperation = new Map<string, Array<number>>()

  for (const measurement of measurements) {
    const values = byOperation.get(measurement.operation) ?? []
    values.push(measurement.durationMs)
    byOperation.set(measurement.operation, values)
  }

  console.log("")
  console.log(`stress: services=${services.join(",")} iterations=${iterations} concurrency=${concurrency}`)
  console.log(`stress: elapsed=${elapsedSeconds.toFixed(2)}s operations=${measurements.length} ok=${successful.length} failed=${failed.length} rate=${formatRate(successful.length / elapsedSeconds)}`)

  for (const [operation, values] of [...byOperation.entries()].sort(([a], [b]) => a.localeCompare(b))) {
    const sorted = [...values].sort((a, b) => a - b)
    const total = sorted.reduce((sum, value) => sum + value, 0)
    console.log([
      operation.padEnd(45),
      `count=${String(sorted.length).padStart(4)}`,
      `avg=${formatMs(total / sorted.length).padStart(8)}`,
      `p50=${formatMs(percentile(sorted, 0.50)).padStart(8)}`,
      `p95=${formatMs(percentile(sorted, 0.95)).padStart(8)}`,
      `max=${formatMs(sorted[sorted.length - 1] ?? 0).padStart(8)}`
    ].join(" "))
  }

  if (failures.length > 0) {
    console.log("")
    console.log("stress: failures")
    for (const failure of failures.slice(0, 10)) {
      console.log(`${failure.service}#${failure.index}: ${failure.error}`)
    }
    if (failures.length > 10) {
      console.log(`... ${failures.length - 10} more failures`)
    }
  }
}

console.log(`stress: waiting for ${services.join(", ")} health`)
await Promise.all(
  services.map((service) =>
    waitForHealth(
      service,
      service === "profile-provider"
        ? profileBaseUrl
        : service === "request-provider"
          ? requestBaseUrl
          : piiBaseUrl
    )
  )
)

const units = services.flatMap((service) =>
  Array.from({ length: iterations }, (_, index) => ({ service, index }))
)
let nextUnit = 0
const startedAt = performance.now()

console.log(`stress: running ${units.length} lifecycles with concurrency ${concurrency}`)

await Promise.all(
  Array.from({ length: Math.min(concurrency, units.length) }, async () => {
    while (nextUnit < units.length) {
      const unit = units[nextUnit]
      nextUnit += 1
      if (unit === undefined) return

      try {
        await runUnit(unit)
      } catch (error) {
        failures.push({
          service: unit.service,
          index: unit.index,
          error: errorMessage(error)
        })
      }
    }
  })
)

summarize(startedAt)

if (failures.length > 0) {
  process.exitCode = 1
}
