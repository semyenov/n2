import { makeFetchClient } from "@semyenov/n2/helpers"
import { ProfileProviderRpcs } from "../services/profile-provider/src/contracts.js"
import { RequestProviderRpcs } from "../services/request-provider/src/contracts.js"

const profileBaseUrl = process.env.PROFILE_PROVIDER_BASE_URL ?? "http://127.0.0.1:4100"
const requestBaseUrl = process.env.REQUEST_PROVIDER_BASE_URL ?? "http://127.0.0.1:4110"

const profileClient = makeFetchClient(ProfileProviderRpcs, `${profileBaseUrl}/rpc/profile-provider`)
const requestClient = makeFetchClient(RequestProviderRpcs, `${requestBaseUrl}/rpc/request-provider`)

const allServices = ["profile-provider", "request-provider"] as const
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
  "request-provider": "request-provider"
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
  created_at: "2026-01-01T00:00:00.000Z",
  updated_at: "2026-01-02T00:00:00.000Z",
  user_data: {
    personal_info: {
      first_name: "Load",
      last_name: "Tester",
      relevant_position: position,
      citizenship: "GB",
      residence: "Berlin",
      relocation: true
    },
    salary_expectations: {
      currency: "USD",
      amount_from: 1000 + index,
      amount_to: 1600 + index
    },
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
    education: [
      {
        degree: "Bachelor",
        field_of_study: "Computer Science",
        institution: "Stress Test University",
        start_year: 2018,
        end_year: 2022
      }
    ]
  },
  user_matching_data: {
    application_history: [{ status: "generated", source: "stress-test" }]
  },
  user_meta_data: {
    version: 1,
    source_platform: "stress-test",
    tags: ["stress", "profile-provider"]
  }
})

const makeRequestDocument = (requestId: string, index: number, position: string) => ({
  id: index + 1,
  uuid: requestId,
  created_at: "2026-01-01T00:00:00.000Z",
  updated_at: "2026-01-02T00:00:00.000Z",
  vacancy_data: {
    relevant_position: position,
    company: "Stress Test Co",
    country: "Germany",
    location: "Berlin",
    job_format: "remote",
    industry: "SaaS",
    description: "Exercise service lifecycle load.",
    technology_stack: ["TypeScript", "Effect"],
    requirements: {
      experience_years: 5,
      hard_skills: [
        { name: "TypeScript", level: "advanced" },
        { name: "Effect", level: "advanced" }
      ],
      soft_skills: [
        { name: "communication", required: true }
      ],
      languages: [
        { name: "English", level: "C1", required: true }
      ]
    },
    salary_offer: {
      currency: "EUR",
      amount_from: 7000 + index,
      amount_to: 9000 + index
    },
    responsibilities: ["Exercise service APIs"],
    employment_types: ["full_time"],
    work_schedule: ["remote"]
  },
  vacancy_meta_data: {
    source_platform: "stress-test",
    version: 1,
    tags: ["stress", "request-provider"]
  }
})

const stressProfileProvider = async (index: number) => {
  const profileId = makeUuid()
  const source = makeSource("profile", index)
  const createdDocument = makeProfileDocument(profileId, index, "Senior TypeScript Engineer")
  const mergedDocument = makeProfileDocument(profileId, index, "Principal Effect Engineer")
  const snapshotDocument = makeProfileDocument(profileId, index, "Staff Platform Engineer")
  const snapshotId = `stress-profile-snapshot-${profileId}`

  await record("profile-provider CreateProfile", () =>
    profileClient.CreateProfile({
      profileId,
      ownerAgentId: "stress-agent",
      branchId: "main",
      schemaVersion: "1.0.0",
      maskedProfileJson: createdDocument,
      metadataJson: json({ source: "stress-test", iteration: index, stage: "create" }),
      piiStorageKey: "",
      piiJson: "",
      piiJurisdiction: "",
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
      piiStorageKey: "",
      piiJson: "",
      piiJurisdiction: "",
      actorId: "stress-agent",
      summary: "Create profile snapshot from stress test"
    })
  )

  const profile = await record("profile-provider GetProfile", () =>
    profileClient.GetProfile({ profileId })
  )

  if (profile.profileId !== profileId || profile.status === "empty") {
    throw new Error(`profile-provider returned invalid profile state for ${profileId}`)
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

const runUnit = (unit: WorkUnit) => {
  switch (unit.service) {
    case "profile-provider":
      return stressProfileProvider(unit.index)
    case "request-provider":
      return stressRequestProvider(unit.index)
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
      service === "profile-provider" ? profileBaseUrl : requestBaseUrl
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
