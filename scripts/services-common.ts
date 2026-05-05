import * as Minio from "minio"
import { makeFetchClient } from "@semyenov/n2/helpers"
import { PIIProviderRpcs } from "qb.service.pii/contracts"
import { ProfileProviderRpcs } from "qb.service.profiler/contracts"
import { RequestProviderRpcs } from "qb.service.request/contracts"

export const allServices = ["profile-provider", "request-provider", "pii-provider"] as const
export type ServiceName = typeof allServices[number]

const envString = (name: string, defaultValue: string) => {
  const raw = process.env[name]?.trim()
  return raw === undefined || raw.length === 0 ? defaultValue : raw
}

export const parsePositiveInt = (name: string, defaultValue: number) => {
  const raw = process.env[name]
  if (raw === undefined || raw.trim() === "") return defaultValue
  const value = Number(raw)
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer, got ${raw}`)
  }
  return value
}

const parseBoolean = (name: string, defaultValue: boolean) => {
  const raw = process.env[name]?.trim()
  if (raw === undefined || raw.length === 0) return defaultValue
  const value = raw.toLowerCase()
  if (value === "1" || value === "true" || value === "yes") return true
  if (value === "0" || value === "false" || value === "no") return false
  throw new Error(`${name} must be a boolean value, got ${raw}`)
}

export const profileBaseUrl = envString("PROFILE_PROVIDER_BASE_URL", "http://127.0.0.1:4100")
export const requestBaseUrl = envString("REQUEST_PROVIDER_BASE_URL", "http://127.0.0.1:4110")
export const piiBaseUrl = envString("PII_PROVIDER_BASE_URL", "http://127.0.0.1:4120")
export const sourceAssetsBucket = envString("SOURCE_ASSETS_BUCKET", "n2-source-assets")
export const piiPayloadsBucket = envString("PII_PAYLOADS_BUCKET", "n2-pii-payloads")

export const profileClient = makeFetchClient(ProfileProviderRpcs, `${profileBaseUrl}/rpc/profile-provider`)
export const requestClient = makeFetchClient(RequestProviderRpcs, `${requestBaseUrl}/rpc/request-provider`)
export const piiClient = makeFetchClient(PIIProviderRpcs, `${piiBaseUrl}/rpc/pii-provider`)

const objectStorageVerifyEndpoint = envString("OBJECT_STORAGE_VERIFY_ENDPOINT", "127.0.0.1")
const objectStorageVerifyPort = parsePositiveInt(
  "OBJECT_STORAGE_VERIFY_PORT",
  parsePositiveInt("MINIO_API_PORT", 9002)
)
const objectStorageVerifyUseSSL = parseBoolean("OBJECT_STORAGE_VERIFY_USE_SSL", false)
const objectStorageAccessKey = envString(
  "OBJECT_STORAGE_ACCESS_KEY",
  envString("MINIO_ROOT_USER", "minioadmin")
)
const objectStorageSecretKey = envString(
  "OBJECT_STORAGE_SECRET_KEY",
  envString("MINIO_ROOT_PASSWORD", "minioadmin")
)

const objectStorageClient = new Minio.Client({
  endPoint: objectStorageVerifyEndpoint,
  port: objectStorageVerifyPort,
  useSSL: objectStorageVerifyUseSSL,
  accessKey: objectStorageAccessKey,
  secretKey: objectStorageSecretKey
})

export const makeUuid = () => crypto.randomUUID()
export const json = (value: unknown) => JSON.stringify(value)
export const sqlString = (value: string) => `'${value.replaceAll("'", "''")}'`
export const errorMessage = (error: unknown) =>
  error instanceof Error ? error.stack ?? error.message : String(error)

export const waitForHealth = async (
  name: string,
  url: string,
  timeoutMs = 30_000,
  options: { readonly log?: boolean } = {}
) => {
  const log = options.log ?? true
  const deadline = Date.now() + timeoutMs
  let lastError = ""

  if (log) console.log(`${name}: waiting for health at ${url}/health`)

  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${url}/health`)
      if (response.ok) {
        if (log) console.log(`${name}: health ok`)
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

export const waitForCondition = async (
  name: string,
  action: () => Promise<boolean>,
  timeoutMs = 30_000
) => {
  const deadline = Date.now() + timeoutMs

  while (Date.now() < deadline) {
    if (await action()) return
    await Bun.sleep(1_000)
  }

  throw new Error(`${name}: condition was not met within ${timeoutMs}ms`)
}

export const queryPostgresScalar = async (query: string) => {
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

export const objectExists = async (bucket: string, key: string) => {
  try {
    await objectStorageClient.statObject(bucket, key)
    return true
  } catch (error) {
    const code = typeof error === "object" && error !== null && "code" in error
      ? String((error as { readonly code: unknown }).code)
      : ""
    if (code === "NotFound" || code === "NoSuchKey" || code === "NoSuchBucket") return false
    throw error
  }
}
