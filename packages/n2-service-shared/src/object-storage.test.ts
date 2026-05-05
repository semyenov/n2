import { it, expect } from "@effect/vitest"
import * as Either from "effect/Either"
import * as Effect from "effect/Effect"
import {
  ObjectStorage,
  ObjectStorageLive,
  ObjectStorageMemory,
  objectUri,
  parseObjectUri,
  piiPayloadsBucket,
  sourceAssetsBucket,
  sourceObjectKey,
  uploadSourceAssetPayloads,
} from "./object-storage.js"

const withEnv = async (
  env: Record<string, string | undefined>,
  run: () => Promise<void>,
) => {
  const previous = new Map<string, string | undefined>()
  for (const key of Object.keys(env)) {
    previous.set(key, process.env[key])
    const value = env[key]
    if (value === undefined) {
      delete process.env[key]
    } else {
      process.env[key] = value
    }
  }

  try {
    await run()
  } finally {
    for (const [key, value] of previous) {
      if (value === undefined) {
        delete process.env[key]
      } else {
        process.env[key] = value
      }
    }
  }
}

it("rejects invalid object storage ports before creating the live service", async () => {
  await withEnv({ OBJECT_STORAGE_PORT: "not-a-port" }, async () => {
    await expect(
      Effect.runPromise(ObjectStorage.pipe(Effect.provide(ObjectStorageLive)))
    ).rejects.toThrow("OBJECT_STORAGE_PORT must be an integer port between 1 and 65535")
  })
})

it("requires explicit object storage config outside local and test deployments", async () => {
  await withEnv({
    DEPLOYMENT_ENVIRONMENT: "production",
    OBJECT_STORAGE_ENDPOINT: undefined,
    OBJECT_STORAGE_ACCESS_KEY: undefined,
    OBJECT_STORAGE_SECRET_KEY: undefined,
  }, async () => {
    await expect(
      Effect.runPromise(ObjectStorage.pipe(Effect.provide(ObjectStorageLive))),
    ).rejects.toThrow(
      "OBJECT_STORAGE_ENDPOINT is required outside local/test environments",
    )
  })
})

it("builds and parses stable object URIs", () => {
  expect(objectUri("bucket", "path/to/file.txt")).toBe(
    "s3://bucket/path/to/file.txt",
  )
  expect(parseObjectUri("s3://bucket/path/to/file.txt")).toEqual({
    bucket: "bucket",
    key: "path/to/file.txt",
    uri: "s3://bucket/path/to/file.txt",
  })
  expect(parseObjectUri("https://bucket/path")).toBeUndefined()
  expect(parseObjectUri("s3://bucket")).toBeUndefined()
  expect(parseObjectUri("s3:///path")).toBeUndefined()
})

it("uses default buckets and trimmed environment overrides", async () => {
  await withEnv({
    SOURCE_ASSETS_BUCKET: undefined,
    PII_PAYLOADS_BUCKET: " pii-payloads ",
  }, async () => {
    expect(sourceAssetsBucket()).toBe("n2-source-assets")
    expect(piiPayloadsBucket()).toBe("pii-payloads")
  })
})

it("sanitizes source object keys without losing hierarchy", () => {
  expect(
    sourceObjectKey("profile provider", "applicant/42", {
      sourceId: "resume #1",
      kind: "file",
      uri: "file://resume.pdf",
      mediaType: "application/pdf",
      storageKey: "raw file.pdf",
      summary: "Resume",
    }),
  ).toMatch(
    /^profile_provider\/applicant\/42\/resume_1\/[a-f0-9]{16}-raw_file\.pdf$/,
  )
})

it("stores, reads, and deletes objects with the memory storage layer", async () => {
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const storage = yield* ObjectStorage
      const reference = yield* storage.putObject({
        bucket: "test-bucket",
        key: "documents/file.txt",
        body: "hello",
      })
      const body = yield* storage.getObject({
        bucket: "test-bucket",
        key: "documents/file.txt",
      })
      yield* storage.deleteObject({
        bucket: "test-bucket",
        key: "documents/file.txt",
      })
      const missing = yield* Effect.either(
        storage.getObject({
          bucket: "test-bucket",
          key: "documents/file.txt",
        }),
      )
      return {
        body: Buffer.from(body).toString("utf8"),
        missing,
        reference,
      }
    }).pipe(Effect.provide(ObjectStorageMemory))
  )

  expect(result.reference).toEqual({
    bucket: "test-bucket",
    key: "documents/file.txt",
    uri: "s3://test-bucket/documents/file.txt",
  })
  expect(result.body).toBe("hello")
  expect(Either.isLeft(result.missing)).toBe(true)
})

it("uploads source payload content and leaves metadata-only sources inline", async () => {
  await withEnv({ SOURCE_ASSETS_BUCKET: "source-assets" }, async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const uploaded = yield* uploadSourceAssetPayloads(
          [
            {
              sourceId: "resume-1",
              kind: "file" as const,
              uri: "file://resume.txt",
              mediaType: "text/plain",
              storageKey: "resume.txt",
              summary: "Resume",
              contentBase64: Buffer.from("resume body").toString("base64"),
            },
            {
              sourceId: "manual-1",
              kind: "manual" as const,
              uri: "manual://note",
              mediaType: "text/plain",
              storageKey: "manual-1",
              summary: "No inline bytes",
            },
          ],
          { serviceName: "profile-provider", entityId: "profile-1" },
        )
        const storage = yield* ObjectStorage
        const saved = yield* storage.getObject({
          bucket: "source-assets",
          key: uploaded[0]?.storageKey ?? "",
        })
        return {
          saved: Buffer.from(saved).toString("utf8"),
          uploaded,
        }
      }).pipe(Effect.provide(ObjectStorageMemory))
    )

    const uploadedSource = result.uploaded[0]
    const metadataOnlySource = result.uploaded[1]
    expect(uploadedSource).toMatchObject({
      uri: `s3://source-assets/${uploadedSource?.storageKey}`,
      storageKey: expect.stringMatching(
        /^profile-provider\/profile-1\/resume-1\/[a-f0-9]{16}-resume\.txt$/,
      ),
    })
    expect(uploadedSource).not.toHaveProperty("contentBase64")
    expect(metadataOnlySource).toEqual({
      sourceId: "manual-1",
      kind: "manual",
      uri: "manual://note",
      mediaType: "text/plain",
      storageKey: "manual-1",
      summary: "No inline bytes",
    })
    expect(result.saved).toBe("resume body")
  })
})
