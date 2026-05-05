import { createHash } from "node:crypto"
import type { Readable } from "node:stream"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Minio from "minio"

export class ObjectStorageError extends Schema.TaggedError<ObjectStorageError>()(
  "ObjectStorageError",
  { message: Schema.String },
) {}

export type ObjectReference = {
  readonly bucket: string
  readonly key: string
  readonly uri: string
}

export type PutObjectInput = {
  readonly bucket: string
  readonly key: string
  readonly body: Uint8Array | string
  readonly contentType?: string
  readonly metadata?: Record<string, string>
}

export type GetObjectInput = {
  readonly bucket: string
  readonly key: string
}

export type DeleteObjectInput = {
  readonly bucket: string
  readonly key: string
}

export class ObjectStorage extends Context.Tag("ObjectStorage")<
  ObjectStorage,
  {
    readonly ensureBucket: (
      bucket: string,
    ) => Effect.Effect<void, ObjectStorageError>
    readonly putObject: (
      input: PutObjectInput,
    ) => Effect.Effect<ObjectReference, ObjectStorageError>
    readonly getObject: (
      input: GetObjectInput,
    ) => Effect.Effect<Uint8Array, ObjectStorageError>
    readonly deleteObject: (
      input: DeleteObjectInput,
    ) => Effect.Effect<void, ObjectStorageError>
  }
>() {}

type SourceAssetPayload = {
  readonly sourceId: string
  readonly kind: "file" | "url" | "manual" | "parsed"
  readonly uri: string
  readonly mediaType: string
  readonly storageKey: string
  readonly summary: string
  readonly contentBase64?: string
}

const defaultSourceBucket = "n2-source-assets"
const defaultPiiBucket = "n2-pii-payloads"

export const sourceAssetsBucket = () =>
  process.env.SOURCE_ASSETS_BUCKET?.trim() || defaultSourceBucket

export const piiPayloadsBucket = () =>
  process.env.PII_PAYLOADS_BUCKET?.trim() || defaultPiiBucket

export const objectUri = (bucket: string, key: string) =>
  `s3://${bucket}/${key}`

export const parseObjectUri = (uri: string): ObjectReference | undefined => {
  if (!uri.startsWith("s3://")) return undefined
  const withoutScheme = uri.slice("s3://".length)
  const separator = withoutScheme.indexOf("/")
  if (separator <= 0 || separator === withoutScheme.length - 1)
    return undefined
  const bucket = withoutScheme.slice(0, separator)
  const key = withoutScheme.slice(separator + 1)
  return { bucket, key, uri }
}

const stripSourceContent = <A extends SourceAssetPayload>(source: A) => {
  const { contentBase64: _contentBase64, ...rest } = source
  return rest
}

const sanitizeObjectKeyPart = (value: string) =>
  value
    .trim()
    .replaceAll(/[^A-Za-z0-9._/-]+/g, "_")
    .replaceAll(/\/+/g, "/")
    .replace(/^\/+|\/+$/g, "") || "object"

const hashText = (value: string) =>
  createHash("sha256").update(value).digest("hex").slice(0, 16)

export const sourceObjectKey = (
  serviceName: string,
  entityId: string,
  source: SourceAssetPayload,
) => {
  const sourceId = sanitizeObjectKeyPart(source.sourceId)
  const storageKey = sanitizeObjectKeyPart(
    source.storageKey || source.uri || source.sourceId,
  )
  return `${sanitizeObjectKeyPart(serviceName)}/${sanitizeObjectKeyPart(entityId)}/${sourceId}/${hashText(storageKey)}-${storageKey}`
}

export const uploadSourceAssetPayloads = <A extends SourceAssetPayload>(
  sources: ReadonlyArray<A>,
  options: {
    readonly serviceName: string
    readonly entityId: string
  },
) =>
  Effect.gen(function* () {
    const storage = yield* ObjectStorage
    const bucket = sourceAssetsBucket()
    return yield* Effect.forEach(
      sources,
      (source) => {
        const content = source.contentBase64?.trim()
        if (content === undefined || content.length === 0) {
          return Effect.succeed(stripSourceContent(source))
        }

        const key = sourceObjectKey(
          options.serviceName,
          options.entityId,
          source,
        )
        return storage
          .putObject({
            bucket,
            key,
            body: Buffer.from(content, "base64"),
            contentType: source.mediaType,
            metadata: {
              sourceId: source.sourceId,
              originalUri: source.uri,
            },
          })
          .pipe(
            Effect.map((reference) => ({
              ...stripSourceContent(source),
              uri: reference.uri,
              storageKey: reference.key,
            })),
          )
      },
      { concurrency: "unbounded" },
    )
  })

const isLocalRuntime = () => {
  const deployment = process.env.DEPLOYMENT_ENVIRONMENT?.trim().toLowerCase()
  const nodeEnv = process.env.NODE_ENV?.trim().toLowerCase()
  if (deployment !== undefined && deployment.length > 0) {
    return deployment === "local" || deployment === "development" || deployment === "test"
  }
  return (
    nodeEnv === "development" ||
    nodeEnv === "test" ||
    process.env.VITEST !== undefined
  )
}

const optionalEnv = (name: string) => {
  const value = process.env[name]?.trim()
  return value === undefined || value.length === 0 ? undefined : value
}

const requireEnv = (
  name: string,
  fallback: string,
  localRuntime: boolean,
) => {
  const value = optionalEnv(name)
  if (value !== undefined) return value
  if (localRuntime) return fallback
  throw new ObjectStorageError({
    message: `${name} is required outside local/test environments`,
  })
}

const envBoolean = (
  name: string,
  fallback: boolean,
) => {
  const raw = optionalEnv(name)
  if (raw === undefined) return fallback

  const value = raw.toLowerCase()
  if (value === "1" || value === "true" || value === "yes") return true
  if (value === "0" || value === "false" || value === "no") return false

  throw new ObjectStorageError({
    message: `${name} must be a boolean value, got "${raw}"`,
  })
}

const envPort = (
  name: string,
  fallback: string,
) => {
  const raw = optionalEnv(name) ?? fallback
  const port = Number(raw)
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new ObjectStorageError({
      message: `${name} must be an integer port between 1 and 65535, got "${raw}"`,
    })
  }
  return port
}

const objectStorageConfig = () =>
  Effect.try({
    try: () => {
      const localRuntime = isLocalRuntime()
      const endpointRaw = requireEnv(
        "OBJECT_STORAGE_ENDPOINT",
        "127.0.0.1",
        localRuntime,
      )
      const parsed =
        endpointRaw.startsWith("http://") || endpointRaw.startsWith("https://")
          ? new URL(endpointRaw)
          : undefined
      const protocolUseSsl = parsed?.protocol === "https:"
      const useSSL = envBoolean("OBJECT_STORAGE_USE_SSL", protocolUseSsl)
      const port = envPort(
        "OBJECT_STORAGE_PORT",
        parsed?.port || (useSSL ? "443" : "9000"),
      )

      return {
        endPoint: parsed?.hostname ?? endpointRaw,
        port,
        useSSL,
        accessKey: requireEnv(
          "OBJECT_STORAGE_ACCESS_KEY",
          "minioadmin",
          localRuntime,
        ),
        secretKey: requireEnv(
          "OBJECT_STORAGE_SECRET_KEY",
          "minioadmin",
          localRuntime,
        ),
        region: optionalEnv("OBJECT_STORAGE_REGION") || "us-east-1",
      }
    },
    catch: (cause) =>
      cause instanceof ObjectStorageError
        ? cause
        : new ObjectStorageError({
            message: `Invalid object storage configuration: ${String(cause)}`,
          }),
  })

const toStorageError = (message: string, cause: unknown) =>
  new ObjectStorageError({ message: `${message}: ${String(cause)}` })

const readStream = async (stream: Readable) => {
  const chunks: Array<Buffer> = []
  for await (const chunk of stream) {
    chunks.push(
      Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array),
    )
  }
  return Buffer.concat(chunks)
}

export const ObjectStorageLive = Layer.effect(
  ObjectStorage,
  Effect.gen(function* () {
    const config = yield* objectStorageConfig()
    const client = new Minio.Client({
      endPoint: config.endPoint,
      port: config.port,
      useSSL: config.useSSL,
      accessKey: config.accessKey,
      secretKey: config.secretKey,
    })

    const ensureBucket = (bucket: string) =>
      Effect.tryPromise({
        try: async () => {
          if (!(await client.bucketExists(bucket))) {
            await client.makeBucket(bucket, config.region)
          }
        },
        catch: (cause) =>
          toStorageError(`Failed to ensure object bucket "${bucket}"`, cause),
      })

    return {
      ensureBucket,
      putObject: (input) =>
        ensureBucket(input.bucket).pipe(
          Effect.zipRight(
            Effect.tryPromise({
              try: async () => {
                const body =
                  typeof input.body === "string"
                    ? Buffer.from(input.body)
                    : Buffer.from(input.body)
                await client.putObject(
                  input.bucket,
                  input.key,
                  body,
                  body.byteLength,
                  {
                    "Content-Type":
                      input.contentType ?? "application/octet-stream",
                    ...input.metadata,
                  },
                )
                return {
                  bucket: input.bucket,
                  key: input.key,
                  uri: objectUri(input.bucket, input.key),
                }
              },
              catch: (cause) =>
                toStorageError(
                  `Failed to put object "${input.bucket}/${input.key}"`,
                  cause,
                ),
            }),
          ),
        ),
      getObject: (input) =>
        Effect.tryPromise({
          try: async () =>
            readStream(await client.getObject(input.bucket, input.key)),
          catch: (cause) =>
            toStorageError(
              `Failed to get object "${input.bucket}/${input.key}"`,
              cause,
            ),
        }),
      deleteObject: (input) =>
        Effect.tryPromise({
          try: async () => {
            await client.removeObject(input.bucket, input.key)
          },
          catch: (cause) =>
            toStorageError(
              `Failed to delete object "${input.bucket}/${input.key}"`,
              cause,
            ),
        }),
    }
  }),
)

export const ObjectStorageMemory = Layer.effect(
  ObjectStorage,
  Effect.sync(() => {
    const objects = new Map<string, Uint8Array>()
    const buckets = new Set<string>()
    const objectKey = (bucket: string, key: string) => `${bucket}/${key}`
    return {
      ensureBucket: (bucket: string) =>
        Effect.sync(() => {
          buckets.add(bucket)
        }),
      putObject: (input: PutObjectInput) =>
        Effect.sync(() => {
          buckets.add(input.bucket)
          const body =
            typeof input.body === "string"
              ? Buffer.from(input.body)
              : Buffer.from(input.body)
          objects.set(objectKey(input.bucket, input.key), body)
          return {
            bucket: input.bucket,
            key: input.key,
            uri: objectUri(input.bucket, input.key),
          }
        }),
      getObject: (input: GetObjectInput) =>
        Effect.try({
          try: () => {
            const value = objects.get(objectKey(input.bucket, input.key))
            if (value === undefined) {
              throw new Error(
                `Object "${input.bucket}/${input.key}" not found`,
              )
            }
            return value
          },
          catch: (cause) => new ObjectStorageError({ message: String(cause) }),
        }),
      deleteObject: (input: DeleteObjectInput) =>
        Effect.sync(() => {
          objects.delete(objectKey(input.bucket, input.key))
        }),
    }
  }),
)
