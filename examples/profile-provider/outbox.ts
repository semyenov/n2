import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { SqlClient, type SqlClient as SqlClientService } from "@effect/sql/SqlClient"
import type { SqlError } from "@effect/sql/SqlError"

export const OutboxStatus = Schema.Literal("PENDING", "PROCESSING", "SENT", "FAILED")
export type OutboxStatus = typeof OutboxStatus.Type

const Headers = Schema.Record({ key: Schema.String, value: Schema.Unknown })

export class ProfileProviderOutboxMessage extends Schema.Class<ProfileProviderOutboxMessage>("ProfileProviderOutboxMessage")({
  id: Schema.String,
  topic: Schema.String,
  partitionKey: Schema.String,
  eventType: Schema.String,
  profileId: Schema.UUID,
  revision: Schema.Number.pipe(Schema.int()),
  occurredAt: Schema.String,
  payload: Schema.Unknown,
  headers: Headers
}) {}

export type ProfileProviderOutboxRow = {
  readonly id: string
  readonly profileId: string
  readonly revision: number
  readonly topic: string
  readonly partitionKey: string
  readonly payloadJson: string
  readonly headersJson: string
  readonly status: OutboxStatus
  readonly retryCount: number
  readonly lastError: string
  readonly availableAt: string
  readonly workerId: string | null
  readonly lockedUntil: string | null
  readonly lastAttemptAt: string | null
  readonly processedAt: string | null
  readonly createdAt: string
  readonly updatedAt: string
}

export type ClaimedOutboxMessage = {
  readonly id: string
  readonly retryCount: number
  readonly message: ProfileProviderOutboxMessage
}

export class OutboxWorkerConfig extends Context.Tag("OutboxWorkerConfig")<
  OutboxWorkerConfig,
  {
    readonly batchSize: number
    readonly pollIntervalMs: number
    readonly leaseDurationMs: number
  }
>() {}

export class ProfileProviderOutboxPublisher extends Context.Tag("ProfileProviderOutboxPublisher")<
  ProfileProviderOutboxPublisher,
  {
    readonly publish: (message: ProfileProviderOutboxMessage) => Effect.Effect<void, unknown>
  }
>() {}

export class ProfileProviderOutboxStore extends Context.Tag("ProfileProviderOutboxStore")<
  ProfileProviderOutboxStore,
  {
    readonly enqueue: (message: ProfileProviderOutboxMessage) => Effect.Effect<void, SqlError>
    readonly claimBatch: (
      options: {
        readonly workerId: string
        readonly batchSize: number
        readonly leaseDurationMs: number
        readonly now: Date
      }
    ) => Effect.Effect<ReadonlyArray<ClaimedOutboxMessage>, SqlError>
    readonly markSent: (
      options: {
        readonly id: string
        readonly workerId: string
        readonly processedAt: Date
      }
    ) => Effect.Effect<void, SqlError>
    readonly markFailed: (
      options: {
        readonly id: string
        readonly workerId: string
        readonly failedAt: Date
        readonly availableAt: Date
        readonly error: string
      }
    ) => Effect.Effect<void, SqlError>
  }
>() {}

const TOPIC = "profile-provider.events"
const encodeMessage = Schema.encodeSync(ProfileProviderOutboxMessage)
const decodeMessage = Schema.decodeUnknownSync(ProfileProviderOutboxMessage)

const toIso = (date: Date) => date.toISOString()

export const computeRetryDelaySeconds = (retryCount: number) =>
  Math.min(2 ** retryCount, 300)

export const computeRetryAvailableAt = (now: Date, retryCount: number) =>
  new Date(now.getTime() + computeRetryDelaySeconds(retryCount) * 1000)

export const makeOutboxMessage = (options: {
  readonly profileId: string
  readonly revision: number
  readonly eventType: string
  readonly occurredAt: string
  readonly payload: unknown
  readonly headers?: Record<string, unknown>
}) =>
  new ProfileProviderOutboxMessage({
    id: `${options.profileId}:${options.revision}:${options.eventType}`,
    topic: TOPIC,
    partitionKey: options.profileId,
    eventType: options.eventType,
    profileId: options.profileId,
    revision: options.revision,
    occurredAt: options.occurredAt,
    payload: options.payload,
    headers: {
      eventType: options.eventType,
      profileId: options.profileId,
      revision: options.revision,
      ...(options.headers ?? {})
    }
  })

export const createOutboxRow = (
  message: ProfileProviderOutboxMessage,
  now: Date = new Date()
): ProfileProviderOutboxRow => {
  const encoded = encodeMessage(message)
  const nowIso = toIso(now)
  return {
    id: message.id,
    profileId: message.profileId,
    revision: message.revision,
    topic: message.topic,
    partitionKey: message.partitionKey,
    payloadJson: JSON.stringify(encoded),
    headersJson: JSON.stringify(encoded.headers),
    status: "PENDING",
    retryCount: 0,
    lastError: "",
    availableAt: nowIso,
    workerId: null,
    lockedUntil: null,
    lastAttemptAt: null,
    processedAt: null,
    createdAt: nowIso,
    updatedAt: nowIso
  }
}

const parseClaimedMessage = (row: { readonly id: string; readonly retry_count: number; readonly payload_json: string }) => ({
  id: row.id,
  retryCount: row.retry_count,
  message: decodeMessage(JSON.parse(row.payload_json))
})

export const ProfileProviderOutboxStoreLive = Layer.effect(
  ProfileProviderOutboxStore,
  Effect.gen(function* () {
    const sql = yield* SqlClient

    return {
      enqueue: (message) => insertOutboxRow(sql, message),
      claimBatch: ({ workerId, batchSize, leaseDurationMs, now }) => {
        const nowIso = toIso(now)
        const lockedUntil = toIso(new Date(now.getTime() + leaseDurationMs))

        return sql<{
          readonly id: string
          readonly retry_count: number
          readonly payload_json: string
        }>`
          WITH candidates AS (
            SELECT id
            FROM profile_provider_event_outbox
            WHERE (
              (status = ${"PENDING"} OR status = ${"FAILED"})
              AND available_at <= ${nowIso}
            ) OR (
              status = ${"PROCESSING"}
              AND locked_until IS NOT NULL
              AND locked_until < ${nowIso}
            )
            ORDER BY available_at ASC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT ${sql.literal(batchSize.toString())}
          ),
          claimed AS (
            UPDATE profile_provider_event_outbox AS outbox
            SET status = ${"PROCESSING"},
                worker_id = ${workerId},
                locked_until = ${lockedUntil},
                last_attempt_at = ${nowIso},
                updated_at = ${nowIso}
            WHERE outbox.id IN (SELECT id FROM candidates)
            RETURNING outbox.id, outbox.retry_count, outbox.payload_json, outbox.available_at, outbox.created_at
          )
          SELECT id, retry_count, payload_json
          FROM claimed
          ORDER BY available_at ASC, created_at ASC
        `.pipe(
          sql.withTransaction,
          Effect.map((rows) => rows.map(parseClaimedMessage))
        )
      },
      markSent: ({ id, workerId, processedAt }) =>
        sql`
          UPDATE profile_provider_event_outbox
          SET status = ${"SENT"},
              worker_id = ${null},
              locked_until = ${null},
              processed_at = ${toIso(processedAt)},
              last_error = ${""},
              updated_at = ${toIso(processedAt)}
          WHERE id = ${id}
            AND status = ${"PROCESSING"}
            AND worker_id = ${workerId}
        `.pipe(Effect.asVoid),
      markFailed: ({ id, workerId, failedAt, availableAt, error }) =>
        sql`
          UPDATE profile_provider_event_outbox
          SET status = ${"FAILED"},
              retry_count = retry_count + 1,
              last_error = ${error},
              available_at = ${toIso(availableAt)},
              worker_id = ${null},
              locked_until = ${null},
              processed_at = ${null},
              updated_at = ${toIso(failedAt)}
          WHERE id = ${id}
            AND status = ${"PROCESSING"}
            AND worker_id = ${workerId}
        `.pipe(Effect.asVoid)
    }
  })
)

export const insertOutboxRow = (
  sql: SqlClientService,
  message: ProfileProviderOutboxMessage
) => {
  const row = createOutboxRow(message)
  return sql`
    INSERT INTO profile_provider_event_outbox
      (id, profile_id, revision, topic, partition_key, payload_json, headers_json, status, retry_count, last_error,
       available_at, worker_id, locked_until, last_attempt_at, processed_at, created_at, updated_at)
    VALUES (
      ${row.id},
      ${row.profileId},
      ${row.revision},
      ${row.topic},
      ${row.partitionKey},
      ${row.payloadJson},
      ${row.headersJson},
      ${row.status},
      ${row.retryCount},
      ${row.lastError},
      ${row.availableAt},
      ${row.workerId},
      ${row.lockedUntil},
      ${row.lastAttemptAt},
      ${row.processedAt},
      ${row.createdAt},
      ${row.updatedAt}
    )
    ON CONFLICT (id) DO NOTHING
  `.pipe(Effect.asVoid)
}

export const ProfileProviderOutboxPublisherLive = Layer.succeed(
  ProfileProviderOutboxPublisher,
  {
    publish: (message) =>
      Effect.logInfo("[profile-provider] outbox message published").pipe(
        Effect.annotateLogs({
          outboxId: message.id,
          topic: message.topic,
          partitionKey: message.partitionKey,
          payloadSize: JSON.stringify(message.payload).length
        })
      )
  }
)

export const OutboxWorkerConfigLive = Layer.succeed(
  OutboxWorkerConfig,
  {
    batchSize: 50,
    pollIntervalMs: 1_000,
    leaseDurationMs: 30_000
  }
)

export const runProfileProviderOutboxDispatch = (options: {
  readonly workerId: string
  readonly now?: Date
}) =>
  Effect.gen(function* () {
    const store = yield* ProfileProviderOutboxStore
    const publisher = yield* ProfileProviderOutboxPublisher
    const config = yield* OutboxWorkerConfig
    const now = options.now ?? new Date()
    const claimed = yield* store.claimBatch({
      workerId: options.workerId,
      batchSize: config.batchSize,
      leaseDurationMs: config.leaseDurationMs,
      now
    })

    let sent = 0
    let failed = 0

    for (const entry of claimed) {
      yield* publisher.publish(entry.message).pipe(
        Effect.matchCauseEffect({
          onFailure: (cause) => {
            failed += 1
            const failedAt = options.now ?? new Date()
            const availableAt = computeRetryAvailableAt(failedAt, entry.retryCount + 1)
            return store.markFailed({
              id: entry.id,
              workerId: options.workerId,
              failedAt,
              availableAt,
              error: Cause.pretty(cause)
            }).pipe(
              Effect.zipRight(
                Effect.logWarning("[profile-provider] outbox publish failed").pipe(
                  Effect.annotateLogs({
                    outboxId: entry.id,
                    workerId: options.workerId,
                    retryCount: entry.retryCount + 1
                  })
                )
              )
            )
          },
          onSuccess: () => {
            sent += 1
            return store.markSent({
              id: entry.id,
              workerId: options.workerId,
              processedAt: options.now ?? new Date()
            })
          }
        })
      )
    }

    if (claimed.length > 0) {
      yield* Effect.logInfo("[profile-provider] outbox batch dispatched").pipe(
        Effect.annotateLogs({
          workerId: options.workerId,
          claimed: claimed.length,
          sent,
          failed
        })
      )
    }

    return {
      claimed: claimed.length,
      sent,
      failed
    } as const
  })

const runWorkerLoop = (workerId: string) =>
  Effect.gen(function* () {
    const config = yield* OutboxWorkerConfig
    while (true) {
      yield* runProfileProviderOutboxDispatch({ workerId }).pipe(
        Effect.catchAllCause((cause) =>
          Effect.logError("[profile-provider] outbox worker loop failed").pipe(
            Effect.annotateLogs({
              workerId,
              cause: Cause.pretty(cause)
            })
          )
        )
      )
      yield* Effect.sleep(Duration.millis(config.pollIntervalMs))
    }
  })

export const ProfileProviderOutboxWorkerLayer = Layer.scopedDiscard(
  Effect.gen(function* () {
    const workerId = crypto.randomUUID()
    yield* Effect.logInfo("[profile-provider] outbox worker started").pipe(
      Effect.annotateLogs({ workerId })
    )
    yield* Effect.forkScoped(runWorkerLoop(workerId))
  })
)
