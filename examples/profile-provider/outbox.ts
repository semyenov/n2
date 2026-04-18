import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { SqlClient, type SqlClient as SqlClientInstance } from "@effect/sql/SqlClient"
import type { SqlError } from "@effect/sql/SqlError"
import {
  computeRetryDelaySeconds,
  ProfileProviderEventMessage,
  startProfileEventPublish
} from "./workflows.js"

export type OutboxEntry = {
  readonly id: string
  readonly message: ProfileProviderEventMessage
  readonly retryCount: number
}

type OutboxRow = {
  readonly id: string
  readonly profile_id: string
  readonly revision: number
  readonly topic: string
  readonly partition_key: string
  readonly occurred_at: string
  readonly payload_json: string
  readonly headers_json: string
  readonly retry_count: number
}

export class ProfileProviderOutbox extends Context.Tag("ProfileProviderOutbox")<
  ProfileProviderOutbox,
  {
    readonly claimPending: (limit: number) => Effect.Effect<ReadonlyArray<OutboxEntry>, SqlError>
    readonly markDispatched: (id: string) => Effect.Effect<void, SqlError>
    readonly markFailed: (id: string, retryCount: number, error: string) => Effect.Effect<void, SqlError>
  }
>() {}

const nowIso = () => new Date().toISOString()

const toOutboxEntry = (row: OutboxRow) => ({
  id: row.id,
  retryCount: row.retry_count,
  message: (() => {
    const headers = JSON.parse(row.headers_json) as Record<string, unknown>
    return new ProfileProviderEventMessage({
      id: row.id,
      topic: row.topic,
      partitionKey: row.partition_key,
      eventType: String(headers.eventType ?? ""),
      profileId: row.profile_id,
      revision: row.revision,
      occurredAt: row.occurred_at,
      payload: JSON.parse(row.payload_json),
      headers
    })
  })()
})

const claimPending = (sql: SqlClientInstance, limit: number) => {
  const now = nowIso()
  return sql.withTransaction(
    sql<OutboxRow>`
      WITH candidates AS (
        SELECT id
        FROM profile_provider_event_outbox
        WHERE (status = ${"pending"} OR status = ${"failed"})
          AND (next_attempt_at = ${""} OR next_attempt_at <= ${now})
        ORDER BY created_at, id
        LIMIT ${limit}
        FOR UPDATE SKIP LOCKED
      )
      UPDATE profile_provider_event_outbox
      SET status = ${"processing"},
          updated_at = ${now}
      WHERE id IN (SELECT id FROM candidates)
      RETURNING id, profile_id, revision, topic, partition_key, occurred_at, payload_json, headers_json, retry_count
    `.pipe(
      Effect.map((rows) => rows.map(toOutboxEntry))
    )
  )
}

export const ProfileProviderOutboxPgLive = Layer.effect(
  ProfileProviderOutbox,
  Effect.gen(function* () {
    const sql = yield* SqlClient

    return {
      claimPending: (limit: number) => claimPending(sql, limit),
      markDispatched: (id: string) => {
        const now = nowIso()
        return sql`
          UPDATE profile_provider_event_outbox
          SET status = ${"dispatched"},
              updated_at = ${now},
              published_at = ${now},
              next_attempt_at = ${""},
              last_error = ${""}
          WHERE id = ${id}
        `.pipe(Effect.asVoid)
      },
      markFailed: (id: string, retryCount: number, error: string) => {
        const now = nowIso()
        const nextAttemptAt = new Date(Date.now() + computeRetryDelaySeconds(retryCount) * 1000).toISOString()
        return sql`
          UPDATE profile_provider_event_outbox
          SET status = ${"failed"},
              retry_count = ${retryCount},
              last_error = ${error},
              updated_at = ${now},
              published_at = ${""},
              next_attempt_at = ${nextAttemptAt}
          WHERE id = ${id}
        `.pipe(Effect.asVoid)
      }
    }
  })
)

const BATCH_SIZE = 50
const IDLE_DELAY = "1 second"

const drainBatch = Effect.gen(function* () {
  const outbox = yield* ProfileProviderOutbox
  const entries = yield* outbox.claimPending(BATCH_SIZE)

  if (entries.length === 0) {
    return false as const
  }

  yield* Effect.forEach(entries, (entry) =>
    startProfileEventPublish(entry.message).pipe(
      Effect.flatMap(() => outbox.markDispatched(entry.id)),
      Effect.catchAllCause((cause) =>
        outbox.markFailed(entry.id, entry.retryCount + 1, Cause.pretty(cause)).pipe(
          Effect.zipRight(
            Effect.logWarning("[profile-provider] outbox dispatch failed").pipe(
              Effect.annotateLogs({
                outboxId: entry.id,
                retryCount: entry.retryCount + 1,
                error: Cause.pretty(cause)
              })
            )
          )
        )
      )
    ), { discard: true, concurrency: 1 })

  return true as const
})

const outboxWorkerLoop: Effect.Effect<never, never, ProfileProviderOutbox> = Effect.gen(function* () {
  const hadWork = yield* drainBatch.pipe(
    Effect.catchAll((error) =>
      Effect.logError("[profile-provider] outbox worker storage failure").pipe(
        Effect.annotateLogs({ error: String(error) }),
        Effect.zipRight(Effect.succeed(false as const))
      )
    )
  )
  if (!hadWork) {
    yield* Effect.sleep(IDLE_DELAY)
  }
  return yield* outboxWorkerLoop
})

export const ProfileProviderOutboxWorkerLive = Layer.scopedDiscard(
  Effect.forkScoped(outboxWorkerLoop).pipe(Effect.asVoid)
)
