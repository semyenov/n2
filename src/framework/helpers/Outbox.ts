/**
 * @since 1.0.0
 *
 * Generic transactional outbox pattern for event-driven services.
 *
 * Provides: enqueue messages during projections, claim/dispatch/retry
 * in a background worker with exponential backoff.
 *
 * **Requires PostgreSQL.** `claimPending` uses `FOR UPDATE SKIP LOCKED`
 * which is a Postgres extension and is not supported by SQLite or MySQL.
 *
 * The domain creates the SQL table via migrations. Expected schema:
 * ```sql
 * CREATE TABLE <table> (
 *   id TEXT PRIMARY KEY,
 *   payload_json TEXT NOT NULL,
 *   status TEXT NOT NULL DEFAULT 'pending',
 *   retry_count INTEGER NOT NULL DEFAULT 0,
 *   last_error TEXT NOT NULL DEFAULT '',
 *   created_at TEXT NOT NULL,
 *   updated_at TEXT NOT NULL,
 *   published_at TEXT NOT NULL DEFAULT '',
 *   next_attempt_at TEXT NOT NULL DEFAULT ''
 * );
 * ```
 *
 * @example
 * ```ts
 * const outbox = makeOutboxService({
 *   table: "my_event_outbox",
 *   idOf: (m) => m.id,
 *   serialize: (m) => JSON.stringify(Schema.encodeSync(MyMessage)(m)),
 *   deserialize: (json) => Schema.decodeUnknownSync(MyMessage)(JSON.parse(json))
 * })
 *
 * export class MyOutbox extends Context.Tag("MyOutbox")<MyOutbox, OutboxService<MyMessage>>() {}
 * export const MyOutboxLive = outbox.makeLive(MyOutbox)
 * export const MyOutboxWorkerLive = outbox.makeWorkerLive({ ... })
 * ```
 */
import * as Cause from "effect/Cause"
import type * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type { DurationInput } from "effect/Duration"
import { SqlClient, type SqlClient as SqlClientInstance } from "@effect/sql/SqlClient"
import type { SqlError } from "@effect/sql/SqlError"

export interface OutboxEntry<Message> {
  readonly id: string
  readonly message: Message
  readonly retryCount: number
}

export interface OutboxService<Message> {
  readonly enqueue: (message: Message) => Effect.Effect<void, SqlError>
  readonly claimPending: (limit: number) => Effect.Effect<ReadonlyArray<OutboxEntry<Message>>, SqlError>
  readonly markDispatched: (id: string) => Effect.Effect<void, SqlError>
  readonly markFailed: (id: string, retryCount: number, error: string) => Effect.Effect<void, SqlError>
}

/** Exponential backoff delay capped at 300 seconds (5 minutes). */
export const computeRetryDelaySeconds = (attempt: number) =>
  Math.min(2 ** attempt, 300)

type OutboxRow = {
  readonly id: string
  readonly payload_json: string
  readonly retry_count: number
}

interface OutboxConfig<Message> {
  readonly table: string
  readonly idOf: (message: Message) => string
  readonly serialize: (message: Message) => string
  readonly deserialize: (json: string) => Message
}

const nowIso = () => new Date().toISOString()

const makeEnqueue = <Message>(sql: SqlClientInstance, config: OutboxConfig<Message>) =>
  (message: Message) => {
    const now = nowIso()
    const id = config.idOf(message)
    const payloadJson = config.serialize(message)
    return sql`
      INSERT INTO ${sql(config.table)}
        (id, payload_json, status, retry_count, last_error, created_at, updated_at, published_at, next_attempt_at)
      VALUES (
        ${id}, ${payloadJson}, ${"pending"}, ${0}, ${""},
        ${now}, ${now}, ${""}, ${""}
      )
      ON CONFLICT (id) DO NOTHING
    `.pipe(Effect.asVoid)
  }

// FOR UPDATE SKIP LOCKED is a Postgres extension — see module-level note.
const makeClaimPending = <Message>(sql: SqlClientInstance, config: OutboxConfig<Message>) =>
  (limit: number) => {
    const now = nowIso()
    return sql.withTransaction(
      sql<OutboxRow>`
        WITH candidates AS (
          SELECT id
          FROM ${sql(config.table)}
          WHERE (status = ${"pending"} OR status = ${"failed"})
            AND (next_attempt_at = ${""} OR next_attempt_at <= ${now})
          ORDER BY created_at, id
          LIMIT ${limit}
          FOR UPDATE SKIP LOCKED
        )
        UPDATE ${sql(config.table)}
        SET status = ${"processing"}, updated_at = ${now}
        WHERE id IN (SELECT id FROM candidates)
        RETURNING id, payload_json, retry_count
      `.pipe(
        Effect.map((rows) => rows.map((row): OutboxEntry<Message> => ({
          id: row.id,
          retryCount: row.retry_count,
          message: config.deserialize(row.payload_json)
        })))
      )
    )
  }

const makeMarkDispatched = (sql: SqlClientInstance, table: string) =>
  (id: string) => {
    const now = nowIso()
    return sql`
      UPDATE ${sql(table)}
      SET status = ${"dispatched"},
          updated_at = ${now},
          published_at = ${now},
          next_attempt_at = ${""},
          last_error = ${""}
      WHERE id = ${id}
    `.pipe(Effect.asVoid)
  }

const makeMarkFailed = (sql: SqlClientInstance, table: string) =>
  (id: string, retryCount: number, error: string) => {
    const now = nowIso()
    const nextAttemptAt = new Date(Date.now() + computeRetryDelaySeconds(retryCount) * 1000).toISOString()
    return sql`
      UPDATE ${sql(table)}
      SET status = ${"failed"},
          retry_count = ${retryCount},
          last_error = ${error},
          updated_at = ${now},
          published_at = ${""},
          next_attempt_at = ${nextAttemptAt}
      WHERE id = ${id}
    `.pipe(Effect.asVoid)
  }

/**
 * Creates a generic outbox service factory for a given message type.
 *
 * @param config.table - SQL table name
 * @param config.idOf - Extract unique ID from a message
 * @param config.serialize - Serialize message to JSON string for storage
 * @param config.deserialize - Deserialize JSON string back to message
 */
export const makeOutboxService = <Message>(config: OutboxConfig<Message>) => ({
  /**
   * Create a Layer that provides the outbox service backed by PostgreSQL.
   */
  makeLive: <I>(
    tag: Context.Tag<I, OutboxService<Message>>
  ): Layer.Layer<I, never, SqlClient> =>
    Layer.effect(
      tag,
      Effect.gen(function* () {
        const sql = yield* SqlClient
        return {
          enqueue: makeEnqueue(sql, config),
          claimPending: makeClaimPending(sql, config),
          markDispatched: makeMarkDispatched(sql, config.table),
          markFailed: makeMarkFailed(sql, config.table)
        } satisfies OutboxService<Message>
      })
    ),

  /**
   * Create a single drain pass that claims pending entries and publishes them.
   * Returns `true` if work was done, `false` if queue was empty.
   */
  makeDrainOnce: <I, PublishR>(options: {
    readonly outbox: Context.Tag<I, OutboxService<Message>>
    readonly publish: (message: Message) => Effect.Effect<void, unknown, PublishR>
    readonly batchSize?: number
  }): Effect.Effect<boolean, SqlError, I | PublishR> =>
    Effect.gen(function* () {
      const outbox = yield* options.outbox
      const entries = yield* outbox.claimPending(options.batchSize ?? 50)

      if (entries.length === 0) return false

      yield* Effect.forEach(entries, (entry) =>
        options.publish(entry.message).pipe(
          Effect.flatMap(() => outbox.markDispatched(entry.id)),
          Effect.catchAllCause((cause) =>
            outbox.markFailed(entry.id, entry.retryCount + 1, Cause.pretty(cause)).pipe(
              Effect.zipRight(
                Effect.logWarning("outbox dispatch failed").pipe(
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

      return true
    }),

  /**
   * Create a Layer that runs a background worker draining the outbox.
   */
  makeWorkerLive: <I, PublishR>(options: {
    readonly outbox: Context.Tag<I, OutboxService<Message>>
    readonly publish: (message: Message) => Effect.Effect<void, unknown, PublishR>
    readonly batchSize?: number
    readonly idleDelay?: DurationInput
  }): Layer.Layer<never, never, I | PublishR> => {
    const drainOnce = makeOutboxService(config).makeDrainOnce({
      outbox: options.outbox,
      publish: options.publish,
      batchSize: options.batchSize
    })
    const idleDelay = options.idleDelay ?? "1 second"

    const loop: Effect.Effect<never, never, I | PublishR> = Effect.gen(function* () {
      const hadWork = yield* drainOnce.pipe(
        Effect.catchAll((error) =>
          Effect.logError("outbox worker storage failure").pipe(
            Effect.annotateLogs({ error: String(error) }),
            Effect.zipRight(Effect.succeed(false as const))
          )
        )
      )
      if (!hadWork) {
        yield* Effect.sleep(idleDelay)
      }
      return yield* loop
    })

    return Layer.scopedDiscard(
      Effect.forkScoped(loop).pipe(Effect.asVoid)
    ) as Layer.Layer<never, never, I | PublishR>
  }
})
