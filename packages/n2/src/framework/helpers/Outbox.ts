/**
 * @since 1.0.0
 *
 * Generic transactional outbox pattern for event-driven services.
 *
 * Provides: enqueue messages during projections, claim/dispatch/retry
 * in a background worker with exponential backoff.
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
import * as Metric from "effect/Metric"
import type { ParseError } from "effect/ParseResult"
import * as Schema from "effect/Schema"
import type { DurationInput } from "effect/Duration"
import { SqlClient, type SqlClient as SqlClientInstance } from "@effect/sql/SqlClient"
import type { SqlError } from "@effect/sql/SqlError"

export interface OutboxEntry<Message> {
  readonly id: string
  readonly message: Message
  readonly retryCount: number
}

export interface OutboxService<Message> {
  readonly enqueue: (message: Message) => Effect.Effect<void, SqlError | ParseError>
  readonly claimPending: (limit: number) => Effect.Effect<ReadonlyArray<OutboxEntry<Message>>, SqlError | ParseError>
  readonly markDispatched: (id: string) => Effect.Effect<void, SqlError>
  readonly markFailed: (id: string, retryCount: number, error: string) => Effect.Effect<void, SqlError>
  readonly markDeadLetter: (id: string, error: string) => Effect.Effect<void, SqlError>
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
  readonly serialize: (message: Message) => Effect.Effect<string, ParseError>
  readonly deserialize: (json: string) => Effect.Effect<Message, ParseError>
  /** Optional metrics prefix. When set, emits counters for dispatch, errors, and dead letters. */
  readonly metrics?: { readonly prefix: string }
}

export interface OutboxJsonConfig<Message, Encoded> {
  readonly table: string
  readonly schema: Schema.Schema<Message, Encoded>
  readonly idOf: (message: Message) => string
  /** Optional metrics prefix. When set, emits counters for dispatch, errors, and dead letters. */
  readonly metrics?: { readonly prefix: string }
}

const nowIso = () => new Date().toISOString()

const makeEnqueue = <Message>(sql: SqlClientInstance, config: OutboxConfig<Message>) =>
  (message: Message): Effect.Effect<void, SqlError | ParseError> =>
    Effect.gen(function* () {
      const now = nowIso()
      const id = config.idOf(message)
      const payloadJson = yield* config.serialize(message)
      yield* sql`
        INSERT INTO ${sql(config.table)}
          (id, payload_json, status, retry_count, last_error, created_at, updated_at, published_at, next_attempt_at)
        VALUES (
          ${id}, ${payloadJson}, ${"pending"}, ${0}, ${""},
          ${now}, ${now}, ${""}, ${""}
        )
        ON CONFLICT (id) DO NOTHING
      `
    })

const makeClaimPending = <Message>(sql: SqlClientInstance, config: OutboxConfig<Message>) =>
  (limit: number): Effect.Effect<ReadonlyArray<OutboxEntry<Message>>, SqlError | ParseError> => {
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
        Effect.flatMap((rows) =>
          Effect.forEach(rows, (row) =>
            config.deserialize(row.payload_json).pipe(
              Effect.map((message): OutboxEntry<Message> => ({
                id: row.id,
                retryCount: row.retry_count,
                message
              }))
            )
          )
        )
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

const makeMarkDeadLetter = (sql: SqlClientInstance, table: string) =>
  (id: string, error: string) => {
    const now = nowIso()
    return sql`
      UPDATE ${sql(table)}
      SET status = ${"dead_letter"},
          last_error = ${error},
          updated_at = ${now},
          next_attempt_at = ${"never"}
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
export const makeOutboxService = <Message>(config: OutboxConfig<Message>) => {
  const meters = config.metrics
    ? {
        dispatchTotal: Metric.counter(`${config.metrics.prefix}.dispatch.total`, { incremental: true }),
        dispatchErrors: Metric.counter(`${config.metrics.prefix}.dispatch.errors`, { incremental: true }),
        deadLetterTotal: Metric.counter(`${config.metrics.prefix}.dead_letter.total`, { incremental: true })
      }
    : undefined

  return ({
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
          markFailed: makeMarkFailed(sql, config.table),
          markDeadLetter: makeMarkDeadLetter(sql, config.table)
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
    /** Maximum retry attempts before moving to dead letter. Default: unlimited. */
    readonly maxRetries?: number
    /** Called when an entry is moved to dead letter status. */
    readonly onDeadLetter?: (entry: OutboxEntry<Message>, error: string) => Effect.Effect<void, unknown, PublishR>
  }): Effect.Effect<boolean, never, I | PublishR> =>
    Effect.gen(function* () {
      const outbox = yield* options.outbox
      const entries = yield* outbox.claimPending(options.batchSize ?? 50)

      if (entries.length === 0) return false as const

      yield* Effect.forEach(entries, (entry) =>
        options.publish(entry.message).pipe(
          Effect.flatMap(() => outbox.markDispatched(entry.id)),
          Effect.tap(() => meters ? Metric.increment(meters.dispatchTotal) : Effect.void),
          Effect.catchAllCause((cause) => {
            const errorMsg = Cause.pretty(cause)
            const nextRetryCount = entry.retryCount + 1
            if (options.maxRetries !== undefined && nextRetryCount > options.maxRetries) {
              return outbox.markDeadLetter(entry.id, errorMsg).pipe(
                Effect.zipRight(meters ? Metric.increment(meters.deadLetterTotal) : Effect.void),
                Effect.zipRight(
                  Effect.logError("outbox entry moved to dead letter").pipe(
                    Effect.annotateLogs({ outboxId: entry.id, retryCount: nextRetryCount, error: errorMsg })
                  )
                ),
                Effect.zipRight(
                  options.onDeadLetter
                    ? options.onDeadLetter(entry, errorMsg).pipe(Effect.ignore)
                    : Effect.void
                )
              )
            }
            return outbox.markFailed(entry.id, nextRetryCount, errorMsg).pipe(
              Effect.zipRight(meters ? Metric.increment(meters.dispatchErrors) : Effect.void),
              Effect.zipRight(
                Effect.logWarning("outbox dispatch failed").pipe(
                  Effect.annotateLogs({ outboxId: entry.id, retryCount: nextRetryCount, error: errorMsg })
                )
              )
            )
          })
        ), { discard: true, concurrency: 1 })

      return true as const
    }) as Effect.Effect<boolean, never, I | PublishR>,

  /**
   * Create a Layer that runs a background worker draining the outbox.
   */
  makeWorkerLive: <I, PublishR>(options: {
    readonly outbox: Context.Tag<I, OutboxService<Message>>
    readonly publish: (message: Message) => Effect.Effect<void, unknown, PublishR>
    readonly batchSize?: number
    readonly idleDelay?: DurationInput
    /** Maximum retry attempts before moving to dead letter. Default: unlimited. */
    readonly maxRetries?: number
    /** Called when an entry is moved to dead letter status. */
    readonly onDeadLetter?: (entry: OutboxEntry<Message>, error: string) => Effect.Effect<void, unknown, PublishR>
  }): Layer.Layer<never, never, I | PublishR> => {
    const drainOnce = makeOutboxService(config).makeDrainOnce({
      outbox: options.outbox,
      publish: options.publish,
      batchSize: options.batchSize,
      maxRetries: options.maxRetries,
      onDeadLetter: options.onDeadLetter
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
      Effect.forkScoped(
        Effect.sleep(idleDelay).pipe(
          Effect.zipRight(loop),
          Effect.onInterrupt(() => Effect.logInfo("outbox worker shutting down"))
        )
      ).pipe(Effect.asVoid)
    )
  }
})
}

/**
 * Creates an outbox service factory for Schema-backed JSON messages.
 *
 * This keeps production services from repeating the same
 * `Schema.encodeSync` / `Schema.decodeUnknownSync` boilerplate while still
 * delegating storage, claiming, retry, and worker behavior to `makeOutboxService`.
 */
export const makeOutboxJsonService = <Message, Encoded>(
  config: OutboxJsonConfig<Message, Encoded>
) => {
  const encode = Schema.encode(config.schema)
  const decode = Schema.decodeUnknown(config.schema)

  return makeOutboxService({
    table: config.table,
    idOf: config.idOf,
    serialize: (message) => encode(message).pipe(Effect.map((encoded) => JSON.stringify(encoded))),
    deserialize: (json) => decode(JSON.parse(json)),
    metrics: config.metrics
  })
}
