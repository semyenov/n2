/**
 * @since 1.0.0
 * @module PgEventLog
 *
 * EventLog implementation using @effect/sql-pg.
 */
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import * as Layer from "effect/Layer"
import * as DateTime from "effect/DateTime"
import * as Schema from "effect/Schema"
import { EntityId, EntityType } from "@effect/cluster"
import { SqlClient } from "@effect/sql"
import { EventLog, type EventLogService } from "../../framework/runtime/EventLog.js"
import { EventEnvelope } from "../../framework/contracts/EventEnvelope.js"
import { Metadata } from "../../framework/contracts/Metadata.js"
import { type Revision, ConcurrencyError, make as makeRevision } from "../../framework/domain/Revision.js"

const decodeMetadata = Schema.decodeUnknownSync(Metadata)

const rowToEnvelope = (row: Record<string, unknown>): EventEnvelope =>
  new EventEnvelope({
    eventId: String(row["event_id"]),
    streamId: String(row["stream_id"]),
    aggregateId: EntityId.make(String(row["aggregate_id"])),
    aggregateType: Schema.decodeSync(EntityType.EntityType)(String(row["aggregate_type"])),
    revision: Number(row["revision"]),
    occurredAt: DateTime.unsafeMake(Number(row["occurred_at"])),
    metadata: decodeMetadata(row["metadata"]),
    payload: row["payload"]
  })

/**
 * @since 1.0.0
 * @category layers
 */
export const layer: Layer.Layer<EventLog, never, SqlClient.SqlClient> =
  Layer.effect(
    EventLog,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient

      const append = (
        streamId: string,
        events: ReadonlyArray<EventEnvelope>,
        expectedRevision: Revision
      ): Effect.Effect<Revision, ConcurrencyError> =>
        Effect.gen(function*() {
          const rows: ReadonlyArray<any> = yield* sql`
            SELECT COALESCE(MAX(revision), 0) as max_rev
            FROM n2_events
            WHERE stream_id = ${streamId}
          `.pipe(Effect.orDie)

          const currentRevision = rows[0]?.max_rev ?? 0

          if (currentRevision !== expectedRevision) {
            return yield* new ConcurrencyError({
              expected: expectedRevision,
              actual: makeRevision(currentRevision),
              streamId
            })
          }

          for (const event of events) {
            yield* sql`
              INSERT INTO n2_events (
                event_id, stream_id, aggregate_id, aggregate_type,
                revision, occurred_at, metadata, payload
              ) VALUES (
                ${event.eventId},
                ${event.streamId},
                ${event.aggregateId},
                ${event.aggregateType},
                ${event.revision},
                ${DateTime.toDate(event.occurredAt).toISOString()},
                ${JSON.stringify(event.metadata)}::jsonb,
                ${JSON.stringify(event.payload)}::jsonb
              )
            `.pipe(Effect.orDie)
          }

          return events.length > 0
            ? makeRevision(events[events.length - 1]!.revision)
            : expectedRevision
        })

      const read = (
        streamId: string,
        fromRevision?: Revision
      ): Stream.Stream<EventEnvelope> => {
        const fromRev = fromRevision ?? 0
        return Stream.fromEffect(
          sql`
            SELECT * FROM n2_events
            WHERE stream_id = ${streamId}
            AND revision > ${fromRev}
            ORDER BY revision ASC
          `.pipe(Effect.orDie)
        ).pipe(
          Stream.flatMap((rows) =>
            Stream.fromIterable(
              (rows as ReadonlyArray<Record<string, unknown>>).map(rowToEnvelope)
            )
          )
        )
      }

      const readAll = (
        fromPosition?: bigint
      ): Stream.Stream<EventEnvelope> => {
        const fromId = fromPosition !== undefined ? Number(fromPosition) : 0
        return Stream.fromEffect(
          sql`
            SELECT * FROM n2_events
            WHERE id > ${fromId}
            ORDER BY id ASC
          `.pipe(Effect.orDie)
        ).pipe(
          Stream.flatMap((rows) =>
            Stream.fromIterable(
              (rows as ReadonlyArray<Record<string, unknown>>).map(rowToEnvelope)
            )
          )
        )
      }

      return { append, read, readAll } satisfies EventLogService
    })
  )
