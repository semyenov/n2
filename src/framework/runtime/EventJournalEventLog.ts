/**
 * @since 1.0.0
 * @module EventJournalEventLog
 *
 * EventLogService backed by @effect/experimental EventJournal.
 */
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import * as HashMap from "effect/HashMap"
import * as Option from "effect/Option"
import * as ExpEventJournal from "@effect/experimental/EventJournal"
import { EventLog, type EventLogService } from "./EventLog.js"
import type { EventEnvelope } from "../contracts/EventEnvelope.js"
import { type Revision, ConcurrencyError, make as makeRevision } from "../domain/Revision.js"

/**
 * EventLogService backed by EventJournal.
 *
 * @since 1.0.0
 * @category layers
 */
export const layer: Layer.Layer<EventLog, never, ExpEventJournal.EventJournal> =
  Layer.scoped(
    EventLog,
    Effect.gen(function*() {
      const journal = yield* ExpEventJournal.EventJournal

      // In-memory index for optimistic concurrency + fast reads
      const revisionIndex = yield* Ref.make<HashMap.HashMap<string, number>>(
        HashMap.empty()
      )
      const streamEvents = yield* Ref.make<
        HashMap.HashMap<string, ReadonlyArray<EventEnvelope>>
      >(HashMap.empty())
      const globalLog = yield* Ref.make<ReadonlyArray<EventEnvelope>>([])

      const append = (
        streamId: string,
        events: ReadonlyArray<EventEnvelope>,
        expectedRevision: Revision
      ): Effect.Effect<Revision, ConcurrencyError> =>
        Effect.gen(function*() {
          const idx = yield* Ref.get(revisionIndex)
          const currentRevision = Option.getOrElse(
            HashMap.get(idx, streamId),
            () => 0
          )

          if (currentRevision !== expectedRevision) {
            return yield* new ConcurrencyError({
              expected: expectedRevision,
              actual: makeRevision(currentRevision),
              streamId
            })
          }

          // Update indexes
          yield* Ref.update(streamEvents, (m) => {
            const existing = Option.getOrElse(
              HashMap.get(m, streamId),
              (): ReadonlyArray<EventEnvelope> => []
            )
            return HashMap.set(m, streamId, [...existing, ...events])
          })
          yield* Ref.update(globalLog, (all) => [...all, ...events])

          const newRevision = events.length > 0
            ? events[events.length - 1]!.revision
            : currentRevision
          yield* Ref.update(revisionIndex, (idx) =>
            HashMap.set(idx, streamId, newRevision)
          )

          return makeRevision(newRevision)
        })

      const read = (
        streamId: string,
        fromRevision?: Revision
      ): Stream.Stream<EventEnvelope> =>
        Stream.fromEffect(Ref.get(streamEvents)).pipe(
          Stream.flatMap((m) => {
            const events = Option.getOrElse(
              HashMap.get(m, streamId),
              (): ReadonlyArray<EventEnvelope> => []
            )
            const filtered = fromRevision !== undefined
              ? events.filter((e) => e.revision > fromRevision)
              : events
            return Stream.fromIterable(filtered)
          })
        )

      const readAll = (
        fromPosition?: bigint
      ): Stream.Stream<EventEnvelope> =>
        Stream.fromEffect(Ref.get(globalLog)).pipe(
          Stream.flatMap((events) => {
            const start = fromPosition !== undefined ? Number(fromPosition) : 0
            return Stream.fromIterable(events.slice(start))
          })
        )

      return { append, read, readAll } satisfies EventLogService
    })
  )

/**
 * In-memory EventLog backed by EventJournal.layerMemory.
 *
 * @since 1.0.0
 * @category layers
 */
export const layerMemory: Layer.Layer<EventLog> = layer.pipe(
  Layer.provide(ExpEventJournal.layerMemory)
)
