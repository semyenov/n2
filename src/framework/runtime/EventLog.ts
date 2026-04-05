/**
 * @since 1.0.0
 * @module EventLog
 *
 * Event log service interface for aggregate event persistence.
 *
 * This module provides both:
 * 1. Our stream-based EventLog interface (for traditional DDD append+concurrency)
 * 2. Re-exports of @effect/experimental EventJournal for the native approach
 *
 * For new code, prefer the native EventJournal + EventLog.group pattern.
 * The stream-based interface is kept for Kafka projection compatibility.
 */
import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import type * as Stream from "effect/Stream"
import { EventJournal } from "@effect/experimental"
import type { EventEnvelope } from "../contracts/EventEnvelope.js"
import type { ConcurrencyError, Revision } from "../domain/Revision.js"

/**
 * Stream-based event log for aggregate event persistence.
 * Use this when you need optimistic concurrency on named streams.
 *
 * @since 1.0.0
 * @category models
 */
export interface EventLogService {
  readonly append: (
    streamId: string,
    events: ReadonlyArray<EventEnvelope>,
    expectedRevision: Revision
  ) => Effect.Effect<Revision, ConcurrencyError>

  readonly read: (
    streamId: string,
    fromRevision?: Revision
  ) => Stream.Stream<EventEnvelope>

  readonly readAll: (
    fromPosition?: bigint
  ) => Stream.Stream<EventEnvelope>
}

/**
 * @since 1.0.0
 * @category tags
 */
export class EventLog extends Context.Tag("n2/EventLog")<
  EventLog,
  EventLogService
>() {}

/**
 * Re-export @effect/experimental EventJournal for native event sourcing.
 *
 * EventJournal provides:
 * - entries(): read all events
 * - write(): transactional write with pre-commit effect
 * - changes(): real-time subscription (Queue<Entry>)
 * - destroy(): wipe journal
 *
 * Use with EventLog.group() + Event/EventGroup for typed event handling.
 *
 * @since 1.0.0
 * @category re-exports
 */
export { EventJournal }
