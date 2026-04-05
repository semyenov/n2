/**
 * @since 1.0.0
 * @module AggregateRuntime
 *
 * Aggregate runtime that orchestrates hydration, command handling,
 * event persistence, and Kafka publication.
 *
 * This is the main piece of custom code in the framework.
 * It bridges AggregateDefinition (decide/evolve) with:
 * - EventLog (our stream-based interface, for Kafka projection compat)
 * - SnapshotStore (backed by @effect/experimental Persistence)
 * - KafkaPublisher (our Kafka abstraction)
 * - Snowflake (from @effect/cluster, for event IDs)
 * - DateTime.now (from core effect, for timestamps)
 *
 * For fully native approach, use Entity.toLayer directly with
 * @effect/cluster MessageStorage for persistence.
 */
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"
import * as DateTime from "effect/DateTime"
import * as Schema from "effect/Schema"
import { EntityId, EntityType } from "@effect/cluster"
import type { AggregateDefinition } from "../domain/AggregateDefinition.js"
import { type Revision, InitialRevision, ConcurrencyError, make as makeRevision, next as incrementRevision } from "../domain/Revision.js"
import { EventEnvelope } from "../contracts/EventEnvelope.js"
import { makeAggregateTopicName, makePartitionKey } from "../contracts/TopicKey.js"
import { EventLog } from "./EventLog.js"
import { SnapshotStore, type SnapshotData } from "./SnapshotStore.js"
import { KafkaPublisher, KafkaProducerRecord } from "./KafkaPublisher.js"
import { IdGenerator } from "./IdGenerator.js"
import { getRequestContext } from "./RequestContext.js"

/**
 * @since 1.0.0
 * @category models
 */
export interface HydrateResult<State> {
  readonly state: State
  readonly revision: Revision
}

/**
 * @since 1.0.0
 * @category models
 */
export interface HandleResult<State, Event> {
  readonly state: State
  readonly revision: Revision
  readonly events: ReadonlyArray<Event>
}

/**
 * @since 1.0.0
 * @category models
 */
export interface AggregateRuntime<State, Command, Event, Err, R> {
  readonly hydrate: (
    aggregateId: EntityId.EntityId
  ) => Effect.Effect<HydrateResult<State>, never, EventLog | SnapshotStore>

  readonly handle: (
    aggregateId: EntityId.EntityId,
    command: Command
  ) => Effect.Effect<
    HandleResult<State, Event>,
    Err | ConcurrencyError,
    EventLog | SnapshotStore | KafkaPublisher | IdGenerator | R
  >
}

/**
 * @since 1.0.0
 * @category models
 */
export interface AggregateRuntimeOptions {
  readonly snapshotEvery?: number
}

/**
 * Creates an aggregate runtime from an aggregate definition.
 *
 * Uses:
 * - Snowflake (via IdGenerator) for event IDs
 * - DateTime.now for timestamps
 * - EventLog for stream-based persistence
 * - Persistence (via SnapshotStore) for state caching
 * - KafkaPublisher for event distribution
 *
 * @since 1.0.0
 * @category constructors
 */
export const make = <
  Name extends string,
  State,
  Command,
  Event,
  Err,
  R
>(
  definition: AggregateDefinition<Name, State, Command, Event, Err, R>,
  options?: AggregateRuntimeOptions
): AggregateRuntime<State, Command, Event, Err, R> => {
  const snapshotEvery = options?.snapshotEvery ?? 0
  const aggregateType = Schema.decodeSync(EntityType.EntityType)(definition.name)
  const streamId = (aggregateId: EntityId.EntityId) => `${definition.name}-${aggregateId}`

  const hydrate = (
    aggregateId: EntityId.EntityId
  ): Effect.Effect<HydrateResult<State>, never, EventLog | SnapshotStore> =>
    Effect.gen(function*() {
      const snapshotStore = yield* SnapshotStore
      const eventLog = yield* EventLog

      const snapshotOpt: Option.Option<SnapshotData> = yield* snapshotStore.load(aggregateId)

      let state = definition.initialState
      let revision = InitialRevision

      if (Option.isSome(snapshotOpt)) {
        // Snapshot boundary: state is stored as unknown, cast to State.
        // Schema.decode could be used here for full validation at cost of performance.
        state = snapshotOpt.value.state as State
        revision = snapshotOpt.value.revision
      }

      const events: ReadonlyArray<EventEnvelope> = yield* eventLog
        .read(streamId(aggregateId), revision)
        .pipe(Stream.runCollect, Effect.map((chunk) => [...chunk]))

      for (const envelope of events) {
        // Event boundary: payload stored as Schema.Unknown, evolve expects Event.
        state = definition.evolve(state, envelope.payload as Event)
        revision = makeRevision(envelope.revision)
      }

      return { state, revision }
    }).pipe(Effect.withSpan("n2.aggregate.hydrate", {
      attributes: { "aggregate.type": definition.name, "aggregate.id": aggregateId }
    }))

  const handle = (
    aggregateId: EntityId.EntityId,
    command: Command
  ): Effect.Effect<
    HandleResult<State, Event>,
    Err | ConcurrencyError,
    EventLog | SnapshotStore | KafkaPublisher | IdGenerator | R
  > =>
    Effect.gen(function*() {
      const eventLog = yield* EventLog
      const snapshotStore = yield* SnapshotStore
      const publisher = yield* KafkaPublisher
      const idGen = yield* IdGenerator
      const metadata = yield* getRequestContext

      const { state, revision: currentRevision } = yield* hydrate(aggregateId)
      const newEvents = yield* definition.decide(state, command)

      if (newEvents.length === 0) {
        return { state, revision: currentRevision, events: newEvents }
      }

      const now = yield* DateTime.now
      const sid = streamId(aggregateId)
      const topicName = makeAggregateTopicName(definition.name)
      const partitionKey = makePartitionKey(aggregateId)
      const envelopes: Array<EventEnvelope> = []
      let nextRevision = currentRevision

      for (const event of newEvents) {
        const eventId = yield* idGen.generate
        nextRevision = incrementRevision(nextRevision)
        envelopes.push(
          new EventEnvelope({
            eventId,
            streamId: sid,
            aggregateId,
            aggregateType,
            revision: nextRevision,
            occurredAt: now,
            metadata,
            payload: event
          })
        )
      }

      const newRevision = yield* eventLog.append(sid, envelopes, currentRevision)

      const kafkaRecords = envelopes.map(
        (env) =>
          new KafkaProducerRecord({
            topic: topicName,
            key: partitionKey,
            value: JSON.stringify(env)
          })
      )
      yield* publisher.publishBatch(kafkaRecords)

      let finalState = state
      for (const event of newEvents) {
        finalState = definition.evolve(finalState, event)
      }

      if (snapshotEvery > 0 && newRevision % snapshotEvery === 0) {
        yield* snapshotStore.save(aggregateId, finalState, newRevision)
      }

      return { state: finalState, revision: newRevision, events: newEvents }
    }).pipe(Effect.withSpan("n2.aggregate.handle", {
      attributes: { "aggregate.type": definition.name, "aggregate.id": aggregateId }
    }))

  return { hydrate, handle }
}
