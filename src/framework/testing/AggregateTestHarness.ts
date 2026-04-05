/**
 * @since 1.0.0
 * @module AggregateTestHarness
 *
 * Given/When/Then DSL for testing aggregate behavior with in-memory doubles.
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as DateTime from "effect/DateTime"
import * as Schema from "effect/Schema"
import type { EntityId } from "@effect/cluster/EntityId"
import { EntityType } from "@effect/cluster"
import type {
  AggregateDefinition
} from "../domain/AggregateDefinition.js"
import * as AggregateRuntime from "../runtime/AggregateRuntime.js"
import { EventLog } from "../runtime/EventLog.js"
import { EventEnvelope } from "../contracts/EventEnvelope.js"
import { type Revision, InitialRevision, ConcurrencyError } from "../domain/Revision.js"
import { layerMemory as EventLogMemory } from "../runtime/EventJournalEventLog.js"
import { layerMemory as SnapshotStoreMemory } from "../runtime/SnapshotStore.js"
import * as InMemoryKafkaPublisher from "./InMemoryKafkaPublisher.js"
import * as TestClock from "./TestClock.js"
import * as DeterministicIdGenerator from "./DeterministicIdGenerator.js"

/**
 * @since 1.0.0
 * @category models
 */
export interface AggregateTestHarness<
  State,
  Command,
  Event,
  Err
> {
  readonly given: (
    aggregateId: EntityId,
    events: ReadonlyArray<Event>
  ) => Effect.Effect<void>

  readonly when: (
    aggregateId: EntityId,
    command: Command
  ) => Effect.Effect<ReadonlyArray<Event>, Err>

  readonly then: (
    aggregateId: EntityId
  ) => Effect.Effect<State>

  readonly thenError: (
    aggregateId: EntityId,
    command: Command
  ) => Effect.Effect<Err>

  readonly published: Effect.Effect<
    ReadonlyArray<import("../runtime/KafkaPublisher.js").KafkaProducerRecord>
  >

  readonly layer: Layer.Layer<
    import("../runtime/EventLog.js").EventLog |
    import("../runtime/SnapshotStore.js").SnapshotStore |
    import("../runtime/KafkaPublisher.js").KafkaPublisher |
    import("../runtime/Clock.js").N2Clock |
    import("../runtime/IdGenerator.js").IdGenerator
  >
}

/**
 * Creates a test harness for an aggregate definition.
 * Automatically provides all in-memory layers.
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
  definition: AggregateDefinition<Name, State, Command, Event, Err, R>
): AggregateTestHarness<State, Command, Event, Err> => {
  const runtime = AggregateRuntime.make(definition)

  const testLayer = Layer.mergeAll(
    EventLogMemory,
    SnapshotStoreMemory,
    InMemoryKafkaPublisher.layerSimple,
    TestClock.layer(),
    DeterministicIdGenerator.layer()
  )

  // Helper to run effects with the test layer, erasing the R requirement
  const run = <A, E>(effect: Effect.Effect<A, E, unknown>): Effect.Effect<A, E> =>
    // @ts-expect-error -- test harness erases R via testLayer
    Effect.provide(effect, testLayer)

  const given = (
    aggregateId: EntityId,
    events: ReadonlyArray<Event>
  ): Effect.Effect<void> =>
    run(
      Effect.gen(function*() {
        const eventLog = yield* EventLog
        const streamId = `${definition.name}-${aggregateId}`
        const now = DateTime.unsafeMake(0)

        let revision = 0
        const envelopes: Array<EventEnvelope> = events.map((event, i) => {
          revision = i + 1
          return new EventEnvelope({
            eventId: `seed-${i}`,
            streamId,
            aggregateId,
            aggregateType: Schema.decodeSync(EntityType.EntityType)(definition.name),
            revision,
            occurredAt: now,
            payload: event
          })
        })

        if (envelopes.length > 0) {
          yield* eventLog.append(
            streamId,
            envelopes,
            InitialRevision
          ).pipe(Effect.catchAll(() => Effect.void))
        }
      })
    )

  const when = (
    aggregateId: EntityId,
    command: Command
  ): Effect.Effect<ReadonlyArray<Event>, Err> =>
    run(
      runtime.handle(aggregateId, command).pipe(
        Effect.map((r) => r.events),
        Effect.catchTag("ConcurrencyError", (e) => Effect.die(e))
      )
    )

  const then_ = (
    aggregateId: EntityId
  ): Effect.Effect<State> =>
    run(runtime.hydrate(aggregateId).pipe(Effect.map((r) => r.state)))

  const thenError = (
    aggregateId: EntityId,
    command: Command
  ): Effect.Effect<Err> =>
    run(
      runtime.handle(aggregateId, command).pipe(
        Effect.matchEffect({
          onSuccess: () => Effect.die("Expected error but command succeeded"),
          onFailure: (err) =>
            err instanceof ConcurrencyError
              ? Effect.die(err)
              : Effect.succeed(err as Err)
        })
      )
    )

  const published = run(
    InMemoryKafkaPublisher.make.pipe(
      Effect.flatMap((svc) => svc.published)
    )
  )

  return {
    given,
    when,
    then: then_,
    thenError,
    published,
    layer: testLayer
  }
}
