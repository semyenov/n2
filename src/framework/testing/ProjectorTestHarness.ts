/**
 * @since 1.0.0
 * @module ProjectorTestHarness
 *
 * Test harness for projectors. Feeds event envelopes and asserts state.
 */
import * as Effect from "effect/Effect"
import type { EventEnvelope } from "../contracts/EventEnvelope.js"
import type { ProjectorDefinition } from "../projection/ProjectorDefinition.js"

/**
 * @since 1.0.0
 * @category models
 */
export interface ProjectorTestHarness<State> {
  readonly given: (
    envelopes: ReadonlyArray<EventEnvelope>
  ) => Effect.Effect<State>

  readonly givenSingle: (
    envelope: EventEnvelope
  ) => Effect.Effect<State>
}

/**
 * Creates a test harness for a projector definition.
 *
 * @since 1.0.0
 * @category constructors
 */
export const make = <State>(
  projector: ProjectorDefinition<State>
): ProjectorTestHarness<State> => {
  const given = (
    envelopes: ReadonlyArray<EventEnvelope>
  ): Effect.Effect<State> =>
    Effect.gen(function*() {
      let state = projector.initialState
      for (const envelope of envelopes) {
        state = yield* projector.handle(state, envelope)
      }
      return state
    })

  const givenSingle = (
    envelope: EventEnvelope
  ): Effect.Effect<State> => given([envelope])

  return { given, givenSingle }
}
