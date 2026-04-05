/**
 * @since 1.0.0
 * @module N2.Aggregate
 *
 * Helper for creating handleCommand from decide + evolve.
 */
import * as Effect from "effect/Effect"

/**
 * Creates a handleCommand function from decide + evolve.
 * Eliminates the repeated pattern of calling decide, then folding evolve.
 *
 * @example
 * ```ts
 * export const handleCommand = N2.Aggregate.makeHandleCommand(decide, evolve)
 * ```
 *
 * @since 1.0.0
 */
export const makeHandleCommand = <State, Command, Event, Err, R>(
  decide: (state: State, command: Command) => Effect.Effect<ReadonlyArray<Event>, Err, R>,
  evolve: (state: State, event: Event) => State
) =>
  (state: State, command: Command): Effect.Effect<
    { readonly events: ReadonlyArray<Event>; readonly state: State },
    Err,
    R
  > =>
    Effect.gen(function*() {
      const events = yield* decide(state, command)
      let newState = state
      for (const event of events) newState = evolve(newState, event)
      return { events, state: newState }
    })
