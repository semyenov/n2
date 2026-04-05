/**
 * @since 1.0.0
 * @module N2.Aggregate
 *
 * Define an aggregate in one pass: state + evolve + decide → handleCommand.
 */
import * as Effect from "effect/Effect"

/**
 * Creates a handleCommand function from decide + evolve.
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

/**
 * Define a complete aggregate in one pass.
 *
 * Takes initial state, then evolve handlers + decide handlers.
 * Returns { evolve, decide, handleCommand, initialState }.
 *
 * @example
 * ```ts
 * import * as C from "./contracts.js"
 *
 * const Order = N2.Aggregate.define<C.OrderEvent, C.OrderCommand>()(C.initialOrderState, {
 *   evolve: {
 *     OrderCreated: (state, e) => ({ ...state, status: "draft" }),
 *     ItemAdded: (state, e) => ({ ...state, ... }),
 *     OrderSubmitted: (state) => ({ ...state, status: "submitted" }),
 *     OrderCancelled: (state) => ({ ...state, status: "cancelled" }),
 *   },
 *   decide: {
 *     CreateOrder: (state, cmd) => Effect.gen(function*() { ... }),
 *     AddItem: (state, cmd) => Effect.gen(function*() { ... }),
 *   }
 * })
 *
 * export const { evolve, decide, handleCommand } = Order
 * ```
 *
 * @since 1.0.0
 */
/**
 * Run a sequence of commands, threading state through each step.
 * Returns the result of the last command.
 *
 * @since 1.0.0
 */
export const runCommands = <State, Command, Event, Err, R>(
  handleCommand: (
    state: State,
    command: Command
  ) => Effect.Effect<{ readonly events: ReadonlyArray<Event>; readonly state: State }, Err, R>,
  initialState: State,
  commands: ReadonlyArray<Command>
): Effect.Effect<{ readonly events: ReadonlyArray<Event>; readonly state: State }, Err, R> =>
  Effect.gen(function*() {
    let current = { events: [] as unknown as ReadonlyArray<Event>, state: initialState }
    for (const command of commands) {
      current = yield* handleCommand(current.state, command)
    }
    return current
  })

export const define = <
  Event extends { readonly _tag: string },
  Command extends { readonly _tag: string }
>() => <State, Err, R>(
  initialState: State,
  config: {
    readonly evolve: {
      readonly [K in Event["_tag"]]: (
        state: State,
        event: Extract<Event, { readonly _tag: K }>
      ) => { readonly [P in keyof State]: State[P] }
    }
    readonly decide: {
      readonly [K in Command["_tag"]]: (
        state: State,
        cmd: Extract<Command, { readonly _tag: K }>
      ) => Effect.Effect<ReadonlyArray<Event>, Err, R>
    }
  }
) => {
  const Ctor = (initialState as object).constructor as new (fields: { readonly [P in keyof State]: State[P] }) => State

  const evolve = (state: State, event: Event): State => {
    const h = config.evolve as unknown as Record<string, (state: State, event: Event) => { readonly [P in keyof State]: State[P] }>
    return new Ctor(h[event._tag]!(state, event))
  }

  const decide = (state: State, command: Command): Effect.Effect<ReadonlyArray<Event>, Err, R> => {
    const h = config.decide as unknown as Record<string, (state: State, cmd: Command) => Effect.Effect<ReadonlyArray<Event>, Err, R>>
    return h[command._tag]!(state, command)
  }

  return {
    initialState,
    evolve,
    decide,
    handleCommand: makeHandleCommand(decide, evolve)
  }
}
