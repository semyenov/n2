/**
 * Framework-level unit tests for N2.define().
 *
 * Tests the core handle/run/evolve/decide logic with a minimal aggregate.
 */
import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { define } from "./Definition.js"
import { defineCommands, defineEvents } from "./Definitions.js"

// Minimal test aggregate: counter with Increment and Reset commands

class Incremented extends Schema.TaggedClass<Incremented>()(
  "Incremented",
  { value: Schema.Number }
) {}

class WasReset extends Schema.TaggedClass<WasReset>()(
  "WasReset",
  { previousValue: Schema.Number }
) {}

const TestEvents = defineEvents(Incremented, WasReset)
type TestEvent = typeof TestEvents.schema.Type

class CounterError extends Schema.TaggedError<CounterError>()(
  "CounterError",
  { message: Schema.String }
) {}

class Increment extends Schema.TaggedRequest<Increment>("Increment")(
  "Increment",
  { failure: CounterError, success: Schema.Number, payload: { amount: Schema.Number } }
) {}

class Reset extends Schema.TaggedRequest<Reset>("Reset")(
  "Reset",
  { failure: CounterError, success: Schema.Number, payload: {} }
) {}

class GetCount extends Schema.TaggedRequest<GetCount>("GetCount")(
  "GetCount",
  { failure: CounterError, success: Schema.Number, payload: {} }
) {}

const TestCommands = defineCommands(Increment, Reset, GetCount)
type TestCommand = typeof TestCommands.schema.Type

interface CounterState {
  readonly count: number
}

const Counter = define<TestEvent, TestCommand>()({
  initialState: { count: 0 } as CounterState,
  commands: TestCommands.constructors,
  evolve: {
    Incremented: (state, event) => ({ count: state.count + event.value }),
    WasReset: (_state, _event) => ({ count: 0 })
  },
  decide: {
    Increment: (state, command) =>
      command.amount <= 0
        ? Effect.fail(new CounterError({ message: "Amount must be positive" }))
        : Effect.succeed([new Incremented({ value: command.amount })]),
    Reset: (state) =>
      state.count === 0
        ? Effect.succeed([])
        : Effect.succeed([new WasReset({ previousValue: state.count })]),
    GetCount: () => Effect.succeed([])
  }
})

const run = <A, E>(effect: Effect.Effect<A, E, never>): Promise<A> =>
  Effect.runPromise(effect)

test("handle produces events and applies evolve", async () => {
  const { state, events } = await run(Counter.handle({ count: 0 }, new Increment({ amount: 5 })))
  expect(state.count).toBe(5)
  expect(events).toHaveLength(1)
  expect(events[0]?._tag).toBe("Incremented")
})

test("handle accumulates state across events", async () => {
  const { state: s1 } = await run(Counter.handle({ count: 0 }, new Increment({ amount: 3 })))
  const { state: s2 } = await run(Counter.handle(s1, new Increment({ amount: 7 })))
  expect(s2.count).toBe(10)
})

test("handle returns error for invalid command", async () => {
  const err = await run(Counter.handle({ count: 0 }, new Increment({ amount: -1 })).pipe(Effect.flip))
  expect(err._tag).toBe("CounterError")
  expect(err.message).toContain("positive")
})

test("handle returns empty events for no-op command", async () => {
  const { state, events } = await run(Counter.handle({ count: 0 }, new Reset({})))
  expect(events).toHaveLength(0)
  expect(state.count).toBe(0)
})

test("run processes multiple commands sequentially", async () => {
  const { state, events } = await run(Counter.run([
    new Increment({ amount: 1 }),
    new Increment({ amount: 2 }),
    new Increment({ amount: 3 })
  ]))
  expect(state.count).toBe(6)
  expect(events).toHaveLength(3)
})

test("run stops on first error", async () => {
  const err = await run(Counter.run([
    new Increment({ amount: 1 }),
    new Increment({ amount: -1 }),
    new Increment({ amount: 3 })
  ]).pipe(Effect.flip))
  expect(err._tag).toBe("CounterError")
})

test("evolve is a pure function", () => {
  const state = Counter.evolve({ count: 10 }, new Incremented({ value: 5 }))
  expect(state.count).toBe(15)

  const resetState = Counter.evolve({ count: 10 }, new WasReset({ previousValue: 10 }))
  expect(resetState.count).toBe(0)
})

test("decide returns events without applying them", async () => {
  const events = await run(Counter.decide({ count: 5 }, new Increment({ amount: 3 })))
  expect(events).toHaveLength(1)
  expect(events[0]?._tag).toBe("Incremented")
})

test("initialState is accessible", () => {
  expect(Counter.initialState.count).toBe(0)
})
