/**
 * Framework-level unit tests for N2.define().
 *
 * Tests the core handle/run/evolve/decide logic with a minimal aggregate.
 */
import { it, expect } from "@effect/vitest"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as TestClock from "effect/TestClock"
import { RpcTest } from "@effect/rpc"
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

class DelayedIncremented extends Schema.TaggedClass<DelayedIncremented>()(
  "DelayedIncremented",
  { amount: Schema.Number }
) {}

const DelayedEvents = defineEvents(DelayedIncremented)
type DelayedEvent = typeof DelayedEvents.schema.Type

class DelayedIncrement extends Schema.TaggedRequest<DelayedIncrement>("DelayedIncrement")(
  "DelayedIncrement",
  {
    failure: CounterError,
    success: Schema.Number,
    payload: {
      counterId: Schema.String,
      amount: Schema.Number
    }
  }
) {}

class GetDelayedCount extends Schema.TaggedRequest<GetDelayedCount>("GetDelayedCount")(
  "GetDelayedCount",
  {
    failure: CounterError,
    success: Schema.Number,
    payload: {
      counterId: Schema.String
    }
  }
) {}

const DelayedCommands = defineCommands(DelayedIncrement, GetDelayedCount)
type DelayedCommand = typeof DelayedCommands.schema.Type
const DelayedEntity = DelayedCommands.toEntity("DelayedCounter", (command) => command.counterId)

it.effect("handle produces events and applies evolve", () => Effect.gen(function* () {
  const { state, events } = yield* Counter.handle({ count: 0 }, new Increment({ amount: 5 }))
  expect(state.count).toBe(5)
  expect(events).toHaveLength(1)
  expect(events[0]?._tag).toBe("Incremented")
}))

it.effect("handle accumulates state across events", () => Effect.gen(function* () {
  const { state: s1 } = yield* Counter.handle({ count: 0 }, new Increment({ amount: 3 }))
  const { state: s2 } = yield* Counter.handle(s1, new Increment({ amount: 7 }))
  expect(s2.count).toBe(10)
}))

it.effect("handle returns error for invalid command", () => Effect.gen(function* () {
  const err = yield* Counter.handle({ count: 0 }, new Increment({ amount: -1 })).pipe(Effect.flip)
  expect(err._tag).toBe("CounterError")
  expect(err.message).toContain("positive")
}))

it.effect("handle returns empty events for no-op command", () => Effect.gen(function* () {
  const { state, events } = yield* Counter.handle({ count: 0 }, new Reset({}))
  expect(events).toHaveLength(0)
  expect(state.count).toBe(0)
}))

it.effect("run processes multiple commands sequentially", () => Effect.gen(function* () {
  const { state, events } = yield* Counter.run([
    new Increment({ amount: 1 }),
    new Increment({ amount: 2 }),
    new Increment({ amount: 3 })
  ])
  expect(state.count).toBe(6)
  expect(events).toHaveLength(3)
}))

it.effect("run stops on first error", () => Effect.gen(function* () {
  const err = yield* Counter.run([
    new Increment({ amount: 1 }),
    new Increment({ amount: -1 }),
    new Increment({ amount: 3 })
  ]).pipe(Effect.flip)
  expect(err._tag).toBe("CounterError")
}))

it("evolve is a pure function", () => {
  const state = Counter.evolve({ count: 10 }, new Incremented({ value: 5 }))
  expect(state.count).toBe(15)

  const resetState = Counter.evolve({ count: 10 }, new WasReset({ previousValue: 10 }))
  expect(resetState.count).toBe(0)
})

it.effect("decide returns events without applying them", () => Effect.gen(function* () {
  const events = yield* Counter.decide({ count: 5 }, new Increment({ amount: 3 }))
  expect(events).toHaveLength(1)
  expect(events[0]?._tag).toBe("Incremented")
}))

it("initialState is accessible", () => {
  expect(Counter.initialState.count).toBe(0)
})

it("defineEvents derives an EventGroup with payload schemas", () => {
  const group = TestEvents.toEventGroup(() => "counter")

  expect(Object.keys(group.events).sort()).toEqual(["Incremented", "WasReset"])
})

it.effect("stateful RPC handlers serialize the same entity without serializing every entity", () =>
  Effect.scoped(Effect.gen(function* () {
    const activeDecisions = yield* Ref.make(0)
    const maxActiveDecisions = yield* Ref.make(0)
    const releaseDecisions = yield* Ref.make<Deferred.Deferred<void> | undefined>(undefined)
    const firstEntered = yield* Ref.make<Deferred.Deferred<void> | undefined>(undefined)
    const secondEntered = yield* Ref.make<Deferred.Deferred<void> | undefined>(undefined)

    const signal = (deferred: Deferred.Deferred<void> | undefined) =>
      deferred === undefined
        ? Effect.void
        : Deferred.succeed(deferred, undefined).pipe(Effect.asVoid)

    const makeBarrier = Effect.gen(function* () {
      const release = yield* Deferred.make<void>()
      const first = yield* Deferred.make<void>()
      const second = yield* Deferred.make<void>()
      yield* Ref.set(releaseDecisions, release)
      yield* Ref.set(firstEntered, first)
      yield* Ref.set(secondEntered, second)
      return { release, first, second }
    })

    const awaitOrFail = (deferred: Deferred.Deferred<void>, message: string) =>
      Effect.gen(function* () {
        const waiter = yield* Deferred.await(deferred).pipe(
          Effect.timeoutFail({
            duration: "1 second",
            onTimeout: () => new Error(message)
          }),
          Effect.fork
        )
        yield* TestClock.adjust("1 second")
        yield* Fiber.join(waiter)
      })

    const trackDecision = Effect.gen(function* () {
      const release = yield* Ref.get(releaseDecisions)
      const active = yield* Ref.updateAndGet(activeDecisions, (n) => n + 1)
      yield* Ref.update(maxActiveDecisions, (max) => Math.max(max, active))
      yield* signal(yield* Ref.get(active === 1 ? firstEntered : secondEntered))
      yield* (release === undefined ? Effect.void : Deferred.await(release)).pipe(
        Effect.ensuring(Ref.update(activeDecisions, (n) => n - 1))
      )
    })

    const DelayedCounter = define<DelayedEvent, DelayedCommand>()({
      initialState: { count: 0 } as CounterState,
      commands: DelayedCommands.constructors,
      evolve: {
        DelayedIncremented: (state, event) => ({ count: state.count + event.amount })
      },
      decide: {
        DelayedIncrement: (_state, command) =>
          trackDecision.pipe(
            Effect.as([new DelayedIncremented({ amount: command.amount })])
          ),
        GetDelayedCount: () => Effect.succeed([])
      }
    })

    const handlers = DelayedCounter.toStatefulRpcHandlers(
      DelayedEntity.protocol,
      {
        entityId: (command) => command.counterId,
        toResult: ({ state }) => state.count,
        toError: (error) =>
          error instanceof CounterError
            ? error
            : new CounterError({ message: String(error) }),
        overrides: {
          GetDelayedCount: (command, ctx) =>
            ctx.getState(command.counterId).pipe(
              Effect.map((state) => state.count)
            )
        }
      }
    )

    yield* Effect.gen(function* () {
      const client = yield* RpcTest.makeClient(DelayedEntity.protocol)

      const differentEntities = yield* makeBarrier
      const differentFiber = yield* Effect.all([
        client.DelayedIncrement({ counterId: "counter-a", amount: 1 }),
        client.DelayedIncrement({ counterId: "counter-b", amount: 1 })
      ], { concurrency: "unbounded", discard: true }).pipe(Effect.fork)
      yield* awaitOrFail(
        differentEntities.second,
        "different entity commands did not enter decisions concurrently"
      )
      expect(yield* Ref.get(maxActiveDecisions)).toBe(2)
      yield* Deferred.succeed(differentEntities.release, undefined)
      yield* Fiber.join(differentFiber)

      yield* Ref.set(activeDecisions, 0)
      yield* Ref.set(maxActiveDecisions, 0)
      const sameEntity = yield* makeBarrier
      const sameFiber = yield* Effect.all([
        client.DelayedIncrement({ counterId: "counter-c", amount: 1 }),
        client.DelayedIncrement({ counterId: "counter-c", amount: 1 })
      ], { concurrency: "unbounded", discard: true }).pipe(Effect.fork)
      yield* awaitOrFail(
        sameEntity.first,
        "same entity command did not enter the first decision"
      )
      const readWhileWriting = yield* client.GetDelayedCount({ counterId: "counter-c" }).pipe(Effect.fork)
      yield* Effect.yieldNow()
      yield* Effect.yieldNow()
      expect(yield* Deferred.isDone(sameEntity.second)).toBe(false)
      expect(Option.isNone(yield* Fiber.poll(readWhileWriting))).toBe(true)
      yield* Deferred.succeed(sameEntity.release, undefined)
      yield* Fiber.join(sameFiber)
      yield* Fiber.join(readWhileWriting)
      expect(yield* Ref.get(maxActiveDecisions)).toBe(1)

      const count = yield* client.GetDelayedCount({ counterId: "counter-c" })
      expect(count).toBe(2)
    }).pipe(Effect.provide(handlers))
  })))

it.effect("stateful RPC handlers evict least-recently-used inactive entries", () =>
  Effect.scoped(Effect.gen(function* () {
    const CacheCounter = define<DelayedEvent, DelayedCommand>()({
      initialState: { count: 0 } as CounterState,
      commands: DelayedCommands.constructors,
      evolve: {
        DelayedIncremented: (state, event) => ({ count: state.count + event.amount })
      },
      decide: {
        DelayedIncrement: (_state, command) =>
          Effect.succeed([new DelayedIncremented({ amount: command.amount })]),
        GetDelayedCount: () => Effect.succeed([])
      }
    })

    const handlers = CacheCounter.toStatefulRpcHandlers(
      DelayedEntity.protocol,
      {
        entityId: (command) => command.counterId,
        toResult: ({ state }) => state.count,
        toError: (error) =>
          error instanceof CounterError
            ? error
            : new CounterError({ message: String(error) }),
        maxEntries: 1,
        overrides: {
          GetDelayedCount: (command, ctx) =>
            ctx.getState(command.counterId).pipe(
              Effect.map((state) => state.count)
            )
        }
      }
    )

    yield* Effect.gen(function* () {
      const client = yield* RpcTest.makeClient(DelayedEntity.protocol)

      expect(yield* client.DelayedIncrement({ counterId: "counter-a", amount: 1 })).toBe(1)
      expect(yield* client.DelayedIncrement({ counterId: "counter-b", amount: 10 })).toBe(10)
      expect(yield* client.GetDelayedCount({ counterId: "counter-b" })).toBe(10)
      expect(yield* client.GetDelayedCount({ counterId: "counter-a" })).toBe(0)
    }).pipe(Effect.provide(handlers))
  })))

it.effect("stateful RPC handlers fall back to initial state on snapshot load failure by default", () =>
  Effect.scoped(Effect.gen(function* () {
    const SnapshotCounter = define<DelayedEvent, DelayedCommand>()({
      initialState: { count: 0 } as CounterState,
      commands: DelayedCommands.constructors,
      evolve: {
        DelayedIncremented: (state, event) => ({ count: state.count + event.amount })
      },
      decide: {
        DelayedIncrement: (_state, command) =>
          Effect.succeed([new DelayedIncremented({ amount: command.amount })]),
        GetDelayedCount: () => Effect.succeed([])
      }
    })

    const handlers = SnapshotCounter.toStatefulRpcHandlers(
      DelayedEntity.protocol,
      {
        entityId: (command) => command.counterId,
        toResult: ({ state }) => state.count,
        toError: (error) =>
          error instanceof CounterError
            ? error
            : new CounterError({ message: String(error) }),
        snapshots: {
          load: () => Effect.fail("snapshot unavailable"),
          save: () => Effect.void,
          every: 10
        }
      }
    )

    yield* Effect.gen(function* () {
      const client = yield* RpcTest.makeClient(DelayedEntity.protocol)
      expect(yield* client.DelayedIncrement({ counterId: "counter-a", amount: 2 })).toBe(2)
    }).pipe(Effect.provide(handlers))
  })))

it.effect("stateful RPC handlers can fail commands on snapshot load failure", () =>
  Effect.scoped(Effect.gen(function* () {
    const SnapshotCounter = define<DelayedEvent, DelayedCommand>()({
      initialState: { count: 0 } as CounterState,
      commands: DelayedCommands.constructors,
      evolve: {
        DelayedIncremented: (state, event) => ({ count: state.count + event.amount })
      },
      decide: {
        DelayedIncrement: (_state, command) =>
          Effect.succeed([new DelayedIncremented({ amount: command.amount })]),
        GetDelayedCount: () => Effect.succeed([])
      }
    })

    const handlers = SnapshotCounter.toStatefulRpcHandlers(
      DelayedEntity.protocol,
      {
        entityId: (command) => command.counterId,
        toResult: ({ state }) => state.count,
        toError: (error) =>
          error instanceof CounterError
            ? error
            : new CounterError({ message: String(error) }),
        snapshotLoadFailure: "fail",
        snapshots: {
          load: () => Effect.fail("snapshot unavailable"),
          save: () => Effect.void,
          every: 10
        }
      }
    )

    yield* Effect.gen(function* () {
      const client = yield* RpcTest.makeClient(DelayedEntity.protocol)
      const error = yield* client.DelayedIncrement({ counterId: "counter-a", amount: 2 }).pipe(Effect.flip)
      expect(error._tag).toBe("CounterError")
      expect(error.message).toContain("snapshot unavailable")
    }).pipe(Effect.provide(handlers))
  })))

it("stateful RPC handlers validate cache and snapshot options", () => {
  expect(() =>
    Counter.toStatefulRpcHandlers(TestCommands.toEntity("InvalidMaxEntries", () => "counter").protocol, {
      entityId: () => "counter",
      toResult: ({ state }) => state.count,
      toError: (error) =>
        error instanceof CounterError
          ? error
          : new CounterError({ message: String(error) }),
      maxEntries: 0
    })
  ).toThrow("maxEntries must be a positive integer")

  expect(() =>
    Counter.toStatefulRpcHandlers(TestCommands.toEntity("InvalidSnapshotCadence", () => "counter").protocol, {
      entityId: () => "counter",
      toResult: ({ state }) => state.count,
      toError: (error) =>
        error instanceof CounterError
          ? error
          : new CounterError({ message: String(error) }),
      snapshots: {
        load: () => Effect.succeed(Option.none()),
        save: () => Effect.void,
        every: 0
      }
    })
  ).toThrow("snapshots.every must be a positive integer")
})
