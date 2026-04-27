/**
 * @since 1.0.0
 * @module N2
 *
 * Definition-centric helpers for aggregate, RPC, and entity wiring.
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Metric from "effect/Metric"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as SynchronizedRef from "effect/SynchronizedRef"
import type { DurationInput } from "effect/Duration"
import { HttpLayerRouter } from "@effect/platform"
import { Entity, type Sharding } from "@effect/cluster"
import type { Rpc, RpcGroup } from "@effect/rpc"
import { RpcSerialization, RpcServer } from "@effect/rpc"

type Tagged = { readonly _tag: string }

type TagOf<A extends Tagged> = Extract<A["_tag"], string>

type CommandConstructors<Command extends Tagged> = {
  readonly [K in TagOf<Command>]:
  new (...args: ReadonlyArray<never>) => Extract<Command, { readonly _tag: K }>
}

type EvolveHandlers<State, Event extends Tagged> = {
  readonly [K in TagOf<Event>]:
  (state: State, event: Extract<Event, { readonly _tag: K }>) => State
}

type DecideHandlers<State, Command extends Tagged, Event extends Tagged, Err, R> = {
  readonly [K in TagOf<Command>]:
  (
    state: State,
    command: Extract<Command, { readonly _tag: K }>
  ) => Effect.Effect<ReadonlyArray<Event>, Err, R>
}

/**
 * Looser constraint for capturing `Handlers` precisely — Err/R use `unknown`
 * (not `any`) to break the circular inference that would occur if they were
 * explicit type params in the same inference site as `Handlers`.
 * `Effect` is covariant in all three positions, so every concrete handler
 * satisfies this constraint.
 */
type DecideHandlersBase<State, Command extends Tagged, Event extends Tagged> = {
  readonly [K in TagOf<Command>]:
  (
    state: State,
    command: Extract<Command, { readonly _tag: K }>
  ) => Effect.Effect<ReadonlyArray<Event>, unknown, unknown>
}

/** Union of all error types across every decide handler. */
type HandlersErr<State, Command extends Tagged, H extends DecideHandlersBase<State, Command, Tagged>> = {
  [K in TagOf<Command>]: H[K] extends (
    state: State,
    command: Extract<Command, { _tag: K }>
  ) => Effect.Effect<ReadonlyArray<infer _A>, infer E, infer _R> ? E : never
}[TagOf<Command>]

/** Union of all requirement types across every decide handler. */
type HandlersR<State, Command extends Tagged, H extends DecideHandlersBase<State, Command, Tagged>> = {
  [K in TagOf<Command>]: H[K] extends (
    state: State,
    command: Extract<Command, { _tag: K }>
  ) => Effect.Effect<ReadonlyArray<infer _A>, infer _E, infer R> ? R : never
}[TagOf<Command>]

/**
 * Extracts the specific event element type emitted by the handler for command `Cmd`.
 * The bounded `infer E extends Event` makes `DecideEventsFor<Cmd, H, Event> extends Event`
 * known to TypeScript, eliminating downstream casts to `ReadonlyArray<Event>`.
 */
type DecideEventsFor<Cmd extends Tagged, Handlers, Event extends Tagged> =
  TagOf<Cmd> extends keyof Handlers
    ? Handlers[TagOf<Cmd>] extends (...args: infer _Args) => Effect.Effect<ReadonlyArray<infer E extends Event>, infer _Err, infer _R>
      ? E
      : Event
    : Event

/** Extract the `R` (requirements) type from a function returning an Effect. */
type ExtractEffectR<T> = T extends (...args: ReadonlyArray<unknown>) => Effect.Effect<unknown, unknown, infer R> ? R : never

/** Extract all requirements from an adapter's lifecycle hooks (snapshots, afterCommit, overrides). */
type AdapterR<A> =
  | (A extends { readonly snapshots: { readonly load: infer L } } ? ExtractEffectR<L> : never)
  | (A extends { readonly snapshots: { readonly save: infer S } } ? ExtractEffectR<S> : never)
  | (A extends { readonly afterCommit: infer AC } ? ExtractEffectR<AC> : never)
  | (A extends { readonly overrides: { readonly [k: string]: infer O } } ? ExtractEffectR<O> : never)

type OverrideContext<State, HooksR> = {
  readonly entityId: string
  readonly getState: (entityId: string) => Effect.Effect<State, unknown, HooksR>
}

type OverrideHandlers<State, Command extends Tagged, HooksR> = {
  readonly [K in TagOf<Command>]?: (
    command: Extract<Command, { readonly _tag: K }>,
    ctx: OverrideContext<State, HooksR>
  ) => Effect.Effect<unknown, unknown, HooksR>
}

const commandTags = <Command extends Tagged>(
  commands: CommandConstructors<Command>
): ReadonlyArray<TagOf<Command>> =>
  Object.keys(commands).filter((tag): tag is TagOf<Command> => tag in commands)

const instantiateCommand = <CurrentCommand extends Tagged>(
  Command: new (...args: ReadonlyArray<never>) => CurrentCommand,
  payload: unknown
): CurrentCommand =>
  Reflect.construct(Command, [payload]) as CurrentCommand

const runOverride = <State, Command extends Tagged, HooksR>(
  override: unknown,
  command: Command,
  ctx: OverrideContext<State, HooksR>
): Effect.Effect<unknown, unknown, HooksR> => {
  const invoke = override as (
    command: Command,
    ctx: OverrideContext<State, HooksR>
  ) => Effect.Effect<unknown, unknown, HooksR>
  return invoke(command, ctx)
}

const dispatchHandler = <State, Cmd extends Tagged, Handlers, Event extends Tagged, Err, R>(
  handlers: Handlers,
  command: Cmd
): (state: State, command: Cmd) => Effect.Effect<ReadonlyArray<DecideEventsFor<Cmd, Handlers, Event>>, Err, R> => {
  // TypeScript cannot correlate a runtime `_tag` lookup with the matching
  // mapped handler parameter, so aggregate dispatch keeps this cast isolated.
  const byTag = handlers as Record<string, unknown>
  return byTag[command._tag] as (
    state: State,
    command: Cmd
  ) => Effect.Effect<ReadonlyArray<DecideEventsFor<Cmd, Handlers, Event>>, Err, R>
}

const makeRoute = <Rpcs extends Rpc.Any, R>(
  group: RpcGroup.RpcGroup<Rpcs>,
  path: `/${string}`,
  handlers: Layer.Layer<Rpc.ToHandler<Rpcs>, never, R>
) =>
  RpcServer
    .layerHttpRouter({ group, path, protocol: "http" })
    .pipe(
      Layer.provide(handlers),
      Layer.provide(RpcSerialization.layerJsonRpc())
    )

/** Options for mapping execution results when wiring to a cluster Entity. */
export interface EntityAdapterOptions<State, Command extends Tagged, Event, Result, MappedErr, HooksR = unknown> {
  readonly toResult: (ctx: {
    readonly entityId: string
    readonly revision: number
    readonly command: Command
    readonly events: ReadonlyArray<Event>
    readonly state: State
  }) => Result
  readonly toError: (error: unknown) => MappedErr
  /** Runs before dispatch/override for every command. Failure rejects the command.
   *  Use for auth, rate limiting, logging, or command validation. */
  readonly middleware?: (ctx: {
    readonly entityId: string
    readonly command: Command
  }) => Effect.Effect<void, MappedErr, HooksR>
  /** Load/save snapshots. Snapshot loading restores state on entity init. */
  readonly snapshots?: {
    readonly load: (entityId: string) => Effect.Effect<Option.Option<{ readonly state: State; readonly revision: number }>, unknown, HooksR>
    readonly save: (entityId: string, state: State, revision: number) => Effect.Effect<void, unknown, HooksR>
    readonly every: number
  }
  /** Transform state after handle() succeeds (pure, sync). Use for non-event-sourced side effects. */
  readonly postHandle?: (ctx: {
    readonly entityId: string
    readonly command: Command
    readonly events: ReadonlyArray<Event>
    readonly state: State
  }) => State
  /** Fire-and-forget side effect after state is committed. Use for event publishing. */
  readonly afterCommit?: (ctx: {
    readonly entityId: string
    readonly revision: number
    readonly command: Command
    readonly events: ReadonlyArray<Event>
    readonly state: State
  }) => Effect.Effect<void, unknown, HooksR>
  /** Timeout for afterCommit hook. Default: 30 seconds. */
  readonly afterCommitTimeout?: DurationInput
  /** Override specific command handlers (e.g. read queries). Bypasses dispatch entirely.
   *  Each key narrows the command to the specific tagged member for that tag.
   *  `getState` takes an entityId for API compatibility with stateful RPC overrides. */
  readonly overrides?: OverrideHandlers<State, Command, HooksR>
}

/** Options for stateful multi-entity RPC handlers (dev/test mode). */
export interface StatefulRpcAdapterOptions<State, Command extends Tagged, Event, Result, MappedErr, HooksR = unknown> {
  /** Extract entity ID from a command. */
  readonly entityId: (command: Command) => string
  readonly toResult: (ctx: {
    readonly entityId: string
    readonly revision: number
    readonly command: Command
    readonly events: ReadonlyArray<Event>
    readonly state: State
  }) => Result
  readonly toError: (error: unknown) => MappedErr
  /** Runs before dispatch/override for every command. Failure rejects the command.
   *  Use for auth, rate limiting, logging, or command validation. */
  readonly middleware?: (ctx: {
    readonly entityId: string
    readonly command: Command
  }) => Effect.Effect<void, MappedErr, HooksR>
  readonly snapshots?: {
    readonly load: (entityId: string) => Effect.Effect<Option.Option<{ readonly state: State; readonly revision: number }>, unknown, HooksR>
    readonly save: (entityId: string, state: State, revision: number) => Effect.Effect<void, unknown, HooksR>
    readonly every: number
  }
  readonly postHandle?: (ctx: {
    readonly entityId: string
    readonly command: Command
    readonly events: ReadonlyArray<Event>
    readonly state: State
  }) => State
  readonly afterCommit?: (ctx: {
    readonly entityId: string
    readonly revision: number
    readonly command: Command
    readonly events: ReadonlyArray<Event>
    readonly state: State
  }) => Effect.Effect<void, unknown, HooksR>
  /** Timeout for afterCommit hook. Default: 30 seconds. */
  readonly afterCommitTimeout?: DurationInput
  /** Auto-instrument with counters and timers: `${prefix}.commands.total`, `${prefix}.commands.errors`, `${prefix}.command.duration_ms`. */
  readonly metrics?: { readonly prefix: string }
  /** Maximum number of entities to keep in the in-memory store. When exceeded, least-recently-accessed entries are evicted. */
  readonly maxEntries?: number
  /** Override specific command handlers (e.g. read queries).
   *  Each key narrows the command to the specific tagged member for that tag. */
  readonly overrides?: OverrideHandlers<State, Command, HooksR>
}

/** Options for mapping execution results when wiring to stateless RPC handlers. */
export interface RpcAdapterOptions<State, Command, Event, Result, MappedErr> {
  readonly toResult: (ctx: {
    readonly command: Command
    readonly events: ReadonlyArray<Event>
    readonly state: State
  }) => Result
  readonly toError: (error: unknown) => MappedErr
}

export interface Definition<
  State,
  Event extends Tagged,
  Command extends Tagged,
  Err,
  R,
  Handlers extends DecideHandlersBase<State, Command, Event> = DecideHandlersBase<State, Command, Event>
> {
  readonly initialState: State
  readonly commands: CommandConstructors<Command>
  readonly evolve: (
    state: State,
    event: Event
  ) => State
  readonly decide: (
    state: State,
    command: Command
  ) => Effect.Effect<ReadonlyArray<Event>, Err, R>
  readonly handle: <Cmd extends Command>(
    state: State,
    command: Cmd
  ) => Effect.Effect<
    { readonly events: ReadonlyArray<DecideEventsFor<Cmd, Handlers, Event>>; readonly state: State },
    Err,
    R
  >
  readonly run: (
    commands: ReadonlyArray<Command>,
    state?: State
  ) => Effect.Effect<
    { readonly events: ReadonlyArray<Event>; readonly state: State },
    Err,
    R
  >
  readonly toEntityLayer: <Type extends string, Rpcs extends Rpc.Any, Result, MappedErr, const Adapter extends EntityAdapterOptions<State, Command, Event, Result, MappedErr>>(
    entity: Entity.Entity<Type, Rpcs>,
    adapter: Adapter,
    layerOptions?: {
      readonly maxIdleTime?: DurationInput
      readonly concurrency?: number | "unbounded"
    }
  ) => Layer.Layer<
    never,
    never,
    R | AdapterR<Adapter> | Rpc.Context<Rpcs> | Rpc.Middleware<Rpcs> | Sharding.Sharding
  >
  readonly toRpcHandlers: <Rpcs extends Rpc.Any, Result, MappedErr>(
    group: RpcGroup.RpcGroup<Rpcs>,
    options: RpcAdapterOptions<State, Command, Event, Result, MappedErr>
  ) => Layer.Layer<Rpc.ToHandler<Rpcs>, never, R>
  readonly toStatefulRpcHandlers: <Rpcs extends Rpc.Any, Result, MappedErr, const Adapter extends StatefulRpcAdapterOptions<State, Command, Event, Result, MappedErr>>(
    group: RpcGroup.RpcGroup<Rpcs>,
    adapter: Adapter
  ) => Layer.Layer<Rpc.ToHandler<Rpcs>, never, R | AdapterR<Adapter>>
  readonly toHttpRoute: <Rpcs extends Rpc.Any>(
    group: RpcGroup.RpcGroup<Rpcs>,
    path: `/${string}`,
    handlers: Layer.Layer<Rpc.ToHandler<Rpcs>, never, R>
  ) => Layer.Layer<never, never, R>
}

/**
 * Defines a domain model once and derives aggregate, RPC, and entity helpers.
 *
 * The double-curry `define<Event, Command>()({...})` enables exhaustive handler
 * checking: TypeScript verifies all events have `evolve` handlers and all
 * commands have `decide` handlers, without requiring `State`/`Err`/`R` to be
 * specified explicitly.
 *
 * @since 1.0.0
 */
export const define = <
  Event extends Tagged,
  Command extends Tagged
>() =>
  <State, const Handlers extends DecideHandlersBase<State, Command, Event>>(options: {
    readonly initialState: State
    readonly commands: CommandConstructors<Command>
    readonly evolve: EvolveHandlers<State, Event>
    readonly decide: Handlers
  }): Definition<State, Event, Command, HandlersErr<State, Command, Handlers>, HandlersR<State, Command, Handlers>, Handlers> => {
    type Err = HandlersErr<State, Command, Handlers>
    type R = HandlersR<State, Command, Handlers>

    const commandsByTag: Readonly<Record<TagOf<Command>, new (...args: ReadonlyArray<never>) => Command>> =
      options.commands

    const evolve: Definition<State, Event, Command, Err, R>["evolve"] = (state, event) => {
      const tag = event._tag as TagOf<Event>
      return (options.evolve as Record<TagOf<Event>, (state: State, event: Event) => State>)[tag](state, event)
    }

    const dispatch = <Cmd extends Command>(state: State, command: Cmd): Effect.Effect<ReadonlyArray<DecideEventsFor<Cmd, Handlers, Event>>, Err, R> => {
      const handler = dispatchHandler<State, Cmd, Handlers, Event, Err, R>(options.decide, command)
      return handler(state, command)
    }

    const decide = (state: State, command: Command): Effect.Effect<ReadonlyArray<Event>, Err, R> =>
      dispatch(state, command)

    // dispatch returns ReadonlyArray<DecideEventsFor<Cmd, Handlers, Event>>,
    // and DecideEventsFor<Cmd, Handlers, Event> extends Event (bounded infer),
    // so evolve(state, event) type-checks without a cast.
    const handle = <Cmd extends Command>(state: State, command: Cmd) =>
      Effect.gen(function* () {
        const events = yield* dispatch(state, command)
        let nextState = state
        for (const event of events) {
          nextState = evolve(nextState, event)
        }
        return { events, state: nextState }
      })

    const run: Definition<State, Event, Command, Err, R>["run"] = (
      commands,
      state = options.initialState
    ) =>
      Effect.gen(function* () {
        let allEvents: Array<Event> = []
        let currentState = state
        for (const command of commands) {
          const result = yield* handle(currentState, command)
          allEvents = allEvents.concat(result.events)
          currentState = result.state
        }
        return { events: allEvents as ReadonlyArray<Event>, state: currentState }
      })

    const toEntityLayer = <
      Type extends string,
      Rpcs extends Rpc.Any,
      Result,
      MappedErr,
      const Adapter extends EntityAdapterOptions<State, Command, Event, Result, MappedErr>
    >(
      entity: Entity.Entity<Type, Rpcs>,
      adapter: Adapter,
      layerOptions?: {
        readonly maxIdleTime?: DurationInput
        readonly concurrency?: number | "unbounded"
      }
    ): Layer.Layer<
      never,
      never,
      R | AdapterR<Adapter> | Rpc.Context<Rpcs> | Rpc.Middleware<Rpcs> | Sharding.Sharding
    > =>
      entity.toLayer(
        Effect.gen(function* () {
          const address = yield* Entity.CurrentAddress

          const initial = adapter.snapshots
            ? yield* adapter.snapshots.load(address.entityId).pipe(
                Effect.orElse(() => Effect.succeed(Option.none<{ readonly state: State; readonly revision: number }>()))
              )
            : Option.none<{ readonly state: State; readonly revision: number }>()

          const stateRef = yield* Ref.make(
            Option.match(initial, { onNone: () => options.initialState, onSome: ({ state }) => state })
          )
          let revision = Option.match(initial, { onNone: () => 0, onSome: ({ revision: r }) => r })

          const handlers: Partial<Entity.HandlersFrom<Rpcs>> = {}

          for (const tag of commandTags(options.commands)) {
            const handlerTag = tag as keyof Entity.HandlersFrom<Rpcs>

            if (adapter.overrides?.[tag]) {
              const override = adapter.overrides[tag]!
              const OverrideCtor = commandsByTag[tag]
              handlers[handlerTag] = ((request: { readonly payload: unknown }) => {
                const command = instantiateCommand(OverrideCtor, request.payload)
                const run = runOverride(override, command, {
                  entityId: address.entityId,
                  getState: (_id: string) => Ref.get(stateRef)
                })
                if (adapter.middleware) {
                  return Effect.zipRight(
                    adapter.middleware({ entityId: address.entityId, command }),
                    run
                  )
                }
                return run
              }) as Entity.HandlersFrom<Rpcs>[typeof handlerTag]
              continue
            }

            const CommandCtor = commandsByTag[tag]
            handlers[handlerTag] = ((request: { readonly payload: unknown }) =>
              Effect.gen(function* () {
                const command = instantiateCommand(CommandCtor, request.payload)
                if (adapter.middleware) {
                  yield* adapter.middleware({ entityId: address.entityId, command })
                }
                const state = yield* Ref.get(stateRef)

                return yield* handle(state, command).pipe(
                  Effect.matchEffect({
                    onFailure: (error) =>
                      Effect.fail(adapter.toError(error)),
                    onSuccess: (result) =>
                      Effect.gen(function* () {
                        let finalState = result.state
                        if (adapter.postHandle) {
                          finalState = adapter.postHandle({
                            entityId: address.entityId, command, events: result.events, state: result.state
                          })
                        }
                        yield* Ref.set(stateRef, finalState)
                        revision += result.events.length

                        if (adapter.snapshots && revision > 0 && revision % adapter.snapshots.every === 0) {
                          yield* adapter.snapshots.save(address.entityId, finalState, revision).pipe(
                            Effect.tapError((e) => Effect.logWarning(`snapshot save failed: ${String(e)}`)),
                            Effect.ignore
                          )
                        }

                        if (adapter.afterCommit && result.events.length > 0) {
                          yield* adapter.afterCommit({
                            entityId: address.entityId, revision, command, events: result.events, state: finalState
                          }).pipe(
                            Effect.timeout(adapter.afterCommitTimeout ?? "30 seconds"),
                            Effect.tapError((e) => Effect.logWarning(`afterCommit timed out or failed: ${String(e)}`)),
                            Effect.ignore
                          )
                        }

                        return adapter.toResult({
                          entityId: address.entityId,
                          revision,
                          command,
                          events: result.events,
                          state: finalState
                        })
                      })
                  })
                )
              })) as Entity.HandlersFrom<Rpcs>[typeof handlerTag]
          }

          return entity.of(
            handlers as Partial<Entity.HandlersFrom<Rpcs>> & Entity.HandlersFrom<Rpcs>
          )
        }),
        layerOptions
      ) as Layer.Layer<
        never,
        never,
        R | AdapterR<Adapter> | Rpc.Context<Rpcs> | Rpc.Middleware<Rpcs> | Sharding.Sharding
      >

    const toRpcHandlers = <
      Rpcs extends Rpc.Any,
      Result,
      MappedErr
    >(
      group: RpcGroup.RpcGroup<Rpcs>,
      adapter: RpcAdapterOptions<State, Command, Event, Result, MappedErr>
    ): Layer.Layer<Rpc.ToHandler<Rpcs>, never, R> => {
      const handlers: Partial<RpcGroup.HandlersFrom<Rpcs>> = {}

      for (const tag of commandTags(options.commands)) {
        const CommandCtor = commandsByTag[tag]
        const handlerTag = tag as keyof RpcGroup.HandlersFrom<Rpcs>
        handlers[handlerTag] = ((payload: unknown) =>
          Effect.gen(function* () {
            const command = instantiateCommand(CommandCtor, payload)

            return yield* handle(options.initialState, command).pipe(
              Effect.matchEffect({
                onFailure: (error) =>
                  Effect.fail(adapter.toError(error)),
                onSuccess: (result) =>
                  Effect.succeed(
                    adapter.toResult({
                      command,
                      events: result.events,
                      state: result.state
                    })
                  )
              })
            )
          })) as RpcGroup.HandlersFrom<Rpcs>[typeof handlerTag]
      }

      return group.toLayer(
        group.of(
          handlers as Partial<RpcGroup.HandlersFrom<Rpcs>> & RpcGroup.HandlersFrom<Rpcs>
        )
      ) as Layer.Layer<Rpc.ToHandler<Rpcs>, never, R>
    }

    type StateEntry = { readonly state: State; readonly revision: number }

    const toStatefulRpcHandlers = <
      Rpcs extends Rpc.Any,
      Result,
      MappedErr,
      const Adapter extends StatefulRpcAdapterOptions<State, Command, Event, Result, MappedErr>
    >(
      group: RpcGroup.RpcGroup<Rpcs>,
      adapter: Adapter
    ): Layer.Layer<Rpc.ToHandler<Rpcs>, never, R | AdapterR<Adapter>> => {
      const metrics = adapter.metrics
        ? {
            total: Metric.counter(`${adapter.metrics.prefix}.commands.total`, { incremental: true }),
            errors: Metric.counter(`${adapter.metrics.prefix}.commands.errors`, { incremental: true }),
            latency: Metric.timer(`${adapter.metrics.prefix}.command.duration_ms`, "milliseconds")
          }
        : undefined

      return group.toLayer(
        Effect.gen(function* () {
          const store = yield* SynchronizedRef.make(new Map<string, StateEntry>())
          const accessOrder = new Map<string, number>()

          const evictIfNeeded = (map: Map<string, StateEntry>) => {
            if (!adapter.maxEntries || map.size <= adapter.maxEntries) return map
            const sorted = [...accessOrder.entries()].sort((a, b) => a[1] - b[1])
            const toEvict = sorted.slice(0, map.size - adapter.maxEntries)
            const next = new Map(map)
            for (const [id] of toEvict) {
              next.delete(id)
              accessOrder.delete(id)
            }
            return next
          }

          const getOrLoad = (map: Map<string, StateEntry>, entityId: string): Effect.Effect<StateEntry, unknown, unknown> => {
            accessOrder.set(entityId, Date.now())
            if (map.has(entityId)) return Effect.succeed(map.get(entityId)!)
            if (!adapter.snapshots) return Effect.succeed({ state: options.initialState, revision: 0 })
            return adapter.snapshots.load(entityId).pipe(
              Effect.map(Option.getOrElse(() => ({ state: options.initialState, revision: 0 }))),
              Effect.orElse(() => Effect.succeed({ state: options.initialState, revision: 0 }))
            )
          }

          const getState = (entityId: string): Effect.Effect<State, unknown, unknown> =>
            SynchronizedRef.get(store).pipe(
              Effect.flatMap((map) => getOrLoad(map, entityId)),
              Effect.map(({ state }) => state)
            )

          const handlers: Partial<RpcGroup.HandlersFrom<Rpcs>> = {}

          for (const tag of commandTags(options.commands)) {
            const handlerTag = tag as keyof RpcGroup.HandlersFrom<Rpcs>

            if (adapter.overrides?.[tag]) {
              const override = adapter.overrides[tag]!
              handlers[handlerTag] = ((payload: unknown) => {
                const command = instantiateCommand(commandsByTag[tag], payload)
                const entityId = adapter.entityId(command)
                const run = runOverride(override, command, { entityId, getState })
                if (adapter.middleware) {
                  return Effect.zipRight(
                    adapter.middleware({ entityId, command }),
                    run
                  )
                }
                return run
              }) as RpcGroup.HandlersFrom<Rpcs>[typeof handlerTag]
              continue
            }

            const CommandCtor = commandsByTag[tag]
            handlers[handlerTag] = ((payload: unknown) => {
              const command = instantiateCommand(CommandCtor, payload)
              const entityId = adapter.entityId(command)

              const run = Effect.gen(function* () {
                if (adapter.middleware) {
                  yield* adapter.middleware({ entityId, command })
                }
                const [result, events, finalState, nextRevision] = yield* SynchronizedRef.modifyEffect(store, (map) =>
                  getOrLoad(map, entityId).pipe(
                    Effect.flatMap(({ state, revision: prevRevision }) =>
                      handle(state, command).pipe(
                        Effect.mapError((error) => adapter.toError(error)),
                        Effect.map(({ events, state: next }) => {
                          let finalState = next
                          if (adapter.postHandle) {
                            finalState = adapter.postHandle({
                              entityId, command, events, state: next
                            })
                          }
                          const nextRevision = prevRevision + events.length
                          const nextMap = evictIfNeeded(new Map(map).set(entityId, { state: finalState, revision: nextRevision }))
                          return [
                            [
                              adapter.toResult({
                                entityId,
                                revision: nextRevision,
                                command,
                                events,
                                state: finalState
                              }),
                              events,
                              finalState,
                              nextRevision
                            ] as const,
                            nextMap
                          ] as const
                        })
                      )
                    )
                  )
                )

                if (adapter.afterCommit && events.length > 0) {
                  yield* adapter.afterCommit({
                    entityId,
                    revision: nextRevision,
                    command,
                    events,
                    state: finalState
                  }).pipe(
                    Effect.timeout(adapter.afterCommitTimeout ?? "30 seconds"),
                    Effect.tapError((e) => Effect.logWarning(`afterCommit timed out or failed: ${String(e)}`)),
                    Effect.ignore
                  )
                }

                if (adapter.snapshots && nextRevision > 0 && nextRevision % adapter.snapshots.every === 0) {
                  yield* adapter.snapshots.save(entityId, finalState, nextRevision).pipe(
                    Effect.tapError((e) => Effect.logWarning(`snapshot save failed: ${String(e)}`)),
                    Effect.ignore
                  )
                }

                return result
              })

              if (metrics) {
                return run.pipe(
                  Metric.trackDuration(Metric.tagged(metrics.latency, "command", tag)),
                  Effect.tap(() => Metric.increment(Metric.tagged(metrics.total, "command", tag))),
                  Effect.tapError(() => Metric.increment(Metric.tagged(metrics.errors, "command", tag)))
                )
              }
              return run
            }) as RpcGroup.HandlersFrom<Rpcs>[typeof handlerTag]
          }

          return group.of(
            handlers as Partial<RpcGroup.HandlersFrom<Rpcs>> & RpcGroup.HandlersFrom<Rpcs>
          )
        })
      ) as Layer.Layer<Rpc.ToHandler<Rpcs>, never, R | AdapterR<Adapter>>
    }

    const toHttpRoute = <Rpcs extends Rpc.Any>(
      group: RpcGroup.RpcGroup<Rpcs>,
      path: `/${string}`,
      handlers: Layer.Layer<Rpc.ToHandler<Rpcs>, never, R>
    ) => makeRoute(group, path, handlers) as Layer.Layer<never, never, R>

    return {
      initialState: options.initialState,
      commands: options.commands,
      evolve,
      decide,
      handle,
      run,
      toEntityLayer,
      toRpcHandlers,
      toStatefulRpcHandlers,
      toHttpRoute
    }
  }
