/**
 * @since 1.0.0
 * @module N2
 *
 * Definition-centric helpers for aggregate, RPC, and entity wiring.
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
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

type DecideEventsFor<Cmd extends Tagged, Handlers> =
  TagOf<Cmd> extends keyof Handlers
    ? Handlers[TagOf<Cmd>] extends (...args: any[]) => Effect.Effect<ReadonlyArray<infer E>, any, any>
      ? E
      : never
    : never

const commandTags = <Command extends Tagged>(
  commands: CommandConstructors<Command>
): ReadonlyArray<TagOf<Command>> =>
  Object.keys(commands).filter((tag): tag is TagOf<Command> => tag in commands)

const instantiateCommand = <CurrentCommand extends Tagged>(
  Command: new (...args: ReadonlyArray<never>) => CurrentCommand,
  payload: unknown
): CurrentCommand =>
  Reflect.construct(Command, [payload]) as CurrentCommand

const makeRoute = <Rpcs extends Rpc.Any, R>(
  group: RpcGroup.RpcGroup<Rpcs>,
  path: `/${string}`,
  handlers: Layer.Layer<Rpc.ToHandler<Rpcs>, never, R>
) =>
  RpcServer
    .layerHttpRouter({ group, path })
    .pipe(
      Layer.provide(handlers),
      Layer.provide(RpcSerialization.layerJson),
      Layer.provide(HttpLayerRouter.cors())
    )

/** Options for mapping execution results when wiring to a cluster Entity. */
export interface EntityAdapterOptions<State, Command, Event, Result, MappedErr> {
  readonly toResult: (ctx: {
    readonly entityId: string
    readonly revision: number
    readonly command: Command
    readonly events: ReadonlyArray<Event>
    readonly state: State
  }) => Result
  readonly toError: (error: unknown) => MappedErr
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
  Handlers extends DecideHandlers<State, Command, Event, Err, R> = DecideHandlers<State, Command, Event, Err, R>
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
    { readonly events: ReadonlyArray<DecideEventsFor<Cmd, Handlers>>; readonly state: State },
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
  readonly toEntityLayer: <Type extends string, Rpcs extends Rpc.Any, Result, MappedErr>(
    entity: Entity.Entity<Type, Rpcs>,
    options: EntityAdapterOptions<State, Command, Event, Result, MappedErr>,
    layerOptions?: {
      readonly maxIdleTime?: DurationInput
      readonly concurrency?: number | "unbounded"
    }
  ) => Layer.Layer<
    never,
    never,
    R | Rpc.Context<Rpcs> | Rpc.Middleware<Rpcs> | Sharding.Sharding
  >
  readonly toRpcHandlers: <Rpcs extends Rpc.Any, Result, MappedErr>(
    group: RpcGroup.RpcGroup<Rpcs>,
    options: RpcAdapterOptions<State, Command, Event, Result, MappedErr>
  ) => Layer.Layer<Rpc.ToHandler<Rpcs>, never, R>
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
  <State, Err, R, const Handlers extends DecideHandlers<State, Command, Event, Err, R>>(options: {
    readonly initialState: State
    readonly commands: CommandConstructors<Command>
    readonly evolve: EvolveHandlers<State, Event>
    readonly decide: Handlers
  }): Definition<State, Event, Command, Err, R, Handlers> => {
    const commandsByTag: Readonly<Record<TagOf<Command>, new (...args: ReadonlyArray<never>) => Command>> =
      options.commands

    const evolve: Definition<State, Event, Command, Err, R>["evolve"] = (state, event) => {
      const tag = event._tag as TagOf<Event>
      return (options.evolve as Record<TagOf<Event>, (state: State, event: Event) => State>)[tag](state, event)
    }

    const decide = (state: State, command: Command): Effect.Effect<ReadonlyArray<Event>, Err, R> => {
      const tag = command._tag as TagOf<Command>
      return (
        options.decide as Record<
          TagOf<Command>,
          (state: State, command: Command) => Effect.Effect<ReadonlyArray<Event>, Err, R>
        >
      )[tag](state, command)
    }

    const handle = <Cmd extends Command>(state: State, command: Cmd) =>
      Effect.gen(function* () {
        const events = yield* decide(state, command)
        let nextState = state
        for (const event of events) {
          nextState = evolve(nextState, event)
        }
        return {
          events,
          state: nextState
        }
      }) as Effect.Effect<
        { readonly events: ReadonlyArray<DecideEventsFor<Cmd, Handlers>>; readonly state: State },
        Err,
        R
      >

    const run: Definition<State, Event, Command, Err, R>["run"] = (
      commands,
      state = options.initialState
    ) =>
      Effect.gen(function* () {
        let allEvents: Array<Event> = []
        let currentState = state
        for (const command of commands) {
          const result = yield* handle(currentState, command)
          allEvents = allEvents.concat(result.events as Array<Event>)
          currentState = result.state
        }
        return { events: allEvents as ReadonlyArray<Event>, state: currentState }
      })

    const toEntityLayer = <
      Type extends string,
      Rpcs extends Rpc.Any,
      Result,
      MappedErr
    >(
      entity: Entity.Entity<Type, Rpcs>,
      adapter: EntityAdapterOptions<State, Command, Event, Result, MappedErr>,
      layerOptions?: {
        readonly maxIdleTime?: DurationInput
        readonly concurrency?: number | "unbounded"
      }
    ): Layer.Layer<
      never,
      never,
      R | Rpc.Context<Rpcs> | Rpc.Middleware<Rpcs> | Sharding.Sharding
    > =>
      entity.toLayer(
        Effect.gen(function* () {
          const address = yield* Entity.CurrentAddress
          const stateRef = yield* Ref.make(options.initialState)
          let revision = 0

          const handlers: Partial<Entity.HandlersFrom<Rpcs>> = {}

          for (const tag of commandTags(options.commands)) {
            const CommandCtor = commandsByTag[tag]
            const handlerTag = tag as keyof Entity.HandlersFrom<Rpcs>
            handlers[handlerTag] = ((request: { readonly payload: unknown }) =>
              Effect.gen(function* () {
                const state = yield* Ref.get(stateRef)
                const command = instantiateCommand(CommandCtor, request.payload)

                return yield* handle(state, command).pipe(
                  Effect.matchEffect({
                    onFailure: (error) =>
                      Effect.fail(adapter.toError(error)),
                    onSuccess: (result) =>
                      Effect.gen(function* () {
                        yield* Ref.set(stateRef, result.state)
                        revision += result.events.length

                        return adapter.toResult({
                          entityId: address.entityId,
                          revision,
                          command,
                          events: result.events,
                          state: result.state
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
        R | Rpc.Context<Rpcs> | Rpc.Middleware<Rpcs> | Sharding.Sharding
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
      toHttpRoute
    }
  }
