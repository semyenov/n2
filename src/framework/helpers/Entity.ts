/**
 * @since 1.0.0
 * @module N2.Entity
 *
 * Helpers for wiring aggregates into cluster entities and RPC handlers.
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import { Entity, type Sharding } from "@effect/cluster"
import type { Rpc, RpcGroup } from "@effect/rpc"
import type { DurationInput } from "effect/Duration"

/**
 * Creates a stateful dispatch function for use inside Entity.toLayer.
 * Manages aggregate state in a Ref, tracks revision count.
 *
 * @since 1.0.0
 */
export const makeDispatch = <State, Command, Event, Err, R, Result, MappedErr>(options: {
  readonly handleCommand: (
    state: State,
    command: Command
  ) => Effect.Effect<{ readonly events: ReadonlyArray<Event>; readonly state: State }, Err, R>
  readonly initialState: State
  readonly toResult: (entityId: string, revision: number, state: State) => Result
  readonly toError: (err: unknown) => MappedErr
}): Effect.Effect<
  (command: Command) => Effect.Effect<Result, MappedErr, R>,
  never,
  Entity.CurrentAddress
> =>
  Effect.gen(function*() {
    const address = yield* Entity.CurrentAddress
    const stateRef = yield* Ref.make(options.initialState)
    let revision = 0

    return (command: Command) =>
      Effect.gen(function*() {
        const state = yield* Ref.get(stateRef)
        const result = yield* options.handleCommand(state, command)
        yield* Ref.set(stateRef, result.state)
        revision += result.events.length
        return options.toResult(address.entityId, revision, result.state)
      }).pipe(
        Effect.catchAll((err) => Effect.fail(options.toError(err)))
      )
  })

/**
 * Creates a complete entity layer with automatic command dispatch.
 * Combines makeDispatch + entity.of + entity.toLayer in one step.
 *
 * @since 1.0.0
 */
export const makeEntityLayer = <
  Type extends string,
  Rpcs extends Rpc.Any,
  State,
  Command extends { readonly _tag: string },
  Event, Err, R, Result, MappedErr
>(
  entity: Entity.Entity<Type, Rpcs>,
  options: {
    readonly handleCommand: (
      state: State,
      command: Command
    ) => Effect.Effect<{ readonly events: ReadonlyArray<Event>; readonly state: State }, Err, R>
    readonly initialState: State
    readonly toResult: (entityId: string, revision: number, state: State) => Result
    readonly toError: (err: unknown) => MappedErr
    readonly commands: { readonly [K in Command["_tag"]]: new (...args: ReadonlyArray<never>) => Extract<Command, { _tag: K }> }
  },
  layerOptions?: {
    readonly maxIdleTime?: DurationInput
    readonly concurrency?: number | "unbounded"
  }
): Layer.Layer<never, never, R | Rpc.Context<Rpcs> | Rpc.Middleware<Rpcs> | Sharding.Sharding> =>
  entity.toLayer(
    makeDispatch({
      handleCommand: options.handleCommand,
      initialState: options.initialState,
      toResult: options.toResult,
      toError: options.toError
    }).pipe(
      Effect.map((dispatch) => {
        const handlers = {} as Record<string, (req: { readonly payload: unknown }) => unknown>
        for (const [tag, Ctor] of Object.entries(options.commands)) {
          const C = Ctor as unknown as new (p: unknown) => Command
          handlers[tag] = (req) => dispatch(new C(req.payload))
        }
        return entity.of(handlers as unknown as Entity.HandlersFrom<Rpcs>)
      })
    ),
    layerOptions
  ) as unknown as Layer.Layer<never, never, R | Rpc.Context<Rpcs> | Rpc.Middleware<Rpcs> | Sharding.Sharding>

/**
 * Creates stateless RPC handlers for all commands in one step.
 * Each request starts from initialState (no state persistence).
 *
 * @since 1.0.0
 */
export const makeRpcHandlers = <
  Rpcs extends Rpc.Any,
  State,
  Command extends { readonly _tag: string },
  Event, Err, R, Result, MappedErr
>(
  rpcs: RpcGroup.RpcGroup<Rpcs>,
  options: {
    readonly handleCommand: (
      state: State,
      command: Command
    ) => Effect.Effect<{ readonly events: ReadonlyArray<Event>; readonly state: State }, Err, R>
    readonly initialState: State
    readonly toResult: (payload: Record<string, unknown>, events: ReadonlyArray<Event>, state: State) => Result
    readonly toError: (err: unknown) => MappedErr
    readonly commands: { readonly [K in Command["_tag"]]: new (...args: ReadonlyArray<never>) => Extract<Command, { _tag: K }> }
  }
): Layer.Layer<Rpc.ToHandler<Rpcs>, never, R> => {
  const handlers = {} as Record<string, (payload: unknown) => unknown>
  for (const [tag, Ctor] of Object.entries(options.commands)) {
    const C = Ctor as unknown as new (p: unknown) => Command
    handlers[tag] = (payload: unknown) =>
      options.handleCommand(options.initialState, new C(payload)).pipe(
        Effect.map(({ events, state }) => options.toResult(payload as Record<string, unknown>, events, state)),
        Effect.catchAll((err: unknown) => Effect.fail(options.toError(err)))
      )
  }
  return rpcs.toLayer(
    rpcs.of(handlers as unknown as RpcGroup.HandlersFrom<Rpcs>)
  ) as unknown as Layer.Layer<Rpc.ToHandler<Rpcs>, never, R>
}
