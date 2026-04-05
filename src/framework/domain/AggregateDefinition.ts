/**
 * @since 1.0.0
 * @module AggregateDefinition
 *
 * Core aggregate definition: decide + evolve.
 *
 * This is the only type our framework adds on top of Effect.
 * Everything else (Entity, Machine, EventJournal, MessageStorage)
 * comes from the native Effect ecosystem.
 *
 * An AggregateDefinition is converted to:
 * - Entity (via @effect/cluster) for distributed command handling
 * - Machine (via @effect/experimental) for stateful actor semantics
 * - EventLog.group (via @effect/experimental) for typed event handling
 */
import type * as Effect from "effect/Effect"
import type * as Schema from "effect/Schema"

/**
 * @since 1.0.0
 * @category type ids
 */
export const TypeId: unique symbol = Symbol.for("n2/AggregateDefinition")

/**
 * @since 1.0.0
 * @category type ids
 */
export type TypeId = typeof TypeId

/**
 * An aggregate definition encapsulates the write-side consistency boundary.
 *
 * - `decide` is effectful: may use services (Clock, IdGenerator, etc.)
 * - `evolve` is pure: deterministic for event replay
 * - All types inferred from schemas
 *
 * @since 1.0.0
 * @category models
 */
export interface AggregateDefinition<
  in out Name extends string,
  in out State,
  in out Command,
  in out Event,
  in out Err,
  in out R
> {
  readonly [TypeId]: TypeId
  readonly name: Name
  readonly initialState: State
  readonly decide: (
    state: State,
    command: Command
  ) => Effect.Effect<ReadonlyArray<Event>, Err, R>
  readonly evolve: (state: State, event: Event) => State
  readonly schemas: {
    readonly state: Schema.Schema.All & { readonly Type: State }
    readonly command: Schema.Schema.All & { readonly Type: Command }
    readonly event: Schema.Schema.All & { readonly Type: Event }
    readonly error: Schema.Schema.All & { readonly Type: Err }
  }
}

/**
 * @since 1.0.0
 * @category models
 */
export type Any = AggregateDefinition<string, unknown, unknown, unknown, unknown, unknown>

/**
 * @since 1.0.0
 * @category type-level
 */
export type NameOf<A extends Any> = A extends AggregateDefinition<infer N, unknown, unknown, unknown, unknown, unknown> ? N : never

/**
 * @since 1.0.0
 * @category type-level
 */
export type StateOf<A extends Any> = A extends AggregateDefinition<string, infer S, unknown, unknown, unknown, unknown> ? S : never

/**
 * @since 1.0.0
 * @category type-level
 */
export type CommandOf<A extends Any> = A extends AggregateDefinition<string, unknown, infer C, unknown, unknown, unknown> ? C : never

/**
 * @since 1.0.0
 * @category type-level
 */
export type EventOf<A extends Any> = A extends AggregateDefinition<string, unknown, unknown, infer E, unknown, unknown> ? E : never

/**
 * @since 1.0.0
 * @category type-level
 */
export type ErrorOf<A extends Any> = A extends AggregateDefinition<string, unknown, unknown, unknown, infer Err, unknown> ? Err : never

/**
 * @since 1.0.0
 * @category type-level
 */
export type ContextOf<A extends Any> = A extends AggregateDefinition<string, unknown, unknown, unknown, unknown, infer R> ? R : never

/**
 * Define an aggregate.
 *
 * @since 1.0.0
 * @category constructors
 */
export const define = <
  const Name extends string,
  State,
  Command,
  Event,
  Err,
  R
>(config: {
  readonly name: Name
  readonly initialState: State
  readonly decide: (
    state: State,
    command: Command
  ) => Effect.Effect<ReadonlyArray<Event>, Err, R>
  readonly evolve: (state: State, event: Event) => State
  readonly schemas: {
    readonly state: Schema.Schema.All & { readonly Type: State }
    readonly command: Schema.Schema.All & { readonly Type: Command }
    readonly event: Schema.Schema.All & { readonly Type: Event }
    readonly error: Schema.Schema.All & { readonly Type: Err }
  }
}): AggregateDefinition<Name, State, Command, Event, Err, R> => ({
  [TypeId]: TypeId,
  ...config
})
