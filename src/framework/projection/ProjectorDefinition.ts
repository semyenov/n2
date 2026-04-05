/**
 * @since 1.0.0
 * @module ProjectorDefinition
 *
 * Projector definition type. For new code, prefer @effect/experimental
 * EventLog.group() + Reactivity for typed event handling with invalidation.
 *
 * This module provides both:
 * 1. Our ProjectorDefinition interface (for Kafka-based projections)
 * 2. Re-exports of EventLog.group and Reactivity for native projections
 */
import type * as Effect from "effect/Effect"
import { EventLog, Reactivity } from "@effect/experimental"
import type { EventEnvelope } from "../contracts/EventEnvelope.js"

/**
 * @since 1.0.0
 * @category type ids
 */
export const TypeId: unique symbol = Symbol.for("n2/ProjectorDefinition")

/**
 * @since 1.0.0
 * @category type ids
 */
export type TypeId = typeof TypeId

/**
 * A projector definition handles events to build read-side state.
 * The handler must be idempotent.
 *
 * For native Effect projections, use EventLog.group() instead.
 *
 * @since 1.0.0
 * @category models
 */
export interface ProjectorDefinition<State> {
  readonly [TypeId]: TypeId
  readonly name: string
  readonly initialState: State
  readonly handle: (
    state: State,
    envelope: EventEnvelope
  ) => Effect.Effect<State>
}

/**
 * @since 1.0.0
 * @category constructors
 */
export const define = <State>(config: {
  readonly name: string
  readonly initialState: State
  readonly handle: (
    state: State,
    envelope: EventEnvelope
  ) => Effect.Effect<State>
}): ProjectorDefinition<State> => ({
  [TypeId]: TypeId,
  ...config
})

/**
 * Re-export @effect/experimental EventLog.group for native event handling.
 *
 * @example
 * ```ts
 * const OrderProjectionHandlers = EventLogGroup(OrderEventGroup, (handlers) =>
 *   handlers
 *     .handle("OrderCreated", ({ payload, entry }) => Effect.succeed(void 0))
 *     .handle("ItemAdded", ({ payload, entry }) => Effect.succeed(void 0))
 * )
 * ```
 *
 * @since 1.0.0
 * @category re-exports
 */
export const EventLogGroup = EventLog.group

/**
 * Re-export @effect/experimental Reactivity for query invalidation.
 *
 * @since 1.0.0
 * @category re-exports
 */
export { Reactivity }
