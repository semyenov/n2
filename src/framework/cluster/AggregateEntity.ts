/**
 * @since 1.0.0
 * @module AggregateEntity
 *
 * Thin bridge between AggregateDefinition and @effect/cluster Entity.
 * Uses Entity.fromRpcGroup directly -- no wrapper overhead.
 */
import * as Effect from "effect/Effect"
import type * as Duration from "effect/Duration"
import { Entity } from "@effect/cluster"
import type { RpcGroup } from "@effect/rpc"
import type { Rpc } from "@effect/rpc"
import type { AggregateDefinition } from "../domain/AggregateDefinition.js"
import * as AggregateRuntime from "../runtime/AggregateRuntime.js"

/**
 * @since 1.0.0
 * @category models
 */
export interface AggregateEntityConfig {
  readonly maxIdleTime?: Duration.DurationInput
  readonly concurrency?: number | "unbounded"
  readonly snapshotEvery?: number
}

/**
 * Creates a cluster Entity from an AggregateDefinition + RpcGroup.
 *
 * Returns the Entity directly (no wrapper) plus an AggregateRuntime
 * for command handling within entity behaviors.
 *
 * @since 1.0.0
 * @category constructors
 */
export const make = <
  Name extends string,
  State,
  Command,
  Event,
  Err,
  R,
  Rpcs extends Rpc.Any
>(
  definition: AggregateDefinition<Name, State, Command, Event, Err, R>,
  rpcGroup: RpcGroup.RpcGroup<Rpcs>,
  config?: AggregateEntityConfig
) => ({
  /**
   * The @effect/cluster Entity. Use entity.toLayer() to register,
   * entity.client to get typed client.
   */
  entity: Entity.fromRpcGroup(definition.name, rpcGroup),

  /**
   * The aggregate runtime for hydrate/handle within entity behaviors.
   */
  runtime: AggregateRuntime.make(definition, {
    snapshotEvery: config?.snapshotEvery
  }),

  /**
   * Convenience: the config for Entity.toLayer options.
   */
  entityOptions: {
    maxIdleTime: config?.maxIdleTime,
    concurrency: config?.concurrency
  } as const
})

/**
 * Re-export Entity for direct usage.
 *
 * @since 1.0.0
 * @category re-exports
 */
export { Entity }
