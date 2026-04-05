/**
 * @since 1.0.0
 * @module projection
 *
 * Projection module. Mix of Kafka-oriented projections and native Effect re-exports.
 */

// Our Kafka-oriented projection infrastructure
export * as ProjectorDefinition from "./ProjectorDefinition.js"
export * as CheckpointStore from "./CheckpointStore.js"
export * as DeadLetter from "./DeadLetter.js"
export * as Subscription from "./Subscription.js"
export * as Replay from "./Replay.js"

// Native @effect/experimental re-exports for non-Kafka projections
export {
  /** @effect/experimental PersistedCache -- projection caching with TTL + invalidation */
  PersistedCache,
  /** @effect/experimental Reactivity -- query invalidation and reactive subscriptions */
  Reactivity
} from "@effect/experimental"
