/**
 * @since 1.0.0
 * @module testing
 */
export * as InMemoryKafkaPublisher from "./InMemoryKafkaPublisher.js"
export * as InMemoryKafkaConsumer from "./InMemoryKafkaConsumer.js"
export * as InMemoryDeadLetter from "./InMemoryDeadLetter.js"
export * as TestClock from "./TestClock.js"
export * as DeterministicIdGenerator from "./DeterministicIdGenerator.js"
export * as AggregateTestHarness from "./AggregateTestHarness.js"
export * as ProjectorTestHarness from "./ProjectorTestHarness.js"

// Native @effect re-exports for testing
export {
  /** @effect/rpc RpcTest -- in-memory RPC client from RpcGroup */
  RpcTest
} from "@effect/rpc"
export {
  /** @effect/cluster TestRunner -- in-memory cluster for testing */
  TestRunner
} from "@effect/cluster"
