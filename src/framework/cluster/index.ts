/**
 * @since 1.0.0
 * @module cluster
 */
export * as AggregateEntity from "./AggregateEntity.js"

// Native @effect/cluster re-exports
export {
  // Entity system
  Entity,
  EntityProxy,
  EntityProxyServer,
  EntityResource,
  EntityId,
  EntityType,
  EntityAddress,

  // Sharding & config
  Sharding,
  ShardingConfig,
  ClusterSchema,
  ClusterMetrics,

  // Runner infrastructure
  Runner,
  Runners,
  RunnerAddress,
  RunnerHealth,
  RunnerServer,
  RunnerStorage,
  SqlRunnerStorage,
  HttpRunner,
  SocketRunner,
  SingleRunner,
  TestRunner,

  // Message persistence
  MessageStorage,
  SqlMessageStorage,
  Message,
  Envelope,
  Reply,

  // Utilities
  Snowflake,
  Singleton,
  DeliverAt,
  ClusterWorkflowEngine
} from "@effect/cluster"
