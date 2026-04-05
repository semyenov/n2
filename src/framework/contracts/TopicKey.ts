/**
 * @since 1.0.0
 * @module TopicKey
 *
 * Branded topic names and partition keys for Kafka.
 */
import * as Schema from "effect/Schema"
import * as Brand from "effect/Brand"
import type { EntityId } from "@effect/cluster/EntityId"

/**
 * @since 1.0.0
 * @category brands
 */
export type TopicName = string & Brand.Brand<"TopicName">

/**
 * @since 1.0.0
 * @category brands
 */
export const TopicName = Brand.nominal<TopicName>()

/**
 * @since 1.0.0
 * @category brands
 */
export type PartitionKey = string & Brand.Brand<"PartitionKey">

/**
 * @since 1.0.0
 * @category brands
 */
export const PartitionKey = Brand.nominal<PartitionKey>()

/**
 * @since 1.0.0
 * @category schemas
 */
export const TopicNameSchema = Schema.String.pipe(
  Schema.brand("TopicName")
)

/**
 * @since 1.0.0
 * @category schemas
 */
export const PartitionKeySchema = Schema.String.pipe(
  Schema.brand("PartitionKey")
)

/**
 * Creates an aggregate event topic name.
 *
 * @since 1.0.0
 * @category constructors
 */
export const makeAggregateTopicName = (aggregateType: string): TopicName =>
  TopicName(`n2.aggregate.${aggregateType}.events`)

/**
 * Creates an integration event topic name.
 *
 * @since 1.0.0
 * @category constructors
 */
export const makeIntegrationTopicName = (name: string): TopicName =>
  TopicName(`n2.integration.${name}`)

/**
 * Creates a dead-letter topic name.
 *
 * @since 1.0.0
 * @category constructors
 */
export const makeDLQTopicName = (projectorName: string): TopicName =>
  TopicName(`n2.dlq.${projectorName}`)

/**
 * Creates a partition key from an aggregate ID.
 *
 * @since 1.0.0
 * @category constructors
 */
export const makePartitionKey = (aggregateId: EntityId): PartitionKey =>
  PartitionKey(aggregateId)
