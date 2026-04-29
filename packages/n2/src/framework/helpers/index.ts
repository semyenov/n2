/**
 * @since 1.0.0
 * @module N2
 *
 * Definition-centric helper functions for domain models.
 *
 * @example
 * ```ts
 * import * as N2 from "@semyenov/n2/helpers"
 *
 * const OrderCommands = N2.defineCommands(
 *   (p) => p.orderId,
 *   CreateOrder, AddItem, SubmitOrder
 * )
 * const OrderEntity = OrderCommands.toPersistedEntity("Order")
 *
 * const Order = N2.define<OrderEvent, OrderCommand>()({
 *   initialState,
 *   commands: OrderCommands.constructors,
 *   evolve,
 *   decide
 * })
 *
 * const OrderHandlers = Order.toRpcHandlers(OrderRpcs, { toResult, toError })
 * const OrderEntityLayer = Order.toEntityLayer(OrderEntity, { toResult, toError })
 * const OrderRoute = Order.toHttpRoute(OrderRpcs, "/rpc/orders", OrderHandlers)
 * ```
 */
export {
  define,
  type Definition,
  type EntityAdapterOptions,
  type RpcAdapterOptions,
  type StatefulRpcAdapterOptions
} from "./Definition.js"

export {
  defineCommands,
  defineErrors,
  defineEvents,
  eventPayloadSchema,
  defineSchemaUnion,
  defineTaggedConstructors,
  type CommandCollection,
  type CommandDefinition,
  type CommandFailureSchemaOf,
  type CommandFields,
  type CommandFieldsOf,
  type CommandInfoOf,
  type CommandPayloadFieldsOf,
  type CommandPayloadTypeOf,
  type CommandSuccessSchemaOf,
  type CommandTagOf,
  type SchemaUnion,
  type Tagged,
  type TaggedCollection,
  type TaggedConstructor,
  type TaggedConstructors,
  type TaggedSchema
} from "./Definitions.js"

export { rpcFromCommand } from "./EntityBuilder.js"

export { makeHttpClient, makePromiseClient, type RpcPromiseClient } from "./Client.js"

export { makeFetchClient } from "./FetchClient.js"

export {
  makeSnapshotService,
  makeSnapshotOps,
  type SnapshotEntry,
  type SnapshotService
} from "./Snapshots.js"

export {
  makeOutboxService,
  makeOutboxJsonService,
  computeRetryDelaySeconds,
  type OutboxEntry,
  type OutboxJsonConfig,
  type OutboxService
} from "./Outbox.js"

export {
  makePublishWorkflow,
  EventPublishError
} from "./PublishWorkflow.js"

export {
  EventMessageHeaders,
  makeEventMessage,
  makeEventMessageFields,
  type EventMessageFields,
  type EventMessageOptions,
  type EventMessagePayload
} from "./EventMessage.js"

export { makeEventDecoder } from "./EventDecoder.js"

export { wireProjectionHandler } from "./Projection.js"

export {
  makeConsoleEventPublisherLayer,
  makeStandardOutboxWiring,
  makeStandardSnapshotWiring,
  type StandardOutboxWiringConfig,
  type StandardSnapshotWiringConfig
} from "./ServiceWiring.js"

export { makeTestAggregate } from "./TestAggregate.js"

export {
  makeReplayTool,
  makeReplayProgram,
  parseReplayOptions,
  type ReplayEvent,
  type ReplayOptions,
  type ReplayProgramConfig,
  type ReplayProgramSummary
} from "./Replay.js"

export {
  makeEventMessageFactory,
  type EventMessageFactoryConfig,
  type EventMessageFactoryOptions
} from "./EventMessageFactory.js"

export {
  mergeAssetsByKey,
  makeToError,
  standardPublishRetry,
  makeStateOverride,
  makeAfterCommitPublisher,
  type StateOverrideConfig,
  type StateOverrideContext,
  type AfterCommitPublisherConfig
} from "./EntityWiring.js"

export {
  makeProjectionLayer,
  type ProjectionHandlerEntry,
  type ProjectionHandlerMap,
  type ProjectionLayerConfig
} from "./ProjectionLayer.js"
