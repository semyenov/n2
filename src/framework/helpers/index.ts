/**
 * @since 1.0.0
 * @module N2
 *
 * Definition-centric helper functions for domain models.
 *
 * @example
 * ```ts
 * import * as N2 from "n2/framework/helpers/index.js"
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
  type SnapshotEntry,
  type SnapshotService
} from "./Snapshots.js"

export {
  makeOutboxService,
  computeRetryDelaySeconds,
  type OutboxEntry,
  type OutboxService
} from "./Outbox.js"

export {
  makePublishWorkflow,
  EventPublishError
} from "./PublishWorkflow.js"

export { makeEventDecoder } from "./EventDecoder.js"

export { makeTestAggregate } from "./TestAggregate.js"

export {
  makeReplayTool,
  parseReplayOptions,
  type ReplayOptions
} from "./Replay.js"
