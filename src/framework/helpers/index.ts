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
 * const Order = N2.define({
 *   initialState,
 *   commands,
 *   evolve,
 *   decide
 * })
 *
 * const OrderHandlers = Order.toRpcHandlers(OrderRpcs, { toResult, toError })
 * const OrderEntityLayer = Order.toEntityLayer(OrderEntity, { toResult, toError })
 * const OrderRoute = Order.toHttpRoute(OrderRpcs, "/rpc/orders", { handlers: OrderHandlers })
 * ```
 */
export {
  define,
  type AdapterOptions,
  type Definition,
  type ExecutionContext,
  type ExecutionErrorContext,
  type ExecutionMode
} from "./Definition.js"

export { rpcFromCommand } from "./EntityBuilder.js"
