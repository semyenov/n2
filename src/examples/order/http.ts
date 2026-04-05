/**
 * HTTP RPC route for the Order service.
 */
import { Order } from "./aggregate.js"
import { OrderRpcs } from "./contracts.js"
import { OrderHandlers } from "./entity.js"

export const OrderRpcRoute = Order.toHttpRoute(
    OrderRpcs,
    "/rpc/orders",
    OrderHandlers
)
