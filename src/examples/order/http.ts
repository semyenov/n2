/**
 * HTTP RPC route for the Order service.
 */
import * as N2 from "../../framework/helpers/index.js"
import { OrderRpcs } from "./contracts.js"
import { OrderHandlers } from "./entity.js"

export const OrderRpcRoute = N2.Http.makeRoute(OrderRpcs, "/rpc/orders", OrderHandlers)
