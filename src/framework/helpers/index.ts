/**
 * @since 1.0.0
 * @module N2
 *
 * Namespaced helper functions to reduce aggregate boilerplate.
 *
 * @example
 * ```ts
 * import * as N2 from "n2/framework/helpers/index.js"
 *
 * const handleCommand = N2.Aggregate.makeHandleCommand(decide, evolve)
 * const dispatch = N2.Entity.makeDispatch({ handleCommand, initialState, ... })
 * const route = N2.Http.makeRoute(rpcs, "/rpc/orders", handlers)
 * ```
 */
export * as Aggregate from "./Aggregate.js"
export * as Entity from "./Entity.js"
export * as Http from "./Http.js"
