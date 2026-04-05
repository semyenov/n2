/**
 * @since 1.0.0
 * @deprecated Use native Effect tracing (Effect.withSpan + Effect.currentSpan).
 * Cluster Envelope automatically propagates traceId/spanId.
 * @module RequestContext
 *
 * Request context propagation. Uses FiberRef to carry metadata through fibers.
 * In cluster mode, context is automatically propagated via Envelope headers
 * (traceId, spanId, sampled) from @effect/cluster.
 */
import * as Effect from "effect/Effect"
import * as FiberRef from "effect/FiberRef"
import { Metadata, empty as emptyMetadata } from "../contracts/Metadata.js"

/**
 * FiberRef carrying the current request metadata through the fiber.
 *
 * @since 1.0.0
 * @category refs
 */
export const RequestContext: FiberRef.FiberRef<Metadata> = FiberRef.unsafeMake(
  emptyMetadata
)

/**
 * Run an effect with specific request metadata in scope.
 *
 * @since 1.0.0
 * @category combinators
 */
export const withRequestContext = <A, E, R>(
  metadata: Metadata,
  effect: Effect.Effect<A, E, R>
): Effect.Effect<A, E, R> => Effect.locally(effect, RequestContext, metadata)

/**
 * Get the current request metadata from the fiber.
 *
 * @since 1.0.0
 * @category getters
 */
export const getRequestContext: Effect.Effect<Metadata> =
  FiberRef.get(RequestContext)
