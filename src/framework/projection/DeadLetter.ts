/**
 * @since 1.0.0
 * @module DeadLetter
 *
 * Dead-letter queue service for failed event processing.
 * Backed by @effect/experimental PersistedQueue when available.
 */
import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import type * as Stream from "effect/Stream"
import type { DLQEnvelope } from "../contracts/DLQEnvelope.js"

/**
 * @since 1.0.0
 * @category models
 */
export interface DeadLetterService {
  readonly send: (envelope: DLQEnvelope) => Effect.Effect<void>
  readonly consume: (projectorName: string) => Stream.Stream<DLQEnvelope>
  readonly retry: (envelope: DLQEnvelope) => Effect.Effect<void>
}

/**
 * @since 1.0.0
 * @category tags
 */
export class DeadLetter extends Context.Tag("n2/DeadLetter")<
  DeadLetter,
  DeadLetterService
>() {}
