/**
 * @since 1.0.0
 * @module DeterministicIdGenerator
 *
 * Sequential ID generator for deterministic testing.
 */
import * as Effect from "effect/Effect"
import * as Ref from "effect/Ref"
import * as Layer from "effect/Layer"
import { IdGenerator, type IdGeneratorService } from "../runtime/IdGenerator.js"

/**
 * @since 1.0.0
 * @category constructors
 */
export const make = (
  prefix: string = "test-id"
): Effect.Effect<IdGeneratorService> =>
  Effect.gen(function*() {
    const counter = yield* Ref.make(0)

    const generate: Effect.Effect<string> = Ref.getAndUpdate(
      counter,
      (n) => n + 1
    ).pipe(Effect.map((n) => `${prefix}-${n}`))

    return { generate }
  })

/**
 * @since 1.0.0
 * @category layers
 */
export const layer = (prefix?: string): Layer.Layer<IdGenerator> =>
  Layer.effect(IdGenerator, make(prefix))
