/**
 * @since 1.0.0
 * @module Shutdown
 *
 * Graceful shutdown helpers. Handles SIGTERM/SIGINT, drains consumers,
 * flushes producers, and closes connections before exit.
 *
 * Usage:
 * ```ts
 * const main = myApp.pipe(
 *   Effect.provide(MyLayers),
 *   Shutdown.withGracefulShutdown
 * )
 * Effect.runFork(main)
 * ```
 */
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Runtime from "effect/Runtime"
import * as Layer from "effect/Layer"
import * as Scope from "effect/Scope"

/**
 * Wraps a long-running effect with SIGTERM/SIGINT signal handling.
 * On signal, interrupts the fiber gracefully (runs all finalizers).
 *
 * @since 1.0.0
 * @category combinators
 */
export const withGracefulShutdown = <A, E, R>(
  effect: Effect.Effect<A, E, R>
): Effect.Effect<A, E, R> =>
  Effect.gen(function*() {
    const fiber = yield* Effect.fork(effect)

    yield* Effect.addFinalizer(() =>
      Effect.log("Graceful shutdown: running finalizers...")
    )

    // Listen for signals
    const onSignal = (signal: string) =>
      Effect.gen(function*() {
        yield* Effect.log(`Received ${signal}, initiating graceful shutdown...`)
        yield* Fiber.interrupt(fiber)
      })

    yield* Effect.async<never>((resume) => {
      const handler = (signal: string) => () => {
        Effect.runFork(onSignal(signal))
      }
      process.once("SIGTERM", handler("SIGTERM"))
      process.once("SIGINT", handler("SIGINT"))
    }).pipe(
      Effect.race(Fiber.join(fiber))
    )

    return yield* Fiber.join(fiber)
  }).pipe(Effect.scoped)

/**
 * Creates a Layer finalizer that logs and cleans up.
 * Use inside Layer.scoped or Layer.effect to add cleanup logic.
 *
 * @since 1.0.0
 * @category combinators
 */
export const addShutdownHook = (name: string, cleanup: Effect.Effect<void>): Effect.Effect<void, never, Scope.Scope> =>
  Effect.addFinalizer(() =>
    Effect.gen(function*() {
      yield* Effect.log(`Shutting down: ${name}`)
      yield* cleanup.pipe(
        Effect.timeout("10 seconds"),
        Effect.catchAll(() => Effect.log(`Shutdown timeout: ${name}`))
      )
      yield* Effect.log(`Shutdown complete: ${name}`)
    })
  )
