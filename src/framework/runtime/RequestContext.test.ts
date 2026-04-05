/**
 * RequestContext test: FiberRef metadata propagation.
 */
import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as DateTime from "effect/DateTime"
import { Metadata } from "../contracts/Metadata.js"
import { withRequestContext, getRequestContext } from "./RequestContext.js"

test("RequestContext propagates metadata through nested effects", async () => {
  const testMetadata = new Metadata({
    correlationId: "corr-123",
    causationId: "cause-456",
    actorId: "user-789",
    timestamp: DateTime.unsafeMake(0)
  })

  const result = await Effect.gen(function*() {
    // Default should be empty
    const before = yield* getRequestContext
    expect(before.actorId).toBe("system")

    // Set context and read in nested effect
    return yield* withRequestContext(
      testMetadata,
      Effect.gen(function*() {
        const ctx = yield* getRequestContext
        expect(ctx.correlationId).toBe("corr-123")
        expect(ctx.causationId).toBe("cause-456")
        expect(ctx.actorId).toBe("user-789")
        return ctx
      })
    )
  }).pipe(Effect.runPromise)

  expect(result.correlationId).toBe("corr-123")
})

test("RequestContext is fiber-local (does not leak between fibers)", async () => {
  const meta1 = new Metadata({
    correlationId: "fiber-1",
    causationId: "c",
    actorId: "a",
    timestamp: DateTime.unsafeMake(0)
  })

  await Effect.gen(function*() {
    const fiber1Result = yield* withRequestContext(
      meta1,
      getRequestContext
    ).pipe(Effect.fork, Effect.flatMap((f) => Effect.fromFiber(f)))

    // Outside the withRequestContext scope, should be default
    const outsideResult = yield* getRequestContext

    expect(fiber1Result.correlationId).toBe("fiber-1")
    expect(outsideResult.correlationId).toBe("00000000-0000-0000-0000-000000000000")
  }).pipe(Effect.runPromise)
})
