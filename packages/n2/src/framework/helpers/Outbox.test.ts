/**
 * Framework-level unit tests for Outbox helpers.
 */
import { it, expect } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import { ParseError } from "effect/ParseResult"
import * as Schema from "effect/Schema"
import { SqlClient, type SqlClient as SqlClientInstance } from "@effect/sql/SqlClient"
import { computeRetryDelaySeconds } from "./Outbox.js"
import { makeOutboxJsonService, type OutboxService } from "./Outbox.js"

class TestMessage extends Schema.Class<TestMessage>("TestMessage")({
  id: Schema.String,
  count: Schema.Number
}) {}

class TestOutbox extends Context.Tag("TestOutbox")<
  TestOutbox,
  OutboxService<TestMessage>
>() {}

it("computeRetryDelaySeconds uses exponential backoff", () => {
  expect(computeRetryDelaySeconds(0)).toBe(1)
  expect(computeRetryDelaySeconds(1)).toBe(2)
  expect(computeRetryDelaySeconds(2)).toBe(4)
  expect(computeRetryDelaySeconds(3)).toBe(8)
  expect(computeRetryDelaySeconds(4)).toBe(16)
})

it("computeRetryDelaySeconds caps at 300 seconds", () => {
  expect(computeRetryDelaySeconds(9)).toBe(300) // 2^9 = 512 > 300
  expect(computeRetryDelaySeconds(10)).toBe(300)
  expect(computeRetryDelaySeconds(20)).toBe(300)
})

it("computeRetryDelaySeconds handles edge case at boundary", () => {
  // 2^8 = 256, which is under 300
  expect(computeRetryDelaySeconds(8)).toBe(256)
})

it.effect("makeOutboxJsonService encodes and decodes schema messages", () => Effect.gen(function* () {
  let payloadJson = ""
  let rows: ReadonlyArray<{ readonly id: string; readonly payload_json: string; readonly retry_count: number }> = []

  type FakeSql = {
    (strings: TemplateStringsArray | string, ...params: ReadonlyArray<unknown>): unknown
    withTransaction: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
  }

  const fakeSql = Object.assign(((strings: TemplateStringsArray | string, ...params: ReadonlyArray<unknown>) => {
    if (typeof strings === "string") return { identifier: strings }

    const text = strings.join("?")
    if (text.includes("INSERT INTO")) {
      payloadJson = String(params[2])
      return Effect.succeed([])
    }
    if (text.includes("RETURNING id, payload_json, retry_count")) {
      return Effect.succeed(rows)
    }
    return Effect.succeed([])
  }) as FakeSql, {
    withTransaction: <A, E, R>(effect: Effect.Effect<A, E, R>) => effect
  })

  const outbox = makeOutboxJsonService({
    table: "test_outbox",
    schema: TestMessage,
    idOf: (message: TestMessage) => message.id
  })

  const layer = Layer.provide(
    outbox.makeLive(TestOutbox),
    Layer.succeed(SqlClient, fakeSql as unknown as SqlClientInstance)
  )

  const claimed = yield* Effect.gen(function* () {
    const service = yield* TestOutbox
    yield* service.enqueue(new TestMessage({ id: "msg-1", count: 2 }))

    rows = [{ id: "msg-1", payload_json: payloadJson, retry_count: 3 }]
    return yield* service.claimPending(1)
  }).pipe(
    Effect.provide(layer),
    Effect.orDie
  )

  expect(JSON.parse(payloadJson)).toEqual({ id: "msg-1", count: 2 })
  expect(claimed[0]?.message).toBeInstanceOf(TestMessage)
  expect(claimed[0]?.message.count).toBe(2)
  expect(claimed[0]?.retryCount).toBe(3)
}))

it.effect("claimPending surfaces ParseError as a typed failure on malformed payload_json", () => Effect.gen(function* () {
  type FakeSql = {
    (strings: TemplateStringsArray | string, ...params: ReadonlyArray<unknown>): unknown
    withTransaction: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
  }

  const malformed = JSON.stringify({ id: "msg-bad", count: "not-a-number" })

  const fakeSql = Object.assign(((strings: TemplateStringsArray | string, ..._params: ReadonlyArray<unknown>) => {
    if (typeof strings === "string") return { identifier: strings }
    const text = strings.join("?")
    if (text.includes("RETURNING id, payload_json, retry_count")) {
      return Effect.succeed([{ id: "msg-bad", payload_json: malformed, retry_count: 0 }])
    }
    return Effect.succeed([])
  }) as FakeSql, {
    withTransaction: <A, E, R>(effect: Effect.Effect<A, E, R>) => effect
  })

  const outbox = makeOutboxJsonService({
    table: "test_outbox",
    schema: TestMessage,
    idOf: (message: TestMessage) => message.id
  })

  const layer = Layer.provide(
    outbox.makeLive(TestOutbox),
    Layer.succeed(SqlClient, fakeSql as unknown as SqlClientInstance)
  )

  const exit = yield* Effect.gen(function* () {
    const service = yield* TestOutbox
    return yield* service.claimPending(1)
  }).pipe(
    Effect.provide(layer),
    Effect.exit
  )

  expect(Exit.isFailure(exit)).toBe(true)
  if (Exit.isFailure(exit)) {
    const failure = Cause.failureOption(exit.cause)
    expect(failure._tag).toBe("Some")
    if (failure._tag === "Some") {
      // ParseError surfaces as a typed failure, not a defect.
      expect(failure.value).toBeInstanceOf(ParseError)
    }
    // Confirm it isn't a defect (which would mean the throw escaped Effect's machinery).
    expect(Cause.defects(exit.cause).length).toBe(0)
  }
}))
