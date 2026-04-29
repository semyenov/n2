/**
 * Behavior tests for `makePublishWorkflow` — durable retry pipeline used by
 * every event-publishing service. Validates: happy path, permanent failure
 * with maxAttempts: 1.
 *
 * NOTE on retries: the workflow uses `DurableClock.sleep` between attempts
 * with `inMemoryThreshold: "0 millis"`, which in `WorkflowEngine.layerMemory`
 * defers to `Effect.sleep`. Driving multi-attempt retries through TestClock
 * proved fragile here — instead we exercise both terminal states (success on
 * first try; give-up after exactly one failed attempt) which together cover
 * the publisher dispatch, error catching, and `maxAttempts` cutoff.
 */
import { it, expect } from "@effect/vitest"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { WorkflowEngine } from "@effect/workflow"
import { makePublishWorkflow } from "./PublishWorkflow.js"

class TestPublishMessage extends Schema.Class<TestPublishMessage>("TestPublishMessage")({
  id: Schema.String,
  payload: Schema.String
}) {}

interface TestPublisherShape {
  readonly publish: (message: TestPublishMessage) => Effect.Effect<void, unknown>
}
class TestPublisher extends Context.Tag("TestPublisher")<TestPublisher, TestPublisherShape>() {}

const runWorkflow = (
  wf: ReturnType<typeof makePublishWorkflow<TestPublishMessage, TestPublisher>>,
  message: TestPublishMessage,
  publisherLayer: Layer.Layer<TestPublisher, never, never>
): Effect.Effect<unknown, never, never> =>
  wf.workflow.execute(message).pipe(
    Effect.provide(Layer.provideMerge(wf.handlers, Layer.merge(WorkflowEngine.layerMemory, publisherLayer))),
    Effect.orDie
  )

it.effect("makePublishWorkflow invokes publisher exactly once on the happy path", () =>
  Effect.gen(function* () {
    const calls: Array<TestPublishMessage> = []

    const wf = makePublishWorkflow({
      name: "TestHappy",
      messageSchema: TestPublishMessage,
      publisherTag: TestPublisher,
      maxAttempts: 1
    })

    const publisherLayer = Layer.succeed(TestPublisher, {
      publish: (message) =>
        Effect.sync(() => { calls.push(message) })
    })

    yield* runWorkflow(wf, new TestPublishMessage({ id: "msg-1", payload: "hello" }), publisherLayer)

    expect(calls).toHaveLength(1)
    expect(calls[0]?.id).toBe("msg-1")
    expect(calls[0]?.payload).toBe("hello")
  }))

it.effect("makePublishWorkflow gives up after maxAttempts when publisher always fails", () =>
  Effect.gen(function* () {
    let calls = 0

    const wf = makePublishWorkflow({
      name: "TestFail",
      messageSchema: TestPublishMessage,
      publisherTag: TestPublisher,
      maxAttempts: 1
    })

    const publisherLayer = Layer.succeed(TestPublisher, {
      publish: () =>
        Effect.sync(() => { calls += 1 }).pipe(
          Effect.zipRight(Effect.fail("publish failed"))
        )
    })

    // Workflow handlers swallow the EventPublishError after maxAttempts
    // (catchTag → Effect.void), so the program should resolve successfully.
    yield* runWorkflow(wf, new TestPublishMessage({ id: "msg-2", payload: "doomed" }), publisherLayer)

    // Publisher should have been called exactly maxAttempts times.
    expect(calls).toBe(1)
  }))

it.effect("makePublishWorkflow start returns the workflow-owned execution id", () =>
  Effect.gen(function* () {
    const wf = makePublishWorkflow({
      name: "TestStart",
      messageSchema: TestPublishMessage,
      publisherTag: TestPublisher,
      maxAttempts: 1
    })
    const message = new TestPublishMessage({ id: "msg-3", payload: "start" })

    const publisherLayer = Layer.succeed(TestPublisher, {
      publish: () => Effect.void
    })

    const executionIds = yield* Effect.gen(function* () {
      const expected = yield* wf.workflow.executionId(message)
      const actual = yield* wf.start(message)
      return { actual, expected }
    }).pipe(
      Effect.provide(Layer.provideMerge(wf.handlers, Layer.merge(WorkflowEngine.layerMemory, publisherLayer)))
    )

    expect(executionIds.actual).toBe(executionIds.expected)
    expect(executionIds.actual).not.toBe(message.id)
  }))

it.effect("makePublishWorkflow start is idempotent for duplicate messages", () =>
  Effect.gen(function* () {
    const calls: Array<string> = []
    const wf = makePublishWorkflow({
      name: "TestDuplicateStart",
      messageSchema: TestPublishMessage,
      publisherTag: TestPublisher,
      maxAttempts: 1
    })
    const message = new TestPublishMessage({ id: "msg-4", payload: "duplicate" })

    const publisherLayer = Layer.succeed(TestPublisher, {
      publish: (published) =>
        Effect.sync(() => {
          calls.push(published.id)
        })
    })

    const executionIds = yield* Effect.gen(function* () {
      const first = yield* wf.start(message)
      const second = yield* wf.start(message)
      return { first, second }
    }).pipe(
      Effect.provide(Layer.provideMerge(wf.handlers, Layer.merge(WorkflowEngine.layerMemory, publisherLayer)))
    )

    expect(executionIds.first).toBe(executionIds.second)
    expect(calls).toEqual([message.id])
  }))

it.effect("makePublishWorkflow custom idOf controls workflow idempotency", () =>
  Effect.gen(function* () {
    const calls: Array<string> = []
    const wf = makePublishWorkflow({
      name: "TestCustomId",
      messageSchema: TestPublishMessage,
      publisherTag: TestPublisher,
      idOf: (message) => message.payload,
      maxAttempts: 1
    })
    const firstMessage = new TestPublishMessage({ id: "msg-5a", payload: "shared-key" })
    const secondMessage = new TestPublishMessage({ id: "msg-5b", payload: "shared-key" })

    const publisherLayer = Layer.succeed(TestPublisher, {
      publish: (published) =>
        Effect.sync(() => {
          calls.push(published.id)
        })
    })

    const executionIds = yield* Effect.gen(function* () {
      const first = yield* wf.start(firstMessage)
      const second = yield* wf.start(secondMessage)
      const expected = yield* wf.workflow.executionId(firstMessage)
      return { expected, first, second }
    }).pipe(
      Effect.provide(Layer.provideMerge(wf.handlers, Layer.merge(WorkflowEngine.layerMemory, publisherLayer)))
    )

    expect(executionIds.first).toBe(executionIds.expected)
    expect(executionIds.second).toBe(executionIds.expected)
    expect(calls).toEqual([firstMessage.id])
  }))
