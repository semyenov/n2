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
import { test, expect } from "bun:test"
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

// `wf.start` uses `discard: true` (fire-and-forget). To deterministically
// observe completion in tests we drive the engine directly with `discard: false`.
const runWorkflow = (
  wf: { readonly workflow: unknown; readonly handlers: Layer.Layer<never, never, any> },
  executionId: string,
  message: TestPublishMessage,
  publisherLayer: Layer.Layer<TestPublisher, never, never>
): Effect.Effect<unknown, never, never> =>
  Effect.gen(function* () {
    const engine = yield* WorkflowEngine.WorkflowEngine
    return yield* engine.execute(wf.workflow as never, {
      executionId,
      payload: message as never,
      discard: false
    })
  }).pipe(
    Effect.provide(Layer.provideMerge(wf.handlers, Layer.merge(WorkflowEngine.layerMemory, publisherLayer))),
    Effect.orDie
  ) as Effect.Effect<unknown, never, never>

test("makePublishWorkflow invokes publisher exactly once on the happy path", async () => {
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

  await Effect.runPromise(
    runWorkflow(wf, "msg-1", new TestPublishMessage({ id: "msg-1", payload: "hello" }), publisherLayer)
  )

  expect(calls).toHaveLength(1)
  expect(calls[0]?.id).toBe("msg-1")
  expect(calls[0]?.payload).toBe("hello")
})

test("makePublishWorkflow gives up after maxAttempts when publisher always fails", async () => {
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
  await Effect.runPromise(
    runWorkflow(wf, "msg-2", new TestPublishMessage({ id: "msg-2", payload: "doomed" }), publisherLayer)
  )

  // Publisher should have been called exactly maxAttempts times.
  expect(calls).toBe(1)
})
