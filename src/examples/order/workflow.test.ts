/**
 * Workflow integration test.
 *
 * Uses WorkflowEngine.layerMemory for in-memory workflow execution.
 * Tests a simplified workflow without DurableClock.sleep (which waits real time).
 */
import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { Workflow, Activity, WorkflowEngine } from "@effect/workflow"

// Test-specific workflow without DurableClock.sleep
class TestFulfillmentError extends Schema.TaggedError<TestFulfillmentError>()(
  "TestFulfillmentError",
  { message: Schema.String }
) {}

const TestWorkflow = Workflow.make({
  name: "TestFulfillment",
  payload: { orderId: Schema.String },
  success: Schema.Struct({ orderId: Schema.String, shipped: Schema.Boolean }),
  error: TestFulfillmentError,
  idempotencyKey: (p) => p.orderId
})

const StepA = Activity.make({
  name: "StepA",
  error: TestFulfillmentError,
  execute: Effect.succeed({ done: true })
})

const StepB = Activity.make({
  name: "StepB",
  error: TestFulfillmentError,
  execute: Effect.succeed({ done: true })
})

const TestHandlers = TestWorkflow.toLayer(
  (payload, _executionId) =>
    Effect.gen(function*() {
      yield* StepA
      yield* StepB
      return { orderId: payload.orderId, shipped: true as const }
    })
)

const TestLayer = Layer.provideMerge(TestHandlers, WorkflowEngine.layerMemory)

test("workflow: executes activities and returns result", async () => {
  await Effect.gen(function*() {
    const result = yield* TestWorkflow.execute({ orderId: "wf-1" })
    expect(result.orderId).toBe("wf-1")
    expect(result.shipped).toBe(true)
  }).pipe(
    Effect.scoped,
    Effect.provide(TestLayer),
    Effect.runPromise
  )
})

test("workflow: idempotent execution", async () => {
  await Effect.gen(function*() {
    const r1 = yield* TestWorkflow.execute({ orderId: "wf-2" })
    const r2 = yield* TestWorkflow.execute({ orderId: "wf-2" })
    expect(r1.orderId).toBe(r2.orderId)
  }).pipe(
    Effect.scoped,
    Effect.provide(TestLayer),
    Effect.runPromise
  )
})

// Test compensation on failure
const FailingStep = Activity.make({
  name: "FailingStep",
  error: TestFulfillmentError,
  execute: Effect.fail(new TestFulfillmentError({ message: "boom" }))
})

const CompensationWorkflow = Workflow.make({
  name: "CompensationTest",
  payload: { id: Schema.String },
  success: Schema.Void,
  error: TestFulfillmentError,
  idempotencyKey: (p) => p.id
})

const compensated: string[] = []

const CompensationHandlers = CompensationWorkflow.toLayer(
  (payload, _executionId) =>
    Effect.gen(function*() {
      yield* StepA.pipe(
        CompensationWorkflow.withCompensation(() =>
          Effect.sync(() => { compensated.push("StepA") })
        )
      )
      yield* FailingStep // this will fail and trigger compensation
    })
)

const CompensationLayer = Layer.provideMerge(
  CompensationHandlers,
  WorkflowEngine.layerMemory
)

test("workflow: compensation runs on failure", async () => {
  compensated.length = 0
  const result = await Effect.gen(function*() {
    return yield* CompensationWorkflow.execute({ id: "comp-1" }).pipe(Effect.either)
  }).pipe(
    Effect.scoped,
    Effect.provide(CompensationLayer),
    Effect.runPromise
  )

  expect(result._tag).toBe("Left")
  expect(compensated).toContain("StepA")
})
