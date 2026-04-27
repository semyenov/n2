import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { WorkflowEngine } from "@effect/workflow"
import {
  makeRequestProviderEventMessage,
  RequestEventPublishWorkflow,
  RequestProviderEventPublishHandlers,
  RequestProviderEventPublisher,
  startRequestEventPublish
} from "./workflows.js"

const makeRequestId = (seed: number) =>
  `00000000-0000-4000-8000-${seed.toString().padStart(12, "0")}`

test("message builder keeps a stable request-provider envelope", () => {
  const requestId = makeRequestId(1)
  const message = makeRequestProviderEventMessage({
    requestId,
    revision: 2,
    eventType: "RequestUpdated",
    occurredAt: "2026-01-01T00:00:00.000Z",
    payload: { requestId }
  })

  expect(message).toEqual({
    id: `${requestId}:2:RequestUpdated`,
    topic: "request-provider.events",
    partitionKey: requestId,
    eventType: "RequestUpdated",
    requestId,
    revision: 2,
    occurredAt: "2026-01-01T00:00:00.000Z",
    payload: { requestId },
    headers: {
      eventType: "RequestUpdated",
      requestId,
      revision: 2
    }
  })
})

test("publish workflow is idempotent for the same event id", async () => {
  const published: Array<string> = []
  const message = makeRequestProviderEventMessage({
    requestId: makeRequestId(2),
    revision: 2,
    eventType: "RequestCreated",
    occurredAt: "2026-01-01T00:00:00.000Z",
    payload: { seed: 2 }
  })

  const testLayer = Layer.mergeAll(
    Layer.provideMerge(RequestProviderEventPublishHandlers, WorkflowEngine.layerMemory),
    Layer.succeed(RequestProviderEventPublisher, {
      publish: (event) =>
        Effect.sync(() => {
          published.push(event.id)
        })
    })
  )

  await Effect.gen(function* () {
    yield* startRequestEventPublish(message)
    yield* startRequestEventPublish(message)
  }).pipe(
    Effect.scoped,
    Effect.provide(testLayer)
  ).pipe(
    (effect) => Effect.runPromise(effect as Effect.Effect<void, never, never>)
  )

  expect(published).toEqual([message.id])
})

test("publish workflow retries after a failed publish and completes", async () => {
  let attempts = 0
  const published: Array<string> = []
  const message = makeRequestProviderEventMessage({
    requestId: makeRequestId(3),
    revision: 3,
    eventType: "RequestUpdated",
    occurredAt: "2026-01-01T00:00:00.000Z",
    payload: { seed: 3 }
  })

  const testLayer = Layer.mergeAll(
    Layer.provideMerge(RequestProviderEventPublishHandlers, WorkflowEngine.layerMemory),
    Layer.succeed(RequestProviderEventPublisher, {
      publish: (event) =>
        Effect.sync(() => {
          attempts += 1
          if (attempts === 1) {
            throw new Error("boom")
          }
          published.push(event.id)
        })
    })
  )

  await Effect.gen(function* () {
    const workflowEngine = yield* Effect.orDie(Effect.serviceOptional(WorkflowEngine.WorkflowEngine))
    yield* workflowEngine.execute(RequestEventPublishWorkflow, {
      executionId: message.id,
      payload: message
    })
  }).pipe(
    Effect.scoped,
    Effect.provide(testLayer)
  ).pipe(
    (effect) => Effect.runPromise(effect as Effect.Effect<void, never, never>)
  )

  expect(attempts).toBe(2)
  expect(published).toEqual([message.id])
})
