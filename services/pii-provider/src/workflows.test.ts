import { it, expect } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { WorkflowEngine } from "@effect/workflow"
import {
  makePIIProviderEventMessage,
  PIIEventPublishWorkflow,
  PIIProviderEventPublishHandlers,
  PIIProviderEventPublisher,
  startPIIEventPublish
} from "./workflows.js"

const makeRecordId = (seed: number) =>
  `00000000-0000-4000-8000-${seed.toString().padStart(12, "0")}`

it("message builder keeps a stable pii-provider envelope", () => {
  const recordId = makeRecordId(1)
  const message = makePIIProviderEventMessage({
    recordId,
    revision: 2,
    eventType: "PIIRecordUpdated",
    occurredAt: "2026-01-01T00:00:00.000Z",
    payload: { recordId }
  })

  expect(message).toEqual({
    id: `${recordId}:2:PIIRecordUpdated`,
    topic: "pii-provider.events",
    partitionKey: recordId,
    eventType: "PIIRecordUpdated",
    recordId,
    revision: 2,
    occurredAt: "2026-01-01T00:00:00.000Z",
    payload: { recordId },
    headers: {
      eventType: "PIIRecordUpdated",
      recordId,
      revision: 2
    }
  })
})

it.scopedLive("publish workflow is idempotent for the same event id", () =>
  Effect.gen(function* () {
    const published: Array<string> = []
    const message = makePIIProviderEventMessage({
      recordId: makeRecordId(2),
      revision: 2,
      eventType: "PIIRecordCreated",
      occurredAt: "2026-01-01T00:00:00.000Z",
      payload: { seed: 2 }
    })

    const testLayer = Layer.mergeAll(
      Layer.provideMerge(PIIProviderEventPublishHandlers, WorkflowEngine.layerMemory),
      Layer.succeed(PIIProviderEventPublisher, {
        publish: (event) =>
          Effect.sync(() => {
            published.push(event.id)
          })
      })
    )

    yield* Effect.gen(function* () {
      yield* startPIIEventPublish(message)
      yield* startPIIEventPublish(message)
    }).pipe(
      Effect.provide(testLayer)
    )

    expect(published).toEqual([message.id])
  }))

it.scopedLive("start publish returns the workflow-owned execution id", () =>
  Effect.gen(function* () {
    const message = makePIIProviderEventMessage({
      recordId: makeRecordId(4),
      revision: 4,
      eventType: "PIIRecordCreated",
      occurredAt: "2026-01-01T00:00:00.000Z",
      payload: { seed: 4 }
    })

    const testLayer = Layer.mergeAll(
      Layer.provideMerge(PIIProviderEventPublishHandlers, WorkflowEngine.layerMemory),
      Layer.succeed(PIIProviderEventPublisher, {
        publish: (_event) => Effect.void
      })
    )

    const executionIds = yield* Effect.gen(function* () {
      const expected = yield* PIIEventPublishWorkflow.executionId(message)
      const actual = yield* startPIIEventPublish(message)
      return { actual, expected }
    }).pipe(
      Effect.provide(testLayer)
    )

    expect(executionIds.actual).toBe(executionIds.expected)
    expect(executionIds.actual).not.toBe(message.id)
  }))

it.scopedLive("publish workflow retries after a failed publish and completes", () =>
  Effect.gen(function* () {
    let attempts = 0
    const published: Array<string> = []
    const message = makePIIProviderEventMessage({
      recordId: makeRecordId(3),
      revision: 3,
      eventType: "PIIRecordUpdated",
      occurredAt: "2026-01-01T00:00:00.000Z",
      payload: { seed: 3 }
    })

    const testLayer = Layer.mergeAll(
      Layer.provideMerge(PIIProviderEventPublishHandlers, WorkflowEngine.layerMemory),
      Layer.succeed(PIIProviderEventPublisher, {
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

    yield* Effect.gen(function* () {
      yield* PIIEventPublishWorkflow.execute(message)
    }).pipe(
      Effect.provide(testLayer)
    )

    expect(attempts).toBe(2)
    expect(published).toEqual([message.id])
  }))
