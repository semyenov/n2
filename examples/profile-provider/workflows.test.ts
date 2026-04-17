import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { WorkflowEngine } from "@effect/workflow"
import {
  makeProfileProviderEventMessage,
  ProfileEventPublishWorkflow,
  ProfileProviderEventPublishHandlers,
  ProfileProviderEventPublisher,
  startProfileEventPublish
} from "./workflows.js"

const makeMessage = (seed: number, eventType = "ProfileCreated") =>
  makeProfileProviderEventMessage({
    profileId: `00000000-0000-4000-8000-${seed.toString().padStart(12, "0")}`,
    revision: seed,
    eventType,
    occurredAt: "2026-01-01T00:00:00.000Z",
    payload: { seed, eventType }
  })

test("message builder keeps a stable transport envelope", () => {
  const message = makeMessage(1)

  expect(message).toEqual({
    id: "00000000-0000-4000-8000-000000000001:1:ProfileCreated",
    topic: "profile-provider.events",
    partitionKey: "00000000-0000-4000-8000-000000000001",
    eventType: "ProfileCreated",
    profileId: "00000000-0000-4000-8000-000000000001",
    revision: 1,
    occurredAt: "2026-01-01T00:00:00.000Z",
    payload: { seed: 1, eventType: "ProfileCreated" },
    headers: {
      eventType: "ProfileCreated",
      profileId: "00000000-0000-4000-8000-000000000001",
      revision: 1
    }
  })
})

test("publish workflow is idempotent for the same event id", async () => {
  const published: Array<string> = []
  const message = makeMessage(2)

  const testLayer = Layer.mergeAll(
    Layer.provideMerge(ProfileProviderEventPublishHandlers, WorkflowEngine.layerMemory),
    Layer.succeed(ProfileProviderEventPublisher, {
      publish: (event) =>
        Effect.sync(() => {
          published.push(event.id)
        })
    })
  )

  await Effect.gen(function* () {
    yield* startProfileEventPublish(message)
    yield* startProfileEventPublish(message)
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
  const message = makeMessage(3)

  const testLayer = Layer.mergeAll(
    Layer.provideMerge(ProfileProviderEventPublishHandlers, WorkflowEngine.layerMemory),
    Layer.succeed(ProfileProviderEventPublisher, {
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
    yield* workflowEngine.execute(ProfileEventPublishWorkflow, {
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
