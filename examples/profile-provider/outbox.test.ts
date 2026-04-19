import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { WorkflowEngine } from "@effect/workflow"
import {
  drainProfileProviderOutboxOnce,
  ProfileProviderOutbox,
  type OutboxEntry
} from "./outbox.js"
import {
  makeProfileProviderEventMessage,
  ProfileProviderEventPublishHandlers,
  ProfileProviderEventPublisher
} from "./workflows.js"

const makeEntry = (seed: number): OutboxEntry => {
  const message = makeProfileProviderEventMessage({
    profileId: `00000000-0000-4000-8000-${seed.toString().padStart(12, "0")}`,
    revision: seed,
    eventType: "ProfileCreated",
    occurredAt: "2026-01-01T00:00:00.000Z",
    payload: { seed }
  })

  return {
    id: message.id,
    message,
    retryCount: 0
  }
}

test("outbox drain dispatches claimed events and marks them dispatched", async () => {
  const entry = makeEntry(1)
  const dispatched: Array<string> = []
  const published: Array<string> = []

  const layer = Layer.mergeAll(
    Layer.provideMerge(ProfileProviderEventPublishHandlers, WorkflowEngine.layerMemory),
    Layer.succeed(ProfileProviderEventPublisher, {
      publish: (message) =>
        Effect.sync(() => {
          published.push(message.id)
        })
    }),
    Layer.succeed(ProfileProviderOutbox, {
      enqueue: (_message) => Effect.void,
      claimPending: (_limit) => Effect.succeed([entry]),
      markDispatched: (id) =>
        Effect.sync(() => {
          dispatched.push(id)
        }),
      markFailed: (_id, _retryCount, _error) => Effect.void
    })
  )

  const result = await Effect.runPromise(
    Effect.scoped(drainProfileProviderOutboxOnce).pipe(
      Effect.provide(layer)
    ) as Effect.Effect<boolean, never, never>
  )

  expect(result).toBe(true)
  expect(published).toEqual([entry.id])
  expect(dispatched).toEqual([entry.id])
})

test("outbox drain marks failures when workflow execution cannot start", async () => {
  const entry = makeEntry(2)
  const failures: Array<{ id: string; retryCount: number; error: string }> = []

  const layer = Layer.succeed(ProfileProviderOutbox, {
    enqueue: (_message) => Effect.void,
    claimPending: (_limit) => Effect.succeed([entry]),
    markDispatched: (_id) => Effect.void,
    markFailed: (id, retryCount, error) =>
      Effect.sync(() => {
        failures.push({ id, retryCount, error })
      })
  })

  const result = await Effect.runPromise(
    Effect.scoped(drainProfileProviderOutboxOnce).pipe(
      Effect.provide(layer)
    ) as Effect.Effect<boolean, never, never>
  )

  expect(result).toBe(true)
  expect(failures).toHaveLength(1)
  expect(failures[0]?.id).toBe(entry.id)
  expect(failures[0]?.retryCount).toBe(1)
})

test("outbox drain reports no work when queue is empty", async () => {
  const layer = Layer.succeed(ProfileProviderOutbox, {
    enqueue: (_message) => Effect.void,
    claimPending: (_limit) => Effect.succeed([]),
    markDispatched: (_id) => Effect.void,
    markFailed: (_id, _retryCount, _error) => Effect.void
  })

  const result = await Effect.runPromise(
    Effect.scoped(drainProfileProviderOutboxOnce).pipe(
      Effect.provide(layer)
    ) as Effect.Effect<boolean, never, never>
  )

  expect(result).toBe(false)
})
