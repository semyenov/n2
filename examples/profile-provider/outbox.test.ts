import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import {
  createOutboxRow,
  makeOutboxMessage,
  OutboxWorkerConfig,
  ProfileProviderOutboxMessage,
  ProfileProviderOutboxPublisher,
  ProfileProviderOutboxStore,
  runProfileProviderOutboxDispatch,
  type ClaimedOutboxMessage,
  type ProfileProviderOutboxRow
} from "./outbox.js"

const decodeMessage = Schema.decodeUnknownSync(ProfileProviderOutboxMessage)

const makeMessage = (seed: number, eventType = "ProfileCreated") =>
  makeOutboxMessage({
    profileId: `00000000-0000-4000-8000-${seed.toString().padStart(12, "0")}`,
    revision: seed,
    eventType,
    occurredAt: "2026-01-01T00:00:00.000Z",
    payload: { seed, eventType }
  })

const makeMemoryOutboxStore = () => {
  const rows = new Map<string, ProfileProviderOutboxRow>()

  const sortedRows = () =>
    Array.from(rows.values()).sort((left, right) =>
      left.availableAt.localeCompare(right.availableAt) || left.createdAt.localeCompare(right.createdAt)
    )

  const api: {
    readonly enqueue: (message: InstanceType<typeof ProfileProviderOutboxMessage>) => Effect.Effect<void>
    readonly claimBatch: (options: {
      readonly workerId: string
      readonly batchSize: number
      readonly leaseDurationMs: number
      readonly now: Date
    }) => Effect.Effect<ReadonlyArray<ClaimedOutboxMessage>>
    readonly markSent: (options: {
      readonly id: string
      readonly workerId: string
      readonly processedAt: Date
    }) => Effect.Effect<void>
    readonly markFailed: (options: {
      readonly id: string
      readonly workerId: string
      readonly failedAt: Date
      readonly availableAt: Date
      readonly error: string
    }) => Effect.Effect<void>
  } = {
    enqueue: (message) =>
      Effect.sync(() => {
        if (!rows.has(message.id)) {
          rows.set(message.id, createOutboxRow(message))
        }
      }),
    claimBatch: ({ workerId, batchSize, leaseDurationMs, now }) =>
      Effect.sync(() => {
        const nowIso = now.toISOString()
        const lockedUntil = new Date(now.getTime() + leaseDurationMs).toISOString()
        const claimable = sortedRows().filter((row) =>
          (((row.status === "PENDING" || row.status === "FAILED") && row.availableAt <= nowIso) ||
            (row.status === "PROCESSING" && row.lockedUntil !== null && row.lockedUntil < nowIso))
        ).slice(0, batchSize)

        for (const row of claimable) {
          rows.set(row.id, {
            ...row,
            status: "PROCESSING",
            workerId,
            lockedUntil,
            lastAttemptAt: nowIso,
            updatedAt: nowIso
          })
        }

        return claimable.map((row) => ({
          id: row.id,
          retryCount: row.retryCount,
          message: decodeMessage(JSON.parse(row.payloadJson))
        }))
      }),
    markSent: ({ id, workerId, processedAt }) =>
      Effect.sync(() => {
        const row = rows.get(id)
        if (!row || row.workerId !== workerId || row.status !== "PROCESSING") return
        rows.set(id, {
          ...row,
          status: "SENT",
          workerId: null,
          lockedUntil: null,
          processedAt: processedAt.toISOString(),
          lastError: "",
          updatedAt: processedAt.toISOString()
        })
      }),
    markFailed: ({ id, workerId, failedAt, availableAt, error }) =>
      Effect.sync(() => {
        const row = rows.get(id)
        if (!row || row.workerId !== workerId || row.status !== "PROCESSING") return
        rows.set(id, {
          ...row,
          status: "FAILED",
          retryCount: row.retryCount + 1,
          lastError: error,
          availableAt: availableAt.toISOString(),
          workerId: null,
          lockedUntil: null,
          processedAt: null,
          updatedAt: failedAt.toISOString()
        })
      })
  }

  return {
    api,
    seed: (row: ProfileProviderOutboxRow) => {
      rows.set(row.id, row)
    },
    snapshot: () => sortedRows()
  }
}

const testConfigLayer = Layer.succeed(OutboxWorkerConfig, {
  batchSize: 50,
  pollIntervalMs: 1,
  leaseDurationMs: 30_000
})

const run = <A, E>(effect: Effect.Effect<A, E, never>): Promise<A> =>
  Effect.runPromise(effect)

test("enqueue stores pending row with stable envelope", async () => {
  const store = makeMemoryOutboxStore()
  const message = makeMessage(1)

  await run(store.api.enqueue(message))

  const row = store.snapshot()[0]
  expect(row?.status).toBe("PENDING")
  expect(row?.retryCount).toBe(0)
  expect(row?.availableAt).toBeTruthy()
  expect(row?.createdAt).toBeTruthy()
  expect(row?.updatedAt).toBeTruthy()
  expect(JSON.parse(row!.payloadJson)).toEqual({
    id: message.id,
    topic: message.topic,
    partitionKey: message.partitionKey,
    eventType: message.eventType,
    profileId: message.profileId,
    revision: message.revision,
    occurredAt: message.occurredAt,
    payload: message.payload,
    headers: message.headers
  })
  expect(JSON.parse(row!.headersJson)).toEqual(message.headers)
})

test("claimBatch only picks ready rows", async () => {
  const store = makeMemoryOutboxStore()
  const ready = createOutboxRow(makeMessage(2), new Date("2026-01-01T00:00:00.000Z"))
  const future = {
    ...createOutboxRow(makeMessage(3), new Date("2026-01-01T00:00:00.000Z")),
    availableAt: "2026-01-01T00:00:10.000Z"
  } satisfies ProfileProviderOutboxRow
  store.seed(ready)
  store.seed(future)

  const claimed = await run(store.api.claimBatch({
    workerId: "worker-1",
    batchSize: 10,
    leaseDurationMs: 30_000,
    now: new Date("2026-01-01T00:00:01.000Z")
  }))

  expect(claimed.map((entry) => entry.id)).toEqual([ready.id])
  expect(store.snapshot().find((row) => row.id === ready.id)?.status).toBe("PROCESSING")
  expect(store.snapshot().find((row) => row.id === future.id)?.status).toBe("PENDING")
})

test("claimBatch reclaims stale processing rows", async () => {
  const store = makeMemoryOutboxStore()
  const stale = {
    ...createOutboxRow(makeMessage(4), new Date("2026-01-01T00:00:00.000Z")),
    status: "PROCESSING" as const,
    workerId: "worker-old",
    lockedUntil: "2026-01-01T00:00:01.000Z",
    lastAttemptAt: "2026-01-01T00:00:00.500Z"
  }
  store.seed(stale)

  const claimed = await run(store.api.claimBatch({
    workerId: "worker-new",
    batchSize: 10,
    leaseDurationMs: 30_000,
    now: new Date("2026-01-01T00:00:02.000Z")
  }))

  expect(claimed).toHaveLength(1)
  expect(store.snapshot()[0]?.workerId).toBe("worker-new")
  expect(store.snapshot()[0]?.status).toBe("PROCESSING")
})

test("dispatcher marks rows as sent after publish", async () => {
  const store = makeMemoryOutboxStore()
  const published: Array<string> = []
  await run(store.api.enqueue(makeMessage(5)))

  const testLayer = Layer.mergeAll(
    Layer.succeed(ProfileProviderOutboxStore, store.api),
    Layer.succeed(ProfileProviderOutboxPublisher, {
      publish: (message) =>
        Effect.sync(() => {
          published.push(message.id)
        })
    }),
    testConfigLayer
  )

  const result = await run(
    runProfileProviderOutboxDispatch({
      workerId: "worker-1",
      now: new Date("2026-01-01T00:00:00.000Z")
    }).pipe(Effect.provide(testLayer)) as Effect.Effect<
      { readonly claimed: number; readonly sent: number; readonly failed: number },
      never,
      never
    >
  )

  expect(result).toEqual({ claimed: 1, sent: 1, failed: 0 })
  expect(published).toEqual([makeMessage(5).id])
  expect(store.snapshot()[0]?.status).toBe("SENT")
  expect(store.snapshot()[0]?.processedAt).toBe("2026-01-01T00:00:00.000Z")
})

test("dispatcher marks failures and schedules retry", async () => {
  const store = makeMemoryOutboxStore()
  const message = makeMessage(6)
  await run(store.api.enqueue(message))

  const testLayer = Layer.mergeAll(
    Layer.succeed(ProfileProviderOutboxStore, store.api),
    Layer.succeed(ProfileProviderOutboxPublisher, {
      publish: () => Effect.fail(new Error("boom"))
    }),
    testConfigLayer
  )

  const result = await run(
    runProfileProviderOutboxDispatch({
      workerId: "worker-1",
      now: new Date("2026-01-01T00:00:00.000Z")
    }).pipe(Effect.provide(testLayer)) as Effect.Effect<
      { readonly claimed: number; readonly sent: number; readonly failed: number },
      never,
      never
    >
  )

  const row = store.snapshot()[0]
  expect(result).toEqual({ claimed: 1, sent: 0, failed: 1 })
  expect(row?.status).toBe("FAILED")
  expect(row?.retryCount).toBe(1)
  expect(row?.availableAt).toBe("2026-01-01T00:00:02.000Z")
  expect(row?.lastError).toContain("boom")
})

test("dispatcher retries after failure and eventually succeeds", async () => {
  const store = makeMemoryOutboxStore()
  const message = makeMessage(7)
  let attempts = 0
  await run(store.api.enqueue(message))

  const testLayer = Layer.mergeAll(
    Layer.succeed(ProfileProviderOutboxStore, store.api),
    Layer.succeed(ProfileProviderOutboxPublisher, {
      publish: () =>
        Effect.sync(() => {
          attempts += 1
          if (attempts === 1) {
            throw new Error("first attempt fails")
          }
        })
    }),
    testConfigLayer
  )

  await run(
    runProfileProviderOutboxDispatch({
      workerId: "worker-1",
      now: new Date("2026-01-01T00:00:00.000Z")
    }).pipe(Effect.provide(testLayer)) as Effect.Effect<unknown, never, never>
  )

  await run(
    runProfileProviderOutboxDispatch({
      workerId: "worker-1",
      now: new Date("2026-01-01T00:00:03.000Z")
    }).pipe(Effect.provide(testLayer)) as Effect.Effect<unknown, never, never>
  )

  expect(attempts).toBe(2)
  expect(store.snapshot()[0]?.status).toBe("SENT")
})

test("two workers do not publish the same row concurrently", async () => {
  const store = makeMemoryOutboxStore()
  const published: Array<string> = []
  const message = makeMessage(8)
  await run(store.api.enqueue(message))

  const testLayer = Layer.mergeAll(
    Layer.succeed(ProfileProviderOutboxStore, store.api),
    Layer.succeed(ProfileProviderOutboxPublisher, {
      publish: (next) =>
        Effect.sleep("20 millis").pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              published.push(next.id)
            })
          )
        )
    }),
    testConfigLayer
  )

  await run(
    Effect.all([
      runProfileProviderOutboxDispatch({
        workerId: "worker-a",
        now: new Date("2026-01-01T00:00:00.000Z")
      }),
      runProfileProviderOutboxDispatch({
        workerId: "worker-b",
        now: new Date("2026-01-01T00:00:00.000Z")
      })
    ], { concurrency: "unbounded" }).pipe(
      Effect.provide(testLayer)
    ) as Effect.Effect<unknown, never, never>
  )

  expect(published).toEqual([message.id])
  expect(store.snapshot()[0]?.status).toBe("SENT")
})
