import type * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type * as Schema from "effect/Schema"
import type { DurationInput } from "effect/Duration"
import {
  makeOutboxJsonService,
  type OutboxEntry,
  type OutboxService
} from "./Outbox.js"
import {
  makeSnapshotOps,
  makeSnapshotService,
  type SnapshotService
} from "./Snapshots.js"

export interface StandardSnapshotWiringConfig<I, State, Encoded> {
  readonly tag: Context.Tag<I, SnapshotService<State>>
  readonly table: string
  readonly stateSchema: Schema.Schema<State, Encoded>
  readonly idColumn?: string
  readonly every: number
  readonly metrics?: { readonly prefix: string }
}

export const makeStandardSnapshotWiring = <I, State, Encoded>(
  config: StandardSnapshotWiringConfig<I, State, Encoded>
) => {
  const snapshots = makeSnapshotService({
    table: config.table,
    stateSchema: config.stateSchema,
    idColumn: config.idColumn,
    metrics: config.metrics
  })

  return {
    live: snapshots.makeLive(config.tag),
    ops: makeSnapshotOps(config.tag, config.every)
  } as const
}

export interface StandardOutboxWiringConfig<I, Message extends { readonly id: string }, Encoded, PublishR> {
  readonly tag: Context.Tag<I, OutboxService<Message>>
  readonly table: string
  readonly schema: Schema.Schema<Message, Encoded>
  readonly idOf?: (message: Message) => string
  readonly publish: (message: Message) => import("effect/Effect").Effect<void, unknown, PublishR>
  readonly metricsPrefix?: string
  readonly batchSize?: number
  readonly maxRetries?: number
  readonly idleDelay?: DurationInput
}

export const makeStandardOutboxWiring = <
  I,
  Message extends { readonly id: string },
  Encoded,
  PublishR
>(
  config: StandardOutboxWiringConfig<I, Message, Encoded, PublishR>
) => {
  const outbox = makeOutboxJsonService({
    table: config.table,
    schema: config.schema,
    idOf: config.idOf ?? ((message) => message.id),
    metrics: config.metricsPrefix ? { prefix: config.metricsPrefix } : undefined
  })
  const batchSize = config.batchSize ?? 50
  const maxRetries = config.maxRetries ?? 5
  const idleDelay = config.idleDelay ?? "1 second"

  return {
    live: outbox.makeLive(config.tag),
    drainOnce: outbox.makeDrainOnce({
      outbox: config.tag,
      publish: config.publish,
      batchSize,
      maxRetries
    }),
    workerLive: outbox.makeWorkerLive({
      outbox: config.tag,
      publish: config.publish,
      batchSize,
      idleDelay,
      maxRetries
    })
  } as const
}

export type { OutboxEntry, OutboxService, SnapshotService }

interface EventPublisher<Message extends {
  readonly id: string
  readonly topic: string
  readonly partitionKey: string
  readonly payload: unknown
}> {
  readonly publish: (message: Message) => Effect.Effect<void, unknown>
}

export const makeConsoleEventPublisherLayer = <
  I,
  Message extends {
    readonly id: string
    readonly topic: string
    readonly partitionKey: string
    readonly payload: unknown
  }
>(
  tag: Context.Tag<I, EventPublisher<Message>>,
  message: string
) =>
  Layer.succeed(
    tag,
    {
      publish: (eventMessage) =>
        Effect.logInfo(message).pipe(
          Effect.annotateLogs({
            eventMessageId: eventMessage.id,
            topic: eventMessage.topic,
            partitionKey: eventMessage.partitionKey,
            payloadSize: JSON.stringify(eventMessage.payload).length
          })
        )
    }
  )
