/**
 * MessageAdapter test: Kafka command bus pattern.
 */
import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import * as Queue from "effect/Queue"
import * as Fiber from "effect/Fiber"
import * as Stream from "effect/Stream"
import * as Context from "effect/Context"
import * as MessageAdapter from "./MessageAdapter.js"
import { KafkaConsumer, KafkaMessage, type KafkaConsumerService, type KafkaConsumerError } from "../runtime/KafkaConsumer.js"

test("MessageAdapter dispatches commands from Kafka", async () => {
  await Effect.gen(function*() {
    const handled = yield* Ref.make<ReadonlyArray<string>>([])
    const inbox = yield* Queue.unbounded<KafkaMessage>()

    // Create a consumer that reads from our queue
    const testConsumer: KafkaConsumerService = {
      subscribe: () => Stream.fromQueue(inbox) as Stream.Stream<KafkaMessage, KafkaConsumerError>,
      commit: () => Effect.void,
      seek: () => Effect.void
    }

    const testLayer = Layer.succeed(KafkaConsumer, testConsumer)

    const fiber = yield* MessageAdapter.make({
      topic: "n2.commands.Order",
      groupId: "order-commands",
      handlers: {
        CreateOrder: (payload) =>
          Ref.update(handled, (h) => [
            ...h,
            `CreateOrder:${(payload as { orderId: string }).orderId}`
          ]).pipe(Effect.map(() => undefined)),
      }
    }).pipe(Effect.provide(testLayer), Effect.fork)

    // Push a command message
    yield* Queue.offer(inbox, new KafkaMessage({
      topic: "n2.commands.Order",
      partition: 0,
      offset: "1",
      key: "order-1",
      value: JSON.stringify({ _tag: "CreateOrder", payload: { orderId: "order-1" } }),
      headers: {},
      timestamp: "0"
    }))

    yield* Effect.sleep("200 millis")
    yield* Fiber.interrupt(fiber)

    const result = yield* Ref.get(handled)
    expect(result).toContain("CreateOrder:order-1")
  }).pipe(
    Effect.scoped,
    Effect.runPromise
  )
})
