/**
 * @since 1.0.0
 *
 * Helper for wiring projection handlers that follow the common pattern:
 * construct event → call store method → enqueue outbox message.
 *
 * This is a convenience for the most common projection pattern. For custom
 * projection logic, use `EventLog.group` directly with manual `.handle()` calls.
 *
 * @example
 * ```ts
 * import { wireProjectionHandler } from "@semyenov/n2/framework/helpers"
 *
 * export const MyProjectionLayer = EventLog.group(
 *   MyEventGroup,
 *   (handlers) =>
 *     handlers
 *       .handle("OrderCreated", wireProjectionHandler(MyStore, MyOutbox, OrderCreated, "onOrderCreated", makeMessage))
 *       .handle("ItemAdded", wireProjectionHandler(MyStore, MyOutbox, ItemAdded, "onItemAdded", makeMessage))
 * )
 * ```
 */
import type * as Context from "effect/Context"
import * as Effect from "effect/Effect"

/**
 * Create a projection handler function for use with `EventLog.group(...).handle()`.
 *
 * @param storeTag - Context.Tag for the projection store service
 * @param outboxTag - Context.Tag for the outbox service (optional — pass `undefined` to skip outbox)
 * @param EventCtor - Event constructor class (e.g., `ProfileCreated`)
 * @param storeMethod - Method name on the store to call (e.g., `"onProfileCreated"`)
 * @param makeMessage - Function to create an outbox message from the event (only needed when outboxTag is provided)
 */
export const wireProjectionHandler = <
  StoreI,
  Event extends { readonly _tag: string },
  Payload,
  StoreMethod extends string,
  StoreR,
  Store extends { readonly [K in StoreMethod]: (event: Event) => Effect.Effect<void, unknown, StoreR> },
  Message = never,
  OutboxR = never,
  OutboxI = never,
  Outbox extends { readonly enqueue: (message: Message) => Effect.Effect<void, unknown, OutboxR> } = {
    readonly enqueue: (message: Message) => Effect.Effect<void, unknown, OutboxR>
  }
>(
  storeTag: Context.Tag<StoreI, Store>,
  outboxTag: Context.Tag<OutboxI, Outbox> | undefined,
  EventCtor: new (payload: Payload) => Event,
  storeMethod: StoreMethod,
  makeMessage?: (event: Event) => Message
) =>
  ({ payload }: { payload: Payload }) =>
    Effect.gen(function* () {
      const store = yield* storeTag
      const event = new EventCtor(payload)
      yield* store[storeMethod](event)
      if (outboxTag && makeMessage) {
        const outbox = yield* outboxTag
        yield* outbox.enqueue(makeMessage(event))
      }
    }).pipe(Effect.orDie)
