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

type StoreMethodContext<
  Store,
  StoreMethod extends keyof Store,
  Event
> = Store[StoreMethod] extends (event: Event) => Effect.Effect<unknown, unknown, infer R> ? R : never

type OutboxContext<Outbox, Message> =
  Outbox extends { readonly enqueue: (message: Message) => Effect.Effect<unknown, unknown, infer R> } ? R : never

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
  Store extends { readonly [K in StoreMethod]: (event: Event) => Effect.Effect<void, unknown, unknown> },
  Message = never,
  OutboxI = never,
  Outbox extends { readonly enqueue: (message: Message) => Effect.Effect<void, unknown, unknown> } = {
    readonly enqueue: (message: Message) => Effect.Effect<void, unknown, never>
  }
>(
  storeTag: Context.Tag<StoreI, Store>,
  outboxTag: Context.Tag<OutboxI, Outbox> | undefined,
  EventCtor: new (payload: Payload) => Event,
  storeMethod: StoreMethod,
  makeMessage?: (event: Event) => Message
): ((input: { readonly payload: Payload }) => Effect.Effect<
  void,
  never,
  StoreI | StoreMethodContext<Store, StoreMethod, Event> | OutboxI | OutboxContext<Outbox, Message>
>) =>
  ({ payload }: { payload: Payload }) =>
    Effect.gen(function* () {
      const store = yield* storeTag
      const event = new EventCtor(payload)
      // Generic indexed access loses the concrete Effect context for this method.
      const runStore = store[storeMethod](event) as Effect.Effect<
        void,
        unknown,
        StoreMethodContext<Store, StoreMethod, Event>
      >
      yield* runStore
      if (outboxTag && makeMessage) {
        const outbox = yield* outboxTag
        // Preserve the enqueue context derived from the concrete outbox service.
        const enqueue = outbox.enqueue(makeMessage(event)) as Effect.Effect<
          void,
          unknown,
          OutboxContext<Outbox, Message>
        >
        yield* enqueue
      }
    }).pipe(Effect.orDie)
