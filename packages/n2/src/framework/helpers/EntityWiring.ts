import * as Effect from "effect/Effect"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as EventLogApi from "@effect/experimental/EventLog"
import * as EventJournalApi from "@effect/experimental/EventJournal"
import type * as Context from "effect/Context"

/**
 * Map-based dedupe by a key function. Later entries overwrite earlier ones.
 * Used to merge incoming source assets into existing state in `postHandle`.
 */
export const mergeAssetsByKey = <Asset>(keyOf: (asset: Asset) => string) =>
(current: ReadonlyArray<Asset>, incoming: ReadonlyArray<Asset>): ReadonlyArray<Asset> => {
  const next = new Map<string, Asset>(current.map((a) => [keyOf(a), a]))
  for (const asset of incoming) {
    next.set(keyOf(asset), asset)
  }
  return Array.from(next.values())
}

/**
 * Build a `(error: unknown) => DomainError` wrapper that preserves an existing
 * domain error instance and wraps anything else with `String(error)` as the message.
 */
export const makeToError = <E>(
  ErrorCtor: new (props: { message: string }) => E
) =>
(error: unknown): E =>
  error instanceof ErrorCtor ? error : new ErrorCtor({ message: String(error) })

/**
 * Standard publish retry: exponential backoff (100ms base), jittered, capped at 3 retries.
 * Use as the default for `afterCommit` event publishing.
 */
export const standardPublishRetry = Schedule.exponential("100 millis").pipe(
  Schedule.jittered,
  Schedule.intersect(Schedule.recurs(3))
)

export interface StateOverrideConfig<State, Out, NotFound> {
  readonly isEmpty: (state: State) => boolean
  readonly notFound: (entityId: string) => NotFound
  readonly project?: (state: State) => Out
}

export interface StateOverrideContext<State, R = unknown> {
  readonly getState: (entityId: string) => Effect.Effect<State, never, R>
}

/**
 * Build a read-query override that fetches state via `ctx.getState`, fails with
 * `notFound(entityId)` if the state `isEmpty`, and otherwise either returns it
 * as-is or projects it via `project`.
 *
 * Pass `entityIdOf` matching how the entity was registered (e.g., `(c) => c.profileId`).
 */
export const makeStateOverride = <Command, State, Out, NotFound>(
  entityIdOf: (command: Command) => string,
  config: StateOverrideConfig<State, Out, NotFound>
) =>
<R = unknown>(command: Command, ctx: StateOverrideContext<State, R>): Effect.Effect<Out, NotFound, R> =>
  ctx.getState(entityIdOf(command)).pipe(
    Effect.flatMap((state) =>
      config.isEmpty(state)
        ? Effect.fail(config.notFound(entityIdOf(command)))
        : Effect.succeed(
          (config.project ? config.project(state) : (state as unknown as Out))
        )
    )
  )

export interface AfterCommitPublisherConfig<EventLogSchema> {
  readonly schema: EventLogSchema
  readonly logPrefix: string
  readonly retry?: Schedule.Schedule<unknown, unknown>
}

type WriteThroughProjectionStore<Event> = {
  readonly dispatch: (event: Event) => Effect.Effect<void, unknown, unknown>
}

type WriteThroughOutbox<Message> = {
  readonly enqueue: (message: Message) => Effect.Effect<void, unknown, unknown>
}

type ProjectionStoreContext<Store, Event> =
  Store extends { readonly dispatch: (event: Event) => Effect.Effect<void, unknown, infer R> }
    ? R
    : never

type OutboxContext<Outbox, Message> =
  Outbox extends { readonly enqueue: (message: Message) => Effect.Effect<void, unknown, infer R> }
    ? R
    : never

type RuntimeEventDefinition<Event> = {
  readonly primaryKey: (payload: Event) => string
  readonly payloadMsgPack: Schema.Schema.Any
}

type WriteThroughEventGroup = {
  readonly events: Readonly<Record<string, unknown>>
}

export interface WriteThroughAfterCommitPublisherConfig<
  Event extends { readonly _tag: string },
  StoreI,
  Store extends WriteThroughProjectionStore<Event>,
  Message = never,
  OutboxI = never,
  Outbox extends WriteThroughOutbox<Message> = WriteThroughOutbox<Message>
> {
  readonly group: WriteThroughEventGroup
  readonly storeTag: Context.Tag<StoreI, Store>
  readonly outboxTag?: Context.Tag<OutboxI, Outbox>
  readonly makeMessage?: (event: Event) => Message
  readonly logPrefix: string
  readonly retry?: Schedule.Schedule<unknown, unknown>
}

/**
 * Build an `afterCommit` handler that publishes each emitted event via
 * `EventLogApi.makeClient(schema)` and retries the whole batch on failure.
 *
 * Returns an effect with `EventLog` in its requirement channel — the same
 * shape `toStatefulRpcHandlers`'s `afterCommit` expects.
 */
export const makeAfterCommitPublisher = <
  EventLogSchema extends Parameters<typeof EventLogApi.makeClient>[0],
  Event extends { readonly _tag: string }
>(
  config: AfterCommitPublisherConfig<EventLogSchema>
) =>
(input: { readonly events: ReadonlyArray<Event> }) =>
  Effect.gen(function* () {
    const publish = yield* EventLogApi.makeClient(config.schema)
    yield* Effect.forEach(
      input.events,
      // The schema's tag union narrows event._tag at runtime; the cast lets
      // us call `publish` with the polymorphic event type.
      (event) => (publish as unknown as (tag: string, e: unknown) => Effect.Effect<unknown, unknown, unknown>)(event._tag, event),
      { discard: true }
    )
  }).pipe(
    Effect.retry(config.retry ?? standardPublishRetry),
    Effect.tapError((error) =>
      Effect.logError(`${config.logPrefix} event publish failed: ${String(error)}`)
    )
  )

/**
 * Write emitted events directly through the EventJournal, projection store, and
 * outbox without EventLog's process-wide write semaphore. The EventJournal still
 * wraps each event write and projection/outbox side effects in its SQL
 * transaction, while callers can keep same-entity ordering with their own
 * entity lock.
 */
export const makeWriteThroughAfterCommitPublisher = <
  Event extends { readonly _tag: string },
  StoreI,
  Store extends WriteThroughProjectionStore<Event>,
  Message = never,
  OutboxI = never,
  Outbox extends WriteThroughOutbox<Message> = WriteThroughOutbox<Message>
>(
  config: WriteThroughAfterCommitPublisherConfig<Event, StoreI, Store, Message, OutboxI, Outbox>
) => {
  type Requirements =
    | EventJournalApi.EventJournal
    | StoreI
    | ProjectionStoreContext<Store, Event>
    | OutboxI
    | OutboxContext<Outbox, Message>

  return (input: { readonly events: ReadonlyArray<Event> }): Effect.Effect<
    void,
    unknown,
    Requirements
  > =>
    Effect.gen(function* () {
      const journal = yield* EventJournalApi.EventJournal
      const store = yield* config.storeTag
      const outbox = config.outboxTag === undefined
        ? undefined
        : yield* config.outboxTag

      yield* Effect.forEach(
        input.events,
        (event) =>
          Effect.gen(function* () {
            const eventDefinition = config.group.events[event._tag] as RuntimeEventDefinition<Event> | undefined
            if (eventDefinition === undefined) {
              return yield* Effect.fail(new Error(`Event definition not found for "${event._tag}"`))
            }
            const payload = yield* Schema.encode(
              eventDefinition.payloadMsgPack as unknown as Schema.Schema<Event, Uint8Array>
            )(event)

            yield* journal.write({
              event: event._tag,
              primaryKey: eventDefinition.primaryKey(event),
              payload,
              effect: () =>
                Effect.gen(function* () {
                  yield* (store.dispatch(event) as Effect.Effect<void, unknown, ProjectionStoreContext<Store, Event>>)
                  if (outbox !== undefined && config.makeMessage !== undefined) {
                    yield* (outbox.enqueue(config.makeMessage(event)) as Effect.Effect<void, unknown, OutboxContext<Outbox, Message>>)
                  }
                })
            })
          }),
        { discard: true }
      )
    }).pipe(
      Effect.retry(config.retry ?? standardPublishRetry),
      Effect.tapError((error) =>
        Effect.logError(`${config.logPrefix} write-through publish failed: ${String(error)}`)
      )
    ) as Effect.Effect<void, unknown, Requirements>
}
