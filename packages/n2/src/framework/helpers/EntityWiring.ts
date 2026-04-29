import * as Effect from "effect/Effect"
import * as Schedule from "effect/Schedule"
import * as EventLogApi from "@effect/experimental/EventLog"

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
