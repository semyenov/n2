/**
 * @since 1.0.0
 * @module Replay
 *
 * Replay projections from the EventLog.
 *
 * For native Effect approach, use EventJournal.entries() directly.
 */
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import type { ProjectorDefinition } from "./ProjectorDefinition.js"
import { EventLog } from "../runtime/EventLog.js"

/**
 * @since 1.0.0
 * @category models
 */
export interface ReplayConfig {
  readonly fromPosition?: bigint
  readonly batchSize?: number
}

/**
 * Replays all events through a projector from the event log.
 *
 * @since 1.0.0
 * @category constructors
 */
export const replay = <State>(
  projector: ProjectorDefinition<State>,
  config?: ReplayConfig
): Effect.Effect<State, never, EventLog> =>
  Effect.gen(function*() {
    const eventLog = yield* EventLog
    const eventStream = eventLog.readAll(config?.fromPosition)

    let state = projector.initialState
    const batchSize = config?.batchSize ?? 1000

    yield* eventStream.pipe(
      Stream.grouped(batchSize),
      Stream.runForEach((batch) =>
        Effect.gen(function*() {
          for (const envelope of batch) {
            state = yield* projector.handle(state, envelope)
          }
        })
      )
    )

    return state
  })
