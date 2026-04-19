/**
 * @since 1.0.0
 *
 * Generic replay tool for rebuilding projections from an EventJournal.
 * Provides option parsing, event filtering, and replay orchestration.
 *
 * @example
 * ```ts
 * const replay = makeReplayTool({
 *   decodeEvent: makeEventDecoder(MyEventGroup, constructors),
 *   entityIdOf: (event) => event.orderId,
 *   dispatch: (event) => store.onEvent(event),
 *   eventGroup: MyEventGroup
 * })
 *
 * // Use in CLI:
 * const events = yield* replay.collectEvents(entries, options)
 * for (const event of events) {
 *   yield* replay.dispatch(event)
 * }
 * ```
 */
import * as Effect from "effect/Effect"
import type * as EventJournalApi from "@effect/experimental/EventJournal"
import type { EventGroup } from "@effect/experimental"

export interface ReplayOptions {
  readonly entityId?: string
  readonly minRevision?: number
  readonly maxRevision?: number
  readonly reset: boolean
  readonly dryRun: boolean
}

const parseRevision = (name: string, value: string) => {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer, received: ${value}`)
  }
  return parsed
}

/** Parse CLI arguments into ReplayOptions. Supports --entity-id, --min-revision, --max-revision, --reset, --no-reset, --dry-run. */
export const parseReplayOptions = (argv: ReadonlyArray<string>, resetFromEnv: boolean): ReplayOptions => {
  let entityId: string | undefined
  let minRevision: number | undefined
  let maxRevision: number | undefined
  let reset = resetFromEnv
  let dryRun = false

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]
    switch (arg) {
      case "--entity-id":
      case "--profile-id": {
        const value = argv[index + 1]
        if (!value) throw new Error(`${arg} requires a value`)
        entityId = value
        index += 1
        break
      }
      case "--min-revision": {
        const value = argv[index + 1]
        if (!value) throw new Error("--min-revision requires a value")
        minRevision = parseRevision("min revision", value)
        index += 1
        break
      }
      case "--max-revision": {
        const value = argv[index + 1]
        if (!value) throw new Error("--max-revision requires a value")
        maxRevision = parseRevision("max revision", value)
        index += 1
        break
      }
      case "--reset":
        reset = true
        break
      case "--no-reset":
        reset = false
        break
      case "--dry-run":
        dryRun = true
        break
      default:
        throw new Error(`Unknown replay flag: ${arg}`)
    }
  }

  if (minRevision !== undefined && maxRevision !== undefined && minRevision > maxRevision) {
    throw new Error(`--min-revision (${minRevision}) cannot be greater than --max-revision (${maxRevision})`)
  }

  return { entityId, minRevision, maxRevision, reset, dryRun }
}

interface ReplayEvent {
  readonly _tag: string
  readonly revision: number
}

/** Create a generic replay tool for an event group. */
export const makeReplayTool = <Event extends ReplayEvent>(config: {
  readonly decodeEvent: (entry: EventJournalApi.Entry) => Effect.Effect<Event, Error>
  readonly entityIdOf: (event: Event) => string
  readonly dispatch: (event: Event) => Effect.Effect<void, unknown, unknown>
  readonly eventGroup: EventGroup.EventGroup.Any
}) => {
  const eventTags = new Set(
    Object.keys((config.eventGroup as unknown as { events: Record<string, unknown> }).events)
  )

  const matchesOptions = (event: Event, options: ReplayOptions): boolean => {
    if (options.entityId !== undefined && config.entityIdOf(event) !== options.entityId) return false
    if (options.minRevision !== undefined && event.revision < options.minRevision) return false
    if (options.maxRevision !== undefined && event.revision > options.maxRevision) return false
    return true
  }

  const collectEvents = (
    entries: ReadonlyArray<EventJournalApi.Entry>,
    options: ReplayOptions
  ) =>
    Effect.gen(function* () {
      const events: Array<Event> = []
      for (const entry of entries) {
        if (!eventTags.has(entry.event)) continue
        const event = yield* config.decodeEvent(entry)
        if (matchesOptions(event, options)) {
          events.push(event)
        }
      }
      return events
    })

  return {
    matchesOptions,
    collectEvents,
    dispatch: config.dispatch
  }
}
