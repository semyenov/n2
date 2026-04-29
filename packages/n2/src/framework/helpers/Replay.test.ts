/**
 * Framework-level unit tests for parseReplayOptions.
 */
import { it, expect } from "@effect/vitest"
import * as Effect from "effect/Effect"
import type { EventGroup } from "@effect/experimental"
import type * as EventJournalApi from "@effect/experimental/EventJournal"
import { makeReplayProgram, parseReplayOptions } from "./Replay.js"

type TestReplayEvent = {
  readonly _tag: "TestEvent"
  readonly entityId: string
  readonly revision: number
}

const TestEventGroup = {
  events: { TestEvent: {} }
} as unknown as EventGroup.EventGroup.Any

const makeEntry = (entityId: string, revision: number) => ({
  event: "TestEvent",
  payload: { entityId, revision }
}) as unknown as EventJournalApi.Entry

const decodeTestEvent = (entry: EventJournalApi.Entry) => {
  const payload = entry.payload as unknown as { readonly entityId: string; readonly revision: number }
  return Effect.succeed({
    _tag: "TestEvent" as const,
    entityId: payload.entityId,
    revision: payload.revision
  })
}

it("parseReplayOptions with no args returns defaults", () => {
  const opts = parseReplayOptions([], false)
  expect(opts.entityId).toBeUndefined()
  expect(opts.minRevision).toBeUndefined()
  expect(opts.maxRevision).toBeUndefined()
  expect(opts.reset).toBe(false)
  expect(opts.dryRun).toBe(false)
})

it("parseReplayOptions respects resetFromEnv default", () => {
  const opts = parseReplayOptions([], true)
  expect(opts.reset).toBe(true)
})

it("parseReplayOptions parses --entity-id", () => {
  const opts = parseReplayOptions(["--entity-id", "abc-123"], false)
  expect(opts.entityId).toBe("abc-123")
})

it("parseReplayOptions parses --profile-id alias", () => {
  const opts = parseReplayOptions(["--profile-id", "abc-123"], false)
  expect(opts.entityId).toBe("abc-123")
})

it("parseReplayOptions parses revision range", () => {
  const opts = parseReplayOptions(["--min-revision", "5", "--max-revision", "10"], false)
  expect(opts.minRevision).toBe(5)
  expect(opts.maxRevision).toBe(10)
})

it("parseReplayOptions parses --dry-run", () => {
  const opts = parseReplayOptions(["--dry-run"], false)
  expect(opts.dryRun).toBe(true)
})

it("parseReplayOptions --reset overrides env default", () => {
  const opts = parseReplayOptions(["--reset"], false)
  expect(opts.reset).toBe(true)
})

it("parseReplayOptions --no-reset overrides env default", () => {
  const opts = parseReplayOptions(["--no-reset"], true)
  expect(opts.reset).toBe(false)
})

it("parseReplayOptions throws on missing --entity-id value", () => {
  expect(() => parseReplayOptions(["--entity-id"], false)).toThrow("requires a value")
})

it("parseReplayOptions throws on missing --min-revision value", () => {
  expect(() => parseReplayOptions(["--min-revision"], false)).toThrow("requires a value")
})

it("parseReplayOptions throws on negative revision", () => {
  expect(() => parseReplayOptions(["--min-revision", "-1"], false)).toThrow()
})

it("parseReplayOptions throws when min > max revision", () => {
  expect(() => parseReplayOptions(["--min-revision", "10", "--max-revision", "5"], false)).toThrow("cannot be greater than")
})

it("parseReplayOptions throws on unknown flag", () => {
  expect(() => parseReplayOptions(["--unknown"], false)).toThrow("Unknown replay flag")
})

it("parseReplayOptions parses all options together", () => {
  const opts = parseReplayOptions([
    "--entity-id", "e-1",
    "--min-revision", "3",
    "--max-revision", "7",
    "--dry-run",
    "--no-reset"
  ], true)
  expect(opts.entityId).toBe("e-1")
  expect(opts.minRevision).toBe(3)
  expect(opts.maxRevision).toBe(7)
  expect(opts.dryRun).toBe(true)
  expect(opts.reset).toBe(false)
})

it.effect("makeReplayProgram filters entries and skips dispatch in dry-run mode", () => Effect.gen(function* () {
  const dispatched: Array<string> = []
  const resetCalls: Array<string> = []

  const summary = yield* makeReplayProgram({
    argv: ["--entity-id", "entity-1", "--min-revision", "2", "--dry-run"],
    resetDefault: true,
    label: "test projection",
    entries: Effect.succeed([
      makeEntry("entity-1", 1),
      makeEntry("entity-1", 2),
      makeEntry("entity-2", 3)
    ]),
    decodeEvent: decodeTestEvent,
    entityIdOf: (event) => event.entityId,
    dispatch: (event: TestReplayEvent) =>
      Effect.sync(() => {
        dispatched.push(`${event.entityId}:${event.revision}`)
      }),
    eventGroup: TestEventGroup,
    reset: Effect.sync(() => {
      resetCalls.push("reset")
    })
  })

  expect(summary.collected).toBe(1)
  expect(summary.dispatched).toBe(0)
  expect(summary.options.reset).toBe(true)
  expect(summary.options.dryRun).toBe(true)
  expect(dispatched).toEqual([])
  expect(resetCalls).toEqual([])
}))

it.effect("makeReplayProgram resets and dispatches matching events sequentially", () => Effect.gen(function* () {
  const calls: Array<string> = []

  const summary = yield* makeReplayProgram({
    argv: ["--entity-id", "entity-1"],
    resetDefault: true,
    label: "test projection",
    entries: Effect.succeed([
      makeEntry("entity-1", 1),
      makeEntry("entity-1", 2),
      makeEntry("entity-2", 3)
    ]),
    decodeEvent: decodeTestEvent,
    entityIdOf: (event) => event.entityId,
    dispatch: (event: TestReplayEvent) =>
      Effect.sync(() => {
        calls.push(`dispatch:${event.revision}`)
      }),
    eventGroup: TestEventGroup,
    reset: Effect.sync(() => {
      calls.push("reset")
    })
  })

  expect(summary.collected).toBe(2)
  expect(summary.dispatched).toBe(2)
  expect(calls).toEqual(["reset", "dispatch:1", "dispatch:2"])
}))
