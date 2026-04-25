/**
 * Framework-level unit tests for parseReplayOptions.
 */
import { test, expect } from "bun:test"
import { parseReplayOptions } from "./Replay.js"

test("parseReplayOptions with no args returns defaults", () => {
  const opts = parseReplayOptions([], false)
  expect(opts.entityId).toBeUndefined()
  expect(opts.minRevision).toBeUndefined()
  expect(opts.maxRevision).toBeUndefined()
  expect(opts.reset).toBe(false)
  expect(opts.dryRun).toBe(false)
})

test("parseReplayOptions respects resetFromEnv default", () => {
  const opts = parseReplayOptions([], true)
  expect(opts.reset).toBe(true)
})

test("parseReplayOptions parses --entity-id", () => {
  const opts = parseReplayOptions(["--entity-id", "abc-123"], false)
  expect(opts.entityId).toBe("abc-123")
})

test("parseReplayOptions parses --profile-id alias", () => {
  const opts = parseReplayOptions(["--profile-id", "abc-123"], false)
  expect(opts.entityId).toBe("abc-123")
})

test("parseReplayOptions parses revision range", () => {
  const opts = parseReplayOptions(["--min-revision", "5", "--max-revision", "10"], false)
  expect(opts.minRevision).toBe(5)
  expect(opts.maxRevision).toBe(10)
})

test("parseReplayOptions parses --dry-run", () => {
  const opts = parseReplayOptions(["--dry-run"], false)
  expect(opts.dryRun).toBe(true)
})

test("parseReplayOptions --reset overrides env default", () => {
  const opts = parseReplayOptions(["--reset"], false)
  expect(opts.reset).toBe(true)
})

test("parseReplayOptions --no-reset overrides env default", () => {
  const opts = parseReplayOptions(["--no-reset"], true)
  expect(opts.reset).toBe(false)
})

test("parseReplayOptions throws on missing --entity-id value", () => {
  expect(() => parseReplayOptions(["--entity-id"], false)).toThrow("requires a value")
})

test("parseReplayOptions throws on missing --min-revision value", () => {
  expect(() => parseReplayOptions(["--min-revision"], false)).toThrow("requires a value")
})

test("parseReplayOptions throws on negative revision", () => {
  expect(() => parseReplayOptions(["--min-revision", "-1"], false)).toThrow()
})

test("parseReplayOptions throws when min > max revision", () => {
  expect(() => parseReplayOptions(["--min-revision", "10", "--max-revision", "5"], false)).toThrow("cannot be greater than")
})

test("parseReplayOptions throws on unknown flag", () => {
  expect(() => parseReplayOptions(["--unknown"], false)).toThrow("Unknown replay flag")
})

test("parseReplayOptions parses all options together", () => {
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
