/**
 * Framework-level unit tests for Outbox helpers.
 */
import { test, expect } from "bun:test"
import { computeRetryDelaySeconds } from "./Outbox.js"

test("computeRetryDelaySeconds uses exponential backoff", () => {
  expect(computeRetryDelaySeconds(0)).toBe(1)
  expect(computeRetryDelaySeconds(1)).toBe(2)
  expect(computeRetryDelaySeconds(2)).toBe(4)
  expect(computeRetryDelaySeconds(3)).toBe(8)
  expect(computeRetryDelaySeconds(4)).toBe(16)
})

test("computeRetryDelaySeconds caps at 300 seconds", () => {
  expect(computeRetryDelaySeconds(9)).toBe(300) // 2^9 = 512 > 300
  expect(computeRetryDelaySeconds(10)).toBe(300)
  expect(computeRetryDelaySeconds(20)).toBe(300)
})

test("computeRetryDelaySeconds handles edge case at boundary", () => {
  // 2^8 = 256, which is under 300
  expect(computeRetryDelaySeconds(8)).toBe(256)
})
