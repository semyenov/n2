/**
 * Tests for the user-facing RPC client factories. No real network — these
 * tests pin down the contract: `makeHttpClient` returns an Effect, and
 * `makePromiseClient` exposes a callable function for any method name via
 * its Proxy.
 */
import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { Rpc, RpcGroup } from "@effect/rpc"
import { makeHttpClient, makePromiseClient } from "./Client.js"

const Echo = Rpc.make("Echo", {
  payload: { message: Schema.String },
  success: Schema.Struct({ echoed: Schema.String })
})
const TestRpcs = RpcGroup.make(Echo)

test("makeHttpClient returns an Effect", () => {
  const program = makeHttpClient(TestRpcs, "http://localhost:9999/rpc")
  // The returned value should be an Effect; we verify by checking Effect's
  // pipe interface and tag without running (which would attempt a real HTTP call).
  expect(typeof (program as { pipe: unknown }).pipe).toBe("function")
  expect(Effect.isEffect(program)).toBe(true)
})

test("makePromiseClient returns a function for any method name via Proxy", () => {
  const client = makePromiseClient(TestRpcs, "http://localhost:9999/rpc")
  // Proxy returns a function for any property access; the caller hasn't yet
  // invoked it, so no network call is made.
  expect(typeof (client as Record<string, unknown>).Echo).toBe("function")
  // Even unrecognized method names produce a function — the proxy is permissive
  // because RPC method validation happens server-side, not on access.
  expect(typeof (client as Record<string, unknown>).NonExistent).toBe("function")
})
