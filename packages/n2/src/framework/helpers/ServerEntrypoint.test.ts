import { test, expect } from "bun:test"
import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { RpcGroup } from "@effect/rpc"
import { makeServerEntrypoint } from "./ServerEntrypoint.js"

type Assert<T extends true> = T
type IsNever<T> = [T] extends [never] ? true : false

const EmptyRpcs = RpcGroup.make()

test("makeServerEntrypoint returns an Effect with E=never via Effect.orDie", () => {
  const program = makeServerEntrypoint({
    group: EmptyRpcs,
    path: "/rpc/test",
    handlers: EmptyRpcs.toLayer({}),
    migrationsLayer: Layer.empty,
    infrastructureLayer: Layer.empty,
    defaultPort: 3000,
    banner: ["test on {port}"],
    portConfig: Config.succeed(3000),
    sqlLayer: Layer.empty as never
  })

  type ProgramErrors = Effect.Effect.Error<typeof program>
  type ErrorsAreNever = Assert<IsNever<ProgramErrors>>
  const _: ErrorsAreNever = true
  expect(_).toBe(true)
  expect(program).toBeDefined()
})
