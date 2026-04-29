import { it, expect } from "@effect/vitest"
import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { RpcGroup } from "@effect/rpc"
import { makeClusterEntrypoint } from "./ClusterEntrypoint.js"

type Assert<T extends true> = T
type IsNever<T> = [T] extends [never] ? true : false

const EmptyRpcs = RpcGroup.make()

it("makeClusterEntrypoint returns an Effect with E=never via Effect.orDie", () => {
  const program = makeClusterEntrypoint({
    proxyGroup: EmptyRpcs,
    path: "/rpc/test",
    proxyHandlers: EmptyRpcs.toLayer({}),
    entityLayer: Layer.empty,
    clusterInfrastructureLayer: Layer.empty,
    migrationsLayer: Layer.empty,
    defaultApiPort: 4000,
    banner: ["test on {port}"],
    apiPortConfig: Config.succeed(4000),
    sqlLayer: Layer.empty as never
  })

  type ProgramErrors = Effect.Effect.Error<typeof program>
  type ErrorsAreNever = Assert<IsNever<ProgramErrors>>
  // assertType-only: we don't actually run the program (would block on Effect.never).
  const _: ErrorsAreNever = true
  expect(_).toBe(true)
  expect(program).toBeDefined()
})
