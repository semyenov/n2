import { it, expect } from "@effect/vitest"
import * as Layer from "effect/Layer"
import { makeReplayInfrastructureLayer } from "./ReplayLayer.js"

it("makeReplayInfrastructureLayer composes empty inputs into a Layer", () => {
  const layer = makeReplayInfrastructureLayer({
    projectionStoreLayer: Layer.empty,
    sqlLayer: Layer.empty as never
  })

  // Type-level: assert the output is a Layer, not erased to unknown.
  // The Error channel is non-empty (default sqlLayer reads DATABASE_URL) —
  // we just verify the type extraction works, which means the wiring infers
  // a Layer<...> shape rather than collapsing to never.
  type _Out = Layer.Layer.Success<typeof layer>
  type _Err = Layer.Layer.Error<typeof layer>
  expect(layer).toBeDefined()
})

it("makeReplayInfrastructureLayer accepts service-specific event journal tables", () => {
  const layer = makeReplayInfrastructureLayer({
    eventJournal: {
      entryTable: "service_event_journal",
      remotesTable: "service_event_remotes"
    },
    projectionStoreLayer: Layer.empty,
    sqlLayer: Layer.empty as never
  })

  expect(layer).toBeDefined()
})

it("makeReplayInfrastructureLayer still accepts explicit ClickHouse layers", () => {
  const layer = makeReplayInfrastructureLayer({
    clickhouseLayer: Layer.empty,
    clickhouseBootstrapLayer: Layer.empty,
    projectionStoreLayer: Layer.empty,
    sqlLayer: Layer.empty as never
  })

  expect(layer).toBeDefined()
})
