import { readFileSync } from "node:fs"
import { it, expect } from "@effect/vitest"
import * as Layer from "effect/Layer"
import { makeServiceInfrastructureLayers } from "./Runtime.js"

const source = (path: string) =>
  readFileSync(new URL(path, import.meta.url), "utf8")

it("service runtime keeps connection URLs redacted until client construction", () => {
  const runtime = source("./Runtime.ts")
  const replay = source("./ReplayLayer.ts")

  expect(runtime).toContain("Config.redacted(\"CLICKHOUSE_URL\")")
  expect(runtime).toContain("Redacted.value(redactedUrl)")
  expect(runtime).toContain("Config.redacted(\"DATABASE_URL\")")
  expect(replay).toContain("Config.redacted(\"DATABASE_URL\")")
  expect(runtime).not.toContain("Config.string(\"CLICKHOUSE_URL\")")
})

it("service infrastructure accepts Postgres projection stores without ClickHouse config", () => {
  const layers = makeServiceInfrastructureLayers({
    eventLogLayer: Layer.empty,
    projectionLayer: Layer.empty,
    projectionStoreLayer: Layer.empty,
    outboxLive: Layer.empty,
    outboxWorkerLive: Layer.empty,
    snapshotsLive: Layer.empty,
    publishHandlers: Layer.empty,
    publisherLive: Layer.empty
  })

  expect(layers.projectionStoreLayer).toBeDefined()
})

it("service infrastructure still accepts explicit ClickHouse config", () => {
  const layers = makeServiceInfrastructureLayers({
    eventLogLayer: Layer.empty,
    projectionLayer: Layer.empty,
    projectionStoreLayer: Layer.empty,
    clickhouseLayer: Layer.empty,
    clickhouseBootstrapLayer: Layer.empty,
    outboxLive: Layer.empty,
    outboxWorkerLive: Layer.empty,
    snapshotsLive: Layer.empty,
    publishHandlers: Layer.empty,
    publisherLive: Layer.empty
  })

  expect(layers.clickhouseReadyLayer).toBeDefined()
})
