import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as ClickhouseClient from "@effect/sql-clickhouse/ClickhouseClient"

const createProjectionEventsTable = Effect.gen(function* () {
  const clickhouse = yield* ClickhouseClient.ClickhouseClient
  yield* clickhouse.asCommand(clickhouse`
    CREATE TABLE IF NOT EXISTS request_provider_projection_events (
      request_id String,
      revision UInt64,
      event_type String,
      occurred_at String,
      snapshot_id String,
      payload_json String
    )
    ENGINE = ReplacingMergeTree(revision)
    ORDER BY (request_id, revision, event_type)
  `)
})

const createRequestsCurrentTable = Effect.gen(function* () {
  const clickhouse = yield* ClickhouseClient.ClickhouseClient
  yield* clickhouse.asCommand(clickhouse`
    CREATE TABLE IF NOT EXISTS request_provider_requests_current (
      request_id String,
      revision UInt64,
      client_id String,
      status String,
      current_schema_version String,
      request_json String,
      latest_metadata_json String,
      latest_snapshot_id String,
      created_at String,
      updated_at String
    )
    ENGINE = ReplacingMergeTree(revision)
    ORDER BY (request_id, revision)
  `)
})

const createSnapshotsCurrentTable = Effect.gen(function* () {
  const clickhouse = yield* ClickhouseClient.ClickhouseClient
  yield* clickhouse.asCommand(clickhouse`
    CREATE TABLE IF NOT EXISTS request_provider_snapshots_current (
      snapshot_id String,
      request_id String,
      revision UInt64,
      snapshot_type String,
      request_json String,
      metadata_json String,
      schema_version String,
      summary String,
      created_at String,
      created_by String
    )
    ENGINE = ReplacingMergeTree(revision)
    ORDER BY (snapshot_id, revision)
  `)
})

export const RequestProviderClickhouseBootstrapLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* createProjectionEventsTable
    yield* createRequestsCurrentTable
    yield* createSnapshotsCurrentTable
  })
)

export const resetRequestProviderClickhouseTables = Effect.gen(function* () {
  const clickhouse = yield* ClickhouseClient.ClickhouseClient
  yield* clickhouse.asCommand(clickhouse`TRUNCATE TABLE IF EXISTS request_provider_projection_events`)
  yield* clickhouse.asCommand(clickhouse`TRUNCATE TABLE IF EXISTS request_provider_requests_current`)
  yield* clickhouse.asCommand(clickhouse`TRUNCATE TABLE IF EXISTS request_provider_snapshots_current`)
})
