import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as ClickhouseClient from "@effect/sql-clickhouse/ClickhouseClient"

const createProjectionEventsTable = Effect.gen(function* () {
  const clickhouse = yield* ClickhouseClient.ClickhouseClient
  yield* clickhouse.asCommand(clickhouse`
    CREATE TABLE IF NOT EXISTS profile_provider_projection_events (
      profile_id String,
      revision UInt64,
      event_type String,
      occurred_at String,
      branch_id String,
      snapshot_id String,
      payload_json String
    )
    ENGINE = ReplacingMergeTree(revision)
    ORDER BY (profile_id, revision, event_type)
  `)
})

const createProfilesCurrentTable = Effect.gen(function* () {
  const clickhouse = yield* ClickhouseClient.ClickhouseClient
  yield* clickhouse.asCommand(clickhouse`
    CREATE TABLE IF NOT EXISTS profile_provider_profiles_current (
      profile_id String,
      revision UInt64,
      owner_agent_id String,
      active_branch_id String,
      status String,
      current_schema_version String,
      masked_profile_json String,
      latest_metadata_json String,
      latest_pii_storage_key String,
      pii_jurisdiction String,
      published_snapshot_id String,
      created_at String,
      updated_at String
    )
    ENGINE = ReplacingMergeTree(revision)
    ORDER BY (profile_id, revision)
  `)
})

const createSnapshotsCurrentTable = Effect.gen(function* () {
  const clickhouse = yield* ClickhouseClient.ClickhouseClient
  yield* clickhouse.asCommand(clickhouse`
    CREATE TABLE IF NOT EXISTS profile_provider_snapshots_current (
      snapshot_id String,
      profile_id String,
      branch_id String,
      revision UInt64,
      snapshot_type String,
      profile_json String,
      metadata_json String,
      schema_version String,
      summary String,
      published UInt8,
      strategy_json String,
      created_at String,
      created_by String
    )
    ENGINE = ReplacingMergeTree(revision)
    ORDER BY (snapshot_id, revision)
  `)
})

export const ProfileProviderClickhouseBootstrapLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* createProjectionEventsTable
    yield* createProfilesCurrentTable
    yield* createSnapshotsCurrentTable
  })
)

export const resetProfileProviderClickhouseTables = Effect.gen(function* () {
  const clickhouse = yield* ClickhouseClient.ClickhouseClient
  yield* clickhouse.asCommand(clickhouse`TRUNCATE TABLE IF EXISTS profile_provider_projection_events`)
  yield* clickhouse.asCommand(clickhouse`TRUNCATE TABLE IF EXISTS profile_provider_profiles_current`)
  yield* clickhouse.asCommand(clickhouse`TRUNCATE TABLE IF EXISTS profile_provider_snapshots_current`)
})
