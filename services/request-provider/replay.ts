import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { BunRuntime } from "@effect/platform-bun"
import * as EventJournalApi from "@effect/experimental/EventJournal"
import * as SqlEventJournal from "@effect/sql/SqlEventJournal"
import { PgClient } from "@effect/sql-pg"
import { makeEventDecoder, makeReplayProgram, makeReplayTool, parseReplayOptions, type ReplayOptions } from "@semyenov/n2/helpers"
import { RequestProviderClickhouseBootstrapLayer, resetRequestProviderClickhouseTables } from "./clickhouse-schema.js"
import { RequestProviderClickhouseLayer } from "./clickhouse.js"
import {
  RequestCreated,
  RequestMetaDataCreated,
  type RequestProviderEvent,
  RequestSnapshotCreated,
  RequestUpdated
} from "./contracts.js"
import { RequestProviderEventGroup } from "./events.js"
import { RequestProviderProjectionStore } from "./projection-store.js"
import { RequestProviderProjectionStoreClickhouseLive } from "./projection-store-clickhouse.js"

const decodeEvent = makeEventDecoder<RequestProviderEvent>(RequestProviderEventGroup, {
  RequestCreated,
  RequestUpdated,
  RequestMetaDataCreated,
  RequestSnapshotCreated
})

const dispatchToStore = (event: RequestProviderEvent) =>
  Effect.flatMap(RequestProviderProjectionStore, (store) => store.dispatch(event))

const replay = makeReplayTool({
  decodeEvent,
  entityIdOf: (event) => event.requestId,
  dispatch: dispatchToStore,
  eventGroup: RequestProviderEventGroup
})

export { parseReplayOptions, type ReplayOptions }
export const toRequestProviderEvent = decodeEvent
export const matchesReplayOptions = replay.matchesOptions
export const collectReplayEvents = replay.collectEvents

const SqlLayer = PgClient.layerConfig(
  Config.map(Config.redacted("DATABASE_URL"), (url) => ({ url }))
)

const clickhouseReadyLayer = Layer.merge(
  RequestProviderClickhouseLayer,
  Layer.provide(RequestProviderClickhouseBootstrapLayer, RequestProviderClickhouseLayer)
)

const ReplayLayer = Layer.mergeAll(
  Layer.provide(SqlEventJournal.layer(), SqlLayer),
  RequestProviderClickhouseLayer,
  Layer.provide(RequestProviderClickhouseBootstrapLayer, RequestProviderClickhouseLayer),
  Layer.provide(RequestProviderProjectionStoreClickhouseLive, clickhouseReadyLayer)
)

const program = Effect.gen(function* () {
  const resetFromEnv = yield* Config.boolean("RESET_CLICKHOUSE").pipe(Config.withDefault(true))
  yield* makeReplayProgram({
    argv: Bun.argv.slice(2),
    resetDefault: resetFromEnv,
    label: "request-provider projection",
    entries: Effect.flatMap(EventJournalApi.EventJournal, (journal) => journal.entries),
    decodeEvent,
    entityIdOf: (event) => event.requestId,
    dispatch: dispatchToStore,
    eventGroup: RequestProviderEventGroup,
    reset: resetRequestProviderClickhouseTables
  })
})

const main = program.pipe(Effect.provide(ReplayLayer))

if (import.meta.main) {
  BunRuntime.runMain(main)
}
