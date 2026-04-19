import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { BunRuntime } from "@effect/platform-bun"
import * as EventJournalApi from "@effect/experimental/EventJournal"
import * as SqlEventJournal from "@effect/sql/SqlEventJournal"
import { PgClient } from "@effect/sql-pg"
import { makeEventDecoder } from "../../src/framework/helpers/EventDecoder.js"
import { makeReplayTool, parseReplayOptions, type ReplayOptions } from "../../src/framework/helpers/Replay.js"
import { ProfileProviderClickhouseBootstrapLayer, resetProfileProviderClickhouseTables } from "./clickhouse-schema.js"
import { ProfileProviderClickhouseLayer } from "./clickhouse.js"
import {
  MergedDataProfile,
  MetaDataCreated,
  PersonalDataExtracted,
  ProfileBranchForked,
  ProfileCreated,
  type ProfileEvent,
  SnapshotCreatedProfile,
  SnapshotPublishedProfile
} from "./contracts.js"
import { ProfileProviderEventGroup } from "./events.js"
import { ProfileProviderProjectionStore } from "./projection-store.js"
import { ProfileProviderProjectionStoreClickhouseLive } from "./projection-store-clickhouse.js"

const decodeEvent = makeEventDecoder<ProfileEvent>(ProfileProviderEventGroup, {
  ProfileCreated,
  MergedDataProfile,
  SnapshotCreatedProfile,
  MetaDataCreated,
  PersonalDataExtracted,
  ProfileBranchForked,
  SnapshotPublishedProfile
})

const dispatchToStore = (event: ProfileEvent) =>
  Effect.gen(function* () {
    const store = yield* ProfileProviderProjectionStore
    switch (event._tag) {
      case "ProfileCreated": return yield* store.onProfileCreated(event)
      case "MergedDataProfile": return yield* store.onMergedDataProfile(event)
      case "SnapshotCreatedProfile": return yield* store.onSnapshotCreatedProfile(event)
      case "MetaDataCreated": return yield* store.onMetaDataCreated(event)
      case "PersonalDataExtracted": return yield* store.onPersonalDataExtracted(event)
      case "ProfileBranchForked": return yield* store.onProfileBranchForked(event)
      case "SnapshotPublishedProfile": return yield* store.onSnapshotPublishedProfile(event)
    }
  })

const replay = makeReplayTool<ProfileEvent>({
  decodeEvent,
  entityIdOf: (event) => event.profileId,
  dispatch: dispatchToStore,
  eventGroup: ProfileProviderEventGroup
})

export { parseReplayOptions, type ReplayOptions }
export const toProfileEvent = decodeEvent
export const matchesReplayOptions = replay.matchesOptions
export const collectReplayEvents = replay.collectEvents

const SqlLayer = PgClient.layerConfig(
  Config.map(Config.redacted("DATABASE_URL"), (url) => ({ url }))
)

const clickhouseReadyLayer = Layer.merge(
  ProfileProviderClickhouseLayer,
  Layer.provide(ProfileProviderClickhouseBootstrapLayer, ProfileProviderClickhouseLayer)
)

const ReplayLayer = Layer.mergeAll(
  SqlLayer,
  SqlEventJournal.layer(),
  ProfileProviderClickhouseLayer,
  ProfileProviderClickhouseBootstrapLayer,
  Layer.provide(ProfileProviderProjectionStoreClickhouseLive, clickhouseReadyLayer)
)

const program = Effect.gen(function* () {
  const resetFromEnv = yield* Config.boolean("RESET_CLICKHOUSE").pipe(Config.withDefault(true))
  const options = yield* Effect.sync(() => parseReplayOptions(Bun.argv.slice(2), resetFromEnv))
  const journal = yield* EventJournalApi.EventJournal

  if (options.reset && !options.dryRun) {
    yield* Effect.log("Resetting ClickHouse profile-provider projection tables")
    yield* resetProfileProviderClickhouseTables
  }

  const entries = yield* journal.entries
  const profileEvents = yield* replay.collectEvents(entries, options)

  yield* Effect.log("Starting profile-provider projection replay").pipe(
    Effect.annotateLogs({
      profileId: options.entityId ?? "all",
      minRevision: options.minRevision ?? "none",
      maxRevision: options.maxRevision ?? "none",
      reset: options.reset,
      dryRun: options.dryRun
    })
  )

  yield* Effect.log(`Replaying ${profileEvents.length} profile-provider events into ClickHouse`)

  if (options.dryRun) {
    yield* Effect.log("Dry run enabled, skipping ClickHouse mutation")
    return
  }

  for (const event of profileEvents) {
    yield* replay.dispatch(event)
  }

  yield* Effect.log("Profile-provider projection replay completed")
})

if (import.meta.main) {
  BunRuntime.runMain(
    program.pipe(Effect.provide(ReplayLayer)) as Effect.Effect<void, never, never>
  )
}
