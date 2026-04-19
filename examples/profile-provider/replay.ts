import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { BunRuntime } from "@effect/platform-bun"
import * as EventJournalApi from "@effect/experimental/EventJournal"
import * as ClickhouseClient from "@effect/sql-clickhouse/ClickhouseClient"
import * as SqlEventJournal from "@effect/sql/SqlEventJournal"
import { PgClient } from "@effect/sql-pg"
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
import {
  insertProfileProviderProjectionEvent,
  upsertProfileProviderCurrentProfile,
  upsertProfileProviderCurrentSnapshot
} from "./projection-store-clickhouse.js"

/** Decode event payloads from EventJournal msgpack using EventGroup decoders. */
const decoders = {
  ProfileCreated: Schema.decode(ProfileProviderEventGroup.events.ProfileCreated!.payloadMsgPack as never) as (u: unknown) => Effect.Effect<ConstructorParameters<typeof ProfileCreated>[0]>,
  MergedDataProfile: Schema.decode(ProfileProviderEventGroup.events.MergedDataProfile!.payloadMsgPack as never) as (u: unknown) => Effect.Effect<ConstructorParameters<typeof MergedDataProfile>[0]>,
  SnapshotCreatedProfile: Schema.decode(ProfileProviderEventGroup.events.SnapshotCreatedProfile!.payloadMsgPack as never) as (u: unknown) => Effect.Effect<ConstructorParameters<typeof SnapshotCreatedProfile>[0]>,
  MetaDataCreated: Schema.decode(ProfileProviderEventGroup.events.MetaDataCreated!.payloadMsgPack as never) as (u: unknown) => Effect.Effect<ConstructorParameters<typeof MetaDataCreated>[0]>,
  PersonalDataExtracted: Schema.decode(ProfileProviderEventGroup.events.PersonalDataExtracted!.payloadMsgPack as never) as (u: unknown) => Effect.Effect<ConstructorParameters<typeof PersonalDataExtracted>[0]>,
  ProfileBranchForked: Schema.decode(ProfileProviderEventGroup.events.ProfileBranchForked!.payloadMsgPack as never) as (u: unknown) => Effect.Effect<ConstructorParameters<typeof ProfileBranchForked>[0]>,
  SnapshotPublishedProfile: Schema.decode(ProfileProviderEventGroup.events.SnapshotPublishedProfile!.payloadMsgPack as never) as (u: unknown) => Effect.Effect<ConstructorParameters<typeof SnapshotPublishedProfile>[0]>
} as const

const SqlLayer = PgClient.layerConfig(
  Config.map(Config.redacted("DATABASE_URL"), (url) => ({ url }))
)

export type ReplayOptions = {
  readonly profileId?: string
  readonly minRevision?: number
  readonly maxRevision?: number
  readonly reset: boolean
  readonly dryRun: boolean
}

const parseRevision = (name: string, value: string) => {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer, received: ${value}`)
  }
  return parsed
}

export const parseReplayOptions = (argv: ReadonlyArray<string>, resetFromEnv: boolean): ReplayOptions => {
  let profileId: string | undefined
  let minRevision: number | undefined
  let maxRevision: number | undefined
  let reset = resetFromEnv
  let dryRun = false

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]
    switch (arg) {
      case "--profile-id": {
        const value = argv[index + 1]
        if (!value) {
          throw new Error("--profile-id requires a value")
        }
        profileId = value
        index += 1
        break
      }
      case "--min-revision": {
        const value = argv[index + 1]
        if (!value) {
          throw new Error("--min-revision requires a value")
        }
        minRevision = parseRevision("min revision", value)
        index += 1
        break
      }
      case "--max-revision": {
        const value = argv[index + 1]
        if (!value) {
          throw new Error("--max-revision requires a value")
        }
        maxRevision = parseRevision("max revision", value)
        index += 1
        break
      }
      case "--reset":
        reset = true
        break
      case "--no-reset":
        reset = false
        break
      case "--dry-run":
        dryRun = true
        break
      default:
        throw new Error(`Unknown replay flag: ${arg}`)
    }
  }

  if (minRevision !== undefined && maxRevision !== undefined && minRevision > maxRevision) {
    throw new Error(`--min-revision (${minRevision}) cannot be greater than --max-revision (${maxRevision})`)
  }

  return {
    profileId,
    minRevision,
    maxRevision,
    reset,
    dryRun
  }
}

export const toProfileEvent = (entry: EventJournalApi.Entry) => {
  switch (entry.event) {
    case "ProfileCreated":
      return decoders.ProfileCreated(entry.payload).pipe(
        Effect.map((payload) => new ProfileCreated(payload))
      )
    case "MergedDataProfile":
      return decoders.MergedDataProfile(entry.payload).pipe(
        Effect.map((payload) => new MergedDataProfile(payload))
      )
    case "SnapshotCreatedProfile":
      return decoders.SnapshotCreatedProfile(entry.payload).pipe(
        Effect.map((payload) => new SnapshotCreatedProfile(payload))
      )
    case "MetaDataCreated":
      return decoders.MetaDataCreated(entry.payload).pipe(
        Effect.map((payload) => new MetaDataCreated(payload))
      )
    case "PersonalDataExtracted":
      return decoders.PersonalDataExtracted(entry.payload).pipe(
        Effect.map((payload) => new PersonalDataExtracted(payload))
      )
    case "ProfileBranchForked":
      return decoders.ProfileBranchForked(entry.payload).pipe(
        Effect.map((payload) => new ProfileBranchForked(payload))
      )
    case "SnapshotPublishedProfile":
      return decoders.SnapshotPublishedProfile(entry.payload).pipe(
        Effect.map((payload) => new SnapshotPublishedProfile(payload))
      )
    default:
      return Effect.fail(new Error(`Unsupported profile-provider event: ${entry.event}`))
  }
}

const replayEvent = (event: ProfileEvent) =>
  Effect.gen(function* () {
    const clickhouse = yield* ClickhouseClient.ClickhouseClient
    yield* insertProfileProviderProjectionEvent(clickhouse, event)
    yield* upsertProfileProviderCurrentProfile(clickhouse, event)
    yield* upsertProfileProviderCurrentSnapshot(clickhouse, event)
  })

const ReplayLayer = Layer.mergeAll(
  SqlLayer,
  SqlEventJournal.layer(),
  ProfileProviderClickhouseLayer,
  ProfileProviderClickhouseBootstrapLayer
)

export const matchesReplayOptions = (event: ProfileEvent, options: ReplayOptions) => {
  if (options.profileId !== undefined && event.profileId !== options.profileId) {
    return false
  }

  if (options.minRevision !== undefined && event.revision < options.minRevision) {
    return false
  }

  if (options.maxRevision !== undefined && event.revision > options.maxRevision) {
    return false
  }

  return true
}

export const collectReplayEvents = (
  entries: ReadonlyArray<EventJournalApi.Entry>,
  options: ReplayOptions
) =>
  Effect.gen(function* () {
    const profileEvents: Array<ProfileEvent> = []

    for (const entry of entries) {
      if (!(entry.event in ProfileProviderEventGroup.events)) {
        continue
      }

      const event = yield* toProfileEvent(entry)
      if (matchesReplayOptions(event, options)) {
        profileEvents.push(event)
      }
    }

    return profileEvents
  })

const program = Effect.gen(function* () {
  const resetFromEnv = yield* Config.boolean("RESET_CLICKHOUSE").pipe(Config.withDefault(true))
  const options = yield* Effect.sync(() => parseReplayOptions(Bun.argv.slice(2), resetFromEnv))
  const journal = yield* EventJournalApi.EventJournal

  if (options.reset && !options.dryRun) {
    yield* Effect.log("Resetting ClickHouse profile-provider projection tables")
    yield* resetProfileProviderClickhouseTables
  }

  const entries = yield* journal.entries
  const profileEvents = yield* collectReplayEvents(entries, options)

  yield* Effect.log("Starting profile-provider projection replay").pipe(
    Effect.annotateLogs({
      profileId: options.profileId ?? "all",
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
    yield* replayEvent(event)
  }

  yield* Effect.log("Profile-provider projection replay completed")
})

if (import.meta.main) {
  BunRuntime.runMain(
    program.pipe(Effect.provide(ReplayLayer)) as Effect.Effect<void, never, never>
  )
}
