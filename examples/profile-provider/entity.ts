import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Metric from "effect/Metric"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Schedule from "effect/Schedule"
import * as SynchronizedRef from "effect/SynchronizedRef"
import { Entity, EntityProxy, EntityProxyServer } from "@effect/cluster"
import * as EventLogApi from "@effect/experimental/EventLog"
import { handle, initialProfileState } from "./aggregate.js"
import { InfrastructureLayer } from "./layers.js"
import { ProfileProviderEventLogSchema } from "./events.js"
import { ProfileProviderSnapshots, SNAPSHOT_EVERY, type SnapshotEntry } from "./snapshots.js"
import {
  type ProfileEvent,
  type ProfileState,
  CommandResult,
  CreateProfile,
  CreateProfileSnapshot,
  ForkProfileBranch,
  GetProfile,
  GetProfileHistory,
  MergeProfileData,
  ProfileError,
  ProfileHistory,
  ProfileNotFound,
  ProfileProviderEntity,
  ProfileProviderRpcs,
  PublishProfileSnapshot
} from "./contracts.js"

type WriteCommand =
  | CreateProfile
  | MergeProfileData
  | ForkProfileBranch
  | CreateProfileSnapshot
  | PublishProfileSnapshot

type ProfileEntry = { readonly state: ProfileState; readonly revision: number }

const commandTotal = Metric.counter("profile_provider.commands.total", { incremental: true })
const commandErrors = Metric.counter("profile_provider.commands.errors", { incremental: true })
const commandLatency = Metric.timer("profile_provider.command.duration_ms", "milliseconds")

const publishRetry = Schedule.exponential("100 millis").pipe(
  Schedule.jittered,
  Schedule.intersect(Schedule.recurs(3))
)

const toHistory = (state: ProfileState) =>
  new ProfileHistory({
    profileId: state.profileId,
    activeBranchId: state.activeBranchId,
    currentRevision: state.revision,
    publishedSnapshotId: state.publishedSnapshotId,
    branches: state.branches,
    revisions: state.revisions,
    snapshots: state.snapshots
  })

export const ProfileProviderEntityLayer = ProfileProviderEntity.toLayer(
  Effect.gen(function* () {
    const address = yield* Entity.CurrentAddress
    const snapshots = yield* ProfileProviderSnapshots
    const initial = yield* snapshots.load(address.entityId).pipe(
      Effect.orElse(() => Effect.succeed(Option.none<SnapshotEntry>()))
    )
    const stateRef = yield* Ref.make(
      Option.match(initial, { onNone: () => initialProfileState, onSome: ({ state }) => state })
    )

    const dispatch = (command: WriteCommand) =>
      Effect.gen(function* () {
        const state = yield* Ref.get(stateRef)
        const result = yield* handle(state, command).pipe(
          Effect.mapError((error) =>
            error instanceof ProfileError ? error : new ProfileError({ message: String(error) })
          )
        )
        yield* Ref.set(stateRef, result.state)
        if (result.state.revision > 0 && result.state.revision % SNAPSHOT_EVERY === 0) {
          yield* snapshots.save(address.entityId, result.state, result.state.revision).pipe(
            Effect.tapError((error) => Effect.logWarning(`[profile-provider] snapshot save failed: ${String(error)}`)),
            Effect.ignore
          )
        }
        return new CommandResult({
          profileId: address.entityId,
          branchId: result.state.activeBranchId,
          revision: result.state.revision
        })
      })

    return ProfileProviderEntity.of({
      CreateProfile: (req) => dispatch(new CreateProfile(req.payload)),
      MergeProfileData: (req) => dispatch(new MergeProfileData(req.payload)),
      ForkProfileBranch: (req) => dispatch(new ForkProfileBranch(req.payload)),
      CreateProfileSnapshot: (req) => dispatch(new CreateProfileSnapshot(req.payload)),
      PublishProfileSnapshot: (req) => dispatch(new PublishProfileSnapshot(req.payload)),
      GetProfile: (_req) =>
        Effect.gen(function* () {
          const state = yield* Ref.get(stateRef)
          if (state.status === "empty") {
            return yield* new ProfileNotFound({ profileId: address.entityId })
          }
          return state
        }),
      GetProfileHistory: (_req) =>
        Effect.gen(function* () {
          const state = yield* Ref.get(stateRef)
          if (state.status === "empty") {
            return yield* new ProfileNotFound({ profileId: address.entityId })
          }
          return toHistory(state)
        })
    })
  }),
  {
    maxIdleTime: "10 minutes",
    concurrency: "unbounded"
  }
)

export const ProfileProviderProxyRpcs = EntityProxy.toRpcGroup(ProfileProviderEntity)
export const ProfileProviderProxyHandlers = EntityProxyServer.layerRpcHandlers(ProfileProviderEntity)

export const ProfileProviderHandlersRaw = ProfileProviderRpcs.toLayer(
  Effect.gen(function* () {
    const store = yield* SynchronizedRef.make(new Map<string, ProfileEntry>())
    const snapshots = yield* ProfileProviderSnapshots
    const publish = yield* EventLogApi.makeClient(ProfileProviderEventLogSchema)

    const publishEvent = (event: ProfileEvent) => {
      switch (event._tag) {
        case "ProfileCreated":
          return publish("ProfileCreated", event)
        case "MergedDataProfile":
          return publish("MergedDataProfile", event)
        case "SnapshotCreatedProfile":
          return publish("SnapshotCreatedProfile", event)
        case "MetaDataCreated":
          return publish("MetaDataCreated", event)
        case "PersonalDataExtracted":
          return publish("PersonalDataExtracted", event)
        case "ProfileBranchForked":
          return publish("ProfileBranchForked", event)
        case "SnapshotPublishedProfile":
          return publish("SnapshotPublishedProfile", event)
      }
    }

    const getOrLoad = (map: Map<string, ProfileEntry>, profileId: string): Effect.Effect<ProfileEntry> =>
      map.has(profileId)
        ? Effect.succeed(map.get(profileId)!)
        : snapshots.load(profileId).pipe(
            Effect.map(Option.getOrElse(() => ({ state: initialProfileState, revision: 0 }))),
            Effect.orElse(() => Effect.succeed({ state: initialProfileState, revision: 0 }))
          )

    const runCommand = (profileId: string, command: WriteCommand) => {
      const tag = command._tag
      return Effect.gen(function* () {
        const [result, events, nextState] = yield* SynchronizedRef.modifyEffect(store, (map) =>
          getOrLoad(map, profileId).pipe(
            Effect.flatMap(({ state }) =>
              handle(state, command).pipe(
                Effect.mapError((error) =>
                  error instanceof ProfileError ? error : new ProfileError({ message: String(error) })
                ),
                Effect.map(({ events, state: next }) => {
                  const nextMap = new Map(map).set(profileId, { state: next, revision: next.revision })
                  return [
                    [
                      new CommandResult({
                        profileId,
                        branchId: next.activeBranchId,
                        revision: next.revision
                      }),
                      events,
                      next
                    ] as const,
                    nextMap
                  ] as const
                })
              )
            )
          )
        )
        yield* Effect.forEach(events, publishEvent, { discard: true }).pipe(
          Effect.retry(publishRetry),
          Effect.tapError((error) => Effect.logError(`[profile-provider] event publish failed: ${String(error)}`)),
          Effect.ignore
        )
        if (result.revision > 0 && result.revision % SNAPSHOT_EVERY === 0) {
          yield* snapshots.save(profileId, nextState, nextState.revision).pipe(
            Effect.tapError((error) => Effect.logWarning(`[profile-provider] snapshot save failed: ${String(error)}`)),
            Effect.ignore
          )
        }
        return result
      }).pipe(
        Metric.trackDuration(Metric.tagged(commandLatency, "command", tag)),
        Effect.tap(() => Metric.increment(Metric.tagged(commandTotal, "command", tag))),
        Effect.tapError(() => Metric.increment(Metric.tagged(commandErrors, "command", tag)))
      )
    }

    const getProfileState = (profileId: string) =>
      SynchronizedRef.modifyEffect(store, (map) => {
        if (map.has(profileId)) {
          const entry = map.get(profileId)!
          return entry.state.status === "empty"
            ? Effect.fail(new ProfileNotFound({ profileId }))
            : Effect.succeed([entry.state, map] as const)
        }
        return snapshots.load(profileId).pipe(
          Effect.flatMap(Option.match({
            onNone: () => Effect.fail(new ProfileNotFound({ profileId })),
            onSome: ({ state, revision }) => {
              if (state.status === "empty") return Effect.fail(new ProfileNotFound({ profileId }))
              return Effect.succeed([
                state,
                new Map(map).set(profileId, { state, revision })
              ] as const)
            }
          })),
          Effect.orElseFail(() => new ProfileNotFound({ profileId }))
        )
      })

    return ProfileProviderRpcs.of({
      CreateProfile: (payload) => runCommand(payload.profileId, new CreateProfile(payload)),
      MergeProfileData: (payload) => runCommand(payload.profileId, new MergeProfileData(payload)),
      ForkProfileBranch: (payload) => runCommand(payload.profileId, new ForkProfileBranch(payload)),
      CreateProfileSnapshot: (payload) => runCommand(payload.profileId, new CreateProfileSnapshot(payload)),
      PublishProfileSnapshot: (payload) => runCommand(payload.profileId, new PublishProfileSnapshot(payload)),
      GetProfile: (payload) => getProfileState(payload.profileId),
      GetProfileHistory: (payload) =>
        getProfileState(payload.profileId).pipe(
          Effect.map(toHistory)
        )
    })
  })
)

export const ProfileProviderHandlers = Layer.provide(ProfileProviderHandlersRaw, InfrastructureLayer)
