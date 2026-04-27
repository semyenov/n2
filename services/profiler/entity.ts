import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import { Entity, EntityProxy, EntityProxyServer } from "@effect/cluster"
import * as EventLogApi from "@effect/experimental/EventLog"
import * as Schedule from "effect/Schedule"
import { ProfileProvider, initialProfileState } from "./aggregate.js"
import { InfrastructureLayer } from "./layers.js"
import { ProfileProviderEventGroup, ProfileProviderEventLogSchema } from "./events.js"
import { ProfileProviderSnapshots, SNAPSHOT_EVERY } from "./snapshots.js"
import {
  type ProfileCommand,
  type ProfileEvent,
  type ProfileState,
  CommandResult,
  CreateProfile,
  MergeProfileData,
  ProfileError,
  ProfileHistory,
  ProfileNotFound,
  ProfileProviderEntity,
  ProfileProviderRpcs,
  SourceAsset
} from "./contracts.js"

const mergeSourceAssets = (current: ReadonlyArray<SourceAsset>, incoming: ReadonlyArray<SourceAsset>) => {
  const next = new Map(current.map((asset) => [asset.sourceId, asset]))
  for (const asset of incoming) {
    next.set(asset.sourceId, asset)
  }
  return Array.from(next.values())
}

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

const snapshotOps = {
  load: (entityId: string) =>
    Effect.flatMap(ProfileProviderSnapshots, (s) => s.load(entityId)),
  save: (entityId: string, state: ProfileState, revision: number) =>
    Effect.flatMap(ProfileProviderSnapshots, (s) => s.save(entityId, state, revision)),
  every: SNAPSHOT_EVERY
}

const postHandle = ({ command, state }: { command: ProfileCommand; events: ReadonlyArray<ProfileEvent>; state: ProfileState; entityId: string }) => {
  if (command._tag === "CreateProfile" || command._tag === "MergeProfileData") {
    return { ...state, sourceAssets: mergeSourceAssets(state.sourceAssets, command.sources) }
  }
  return state
}

export const ProfileProviderEntityLayer = ProfileProvider.toEntityLayer(
  ProfileProviderEntity,
  {
    toResult: ({ entityId, state }) =>
      new CommandResult({ profileId: entityId, branchId: state.activeBranchId, revision: state.revision }),
    toError: (error) =>
      error instanceof ProfileError ? error : new ProfileError({ message: String(error) }),
    snapshots: snapshotOps,
    postHandle,
    overrides: {
      GetProfile: (command, ctx) =>
        ctx.getState(command.profileId).pipe(
          Effect.flatMap((state) =>
            state.status === "empty"
              ? Effect.fail(new ProfileNotFound({ profileId: command.profileId }))
              : Effect.succeed(state)
          )
        ),
      GetProfileHistory: (command, ctx) =>
        ctx.getState(command.profileId).pipe(
          Effect.flatMap((state) =>
            state.status === "empty"
              ? Effect.fail(new ProfileNotFound({ profileId: command.profileId }))
              : Effect.succeed(toHistory(state))
          )
        )
    },
  },
  { maxIdleTime: "10 minutes", concurrency: "unbounded" }
)

export const ProfileProviderProxyRpcs = EntityProxy.toRpcGroup(ProfileProviderEntity)
export const ProfileProviderProxyHandlers = EntityProxyServer.layerRpcHandlers(ProfileProviderEntity)

const publishRetry = Schedule.exponential("100 millis").pipe(
  Schedule.jittered,
  Schedule.intersect(Schedule.recurs(3))
)

export const ProfileProviderHandlersRaw = ProfileProvider.toStatefulRpcHandlers(
  ProfileProviderRpcs,
  {
    entityId: (command) => command.profileId,
    toResult: ({ entityId, state }) =>
      new CommandResult({ profileId: entityId, branchId: state.activeBranchId, revision: state.revision }),
    toError: (error) =>
      error instanceof ProfileError ? error : new ProfileError({ message: String(error) }),
    snapshots: snapshotOps,
    postHandle,
    afterCommit: ({ events }) =>
      Effect.gen(function* () {
        const publish = yield* EventLogApi.makeClient(ProfileProviderEventLogSchema)
        type EventTag = ProfileEvent["_tag"]
        yield* Effect.forEach(
          events,
          (event) => publish(event._tag, event),
          { discard: true }
        )
      }).pipe(
        Effect.retry(publishRetry),
        Effect.tapError((error) => Effect.logError(`[profile-provider] event publish failed: ${String(error)}`))
      ),
    metrics: { prefix: "profile_provider" },
    overrides: {
      GetProfile: (command, ctx) =>
        ctx.getState(command.profileId).pipe(
          Effect.flatMap((state) =>
            state.status === "empty"
              ? Effect.fail(new ProfileNotFound({ profileId: command.profileId }))
              : Effect.succeed(state)
          )
        ),
      GetProfileHistory: (command, ctx) =>
        ctx.getState(command.profileId).pipe(
          Effect.flatMap((state) =>
            state.status === "empty"
              ? Effect.fail(new ProfileNotFound({ profileId: command.profileId }))
              : Effect.succeed(toHistory(state))
          )
        )
    }
  }
)

export const ProfileProviderHandlers = Layer.provide(ProfileProviderHandlersRaw, InfrastructureLayer)
