import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { EntityProxy, EntityProxyServer } from "@effect/cluster"
import * as EventLogApi from "@effect/experimental/EventLog"
import * as Schedule from "effect/Schedule"
import { ProfileProvider } from "./aggregate.js"
import { InfrastructureLayer } from "./layers.js"
import { ProfileProviderEventLogSchema } from "./events.js"
import { ProfileProviderSnapshots, SNAPSHOT_EVERY } from "./snapshots.js"
import {
  type ProfileCommand,
  type ProfileEvent,
  CommandResult,
  ProfileError,
  ProfileHistory,
  ProfileNotFound,
  ProfileProviderEntity,
  ProfileProviderRpcs,
  ProfileState,
  SourceAsset
} from "./contracts.js"

const mergeSourceAssets = (
  current: ReadonlyArray<SourceAsset>,
  incoming: ReadonlyArray<SourceAsset>
) => {
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

const toCommandResult = ({ entityId, state }: {
  readonly entityId: string
  readonly state: ProfileState
}) =>
  new CommandResult({
    profileId: entityId,
    branchId: state.activeBranchId,
    revision: state.revision
  })

const toProfileError = (error: unknown) =>
  error instanceof ProfileError
    ? error
    : new ProfileError({ message: String(error) })

const formatUnknown = (error: unknown) => {
  if (error instanceof Error) return error.message
  if (typeof error === "string") return error
  const encoded = JSON.stringify(error)
  return encoded === undefined ? Object.prototype.toString.call(error) : encoded
}

const getProfileOrNotFound = (profileId: string, state: ProfileState) =>
  state.status === "empty"
    ? Effect.fail(new ProfileNotFound({ profileId }))
    : Effect.succeed(state)

const getHistoryOrNotFound = (profileId: string, state: ProfileState) =>
  state.status === "empty"
    ? Effect.fail(new ProfileNotFound({ profileId }))
    : Effect.succeed(toHistory(state))

const snapshotOps = {
  load: (entityId: string) =>
    Effect.flatMap(ProfileProviderSnapshots, (s) => s.load(entityId)),
  save: (entityId: string, state: ProfileState, revision: number) =>
    Effect.flatMap(ProfileProviderSnapshots, (s) =>
      s.save(entityId, state, revision)
    ),
  every: SNAPSHOT_EVERY
}

const postHandle = ({
  command,
  state,
}: {
  command: ProfileCommand
  events: ReadonlyArray<ProfileEvent>
  state: ProfileState
  entityId: string
}) => {
  if (command._tag === "CreateProfile" || command._tag === "MergeProfileData") {
    return new ProfileState({
      status: state.status,
      profileId: state.profileId,
      ownerAgentId: state.ownerAgentId,
      activeBranchId: state.activeBranchId,
      currentSchemaVersion: state.currentSchemaVersion,
      maskedProfileJson: state.maskedProfileJson,
      latestMetadataJson: state.latestMetadataJson,
      latestPiiStorageKey: state.latestPiiStorageKey,
      piiJurisdiction: state.piiJurisdiction,
      sourceAssets: mergeSourceAssets(state.sourceAssets, command.sources),
      branches: state.branches,
      revisions: state.revisions,
      snapshots: state.snapshots,
      publishedSnapshotId: state.publishedSnapshotId,
      revision: state.revision
    })
  }
  return state
}

const readQueryOverrides = {
  GetProfile: (
    _command: ProfileCommand,
    ctx: {
      readonly entityId: string
      readonly getState: Effect.Effect<ProfileState>
    }
  ) =>
    Effect.flatMap(ctx.getState, (state) =>
      getProfileOrNotFound(ctx.entityId, state)
    ),
  GetProfileHistory: (
    _command: ProfileCommand,
    ctx: {
      readonly entityId: string
      readonly getState: Effect.Effect<ProfileState>
    }
  ) =>
    Effect.flatMap(ctx.getState, (state) =>
      getHistoryOrNotFound(ctx.entityId, state)
    )
}

const statefulReadQueryOverrides = {
  GetProfile: (command: ProfileCommand, ctx: {
    readonly getState: (entityId: string) => Effect.Effect<ProfileState, unknown, unknown>
  }) =>
    ctx.getState(command.profileId).pipe(
      Effect.flatMap((state) => getProfileOrNotFound(command.profileId, state))
    ),
  GetProfileHistory: (command: ProfileCommand, ctx: {
    readonly getState: (entityId: string) => Effect.Effect<ProfileState, unknown, unknown>
  }) =>
    ctx.getState(command.profileId).pipe(
      Effect.flatMap((state) => getHistoryOrNotFound(command.profileId, state))
    )
}

export const ProfileProviderEntityLayer = ProfileProvider.toEntityLayer(
  ProfileProviderEntity,
  {
    postHandle,
    toResult: toCommandResult,
    toError: toProfileError,
    snapshots: snapshotOps,
    overrides: readQueryOverrides
  },
  {
    maxIdleTime: "10 minutes",
    concurrency: "unbounded"
  }
)

export const ProfileProviderProxyRpcs = EntityProxy.toRpcGroup(
  ProfileProviderEntity
)
export const ProfileProviderProxyHandlers = EntityProxyServer.layerRpcHandlers(
  ProfileProviderEntity
)

const publishRetry = Schedule.exponential("100 millis").pipe(
  Schedule.jittered,
  Schedule.intersect(Schedule.recurs(3))
)

export const ProfileProviderHandlersRaw = ProfileProvider.toStatefulRpcHandlers(
  ProfileProviderRpcs,
  {
    entityId: (command) => command.profileId,
    toResult: toCommandResult,
    toError: toProfileError,
    snapshots: snapshotOps,
    postHandle,
    afterCommit: ({ events }) =>
      Effect.gen(function* () {
        const publish = yield* EventLogApi.makeClient(
          ProfileProviderEventLogSchema
        )
        yield* Effect.forEach(events, (event) => publish(event._tag, event), {
          discard: true
        })
      }).pipe(
        Effect.retry(publishRetry),
        Effect.tapError((error) =>
          Effect.logError(
            `[profile-provider] event publish failed: ${formatUnknown(error)}`
          )
        )
      ),
    metrics: { prefix: "profile_provider" },
    overrides: statefulReadQueryOverrides
  }
)

export const ProfileProviderHandlers = Layer.provide(
  ProfileProviderHandlersRaw,
  InfrastructureLayer
)
