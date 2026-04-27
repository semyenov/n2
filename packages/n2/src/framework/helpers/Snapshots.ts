/**
 * @since 1.0.0
 *
 * Generic snapshot service factory for event-sourced aggregates.
 * Provides SQL-based snapshot persistence using Schema codecs for state serialization.
 *
 * @example
 * ```ts
 * import { makeSnapshotService } from "@semyenov/n2/framework/helpers"
 *
 * export class MySnapshots extends Context.Tag("MySnapshots")<MySnapshots, SnapshotService<MyState>>() {}
 * export const MySnapshotsLive = makeSnapshotService({
 *   table: "my_snapshots",
 *   stateSchema: MyState,
 *   idColumn: "entity_id"
 * }).makeLive(MySnapshots)
 * ```
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Metric from "effect/Metric"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import type * as Context from "effect/Context"
import { SqlClient } from "@effect/sql/SqlClient"
import type { SqlError } from "@effect/sql/SqlError"

export interface SnapshotEntry<State> {
  readonly state: State
  readonly revision: number
}

export interface SnapshotService<State> {
  readonly load: (entityId: string) => Effect.Effect<Option.Option<SnapshotEntry<State>>, SqlError>
  readonly save: (entityId: string, state: State, revision: number) => Effect.Effect<void, SqlError>
}

/**
 * Build the snapshot hook object consumed by `Definition.toEntityLayer()`.
 *
 * Production services usually only need to pass a snapshot Context.Tag and a
 * cadence. This helper keeps entity wiring focused on business adapters while
 * preserving access to the lower-level `SnapshotService` when needed.
 */
export const makeSnapshotOps = <I, State>(
  tag: Context.Tag<I, SnapshotService<State>>,
  every: number
) => ({
  load: (entityId: string) =>
    Effect.flatMap(tag, (snapshots) => snapshots.load(entityId)),
  save: (entityId: string, state: State, revision: number) =>
    Effect.flatMap(tag, (snapshots) => snapshots.save(entityId, state, revision)),
  every
})

/**
 * Creates a snapshot service factory for a given state schema and table name.
 * Call `.makeLive(tag)` to produce a Layer for the given Context.Tag.
 */
export const makeSnapshotService = <State, Encoded>(config: {
  readonly table: string
  readonly stateSchema: Schema.Schema<State, Encoded>
  readonly idColumn?: string
  /** Optional metrics prefix. When set, emits `{prefix}.load.duration_ms` and `{prefix}.save.duration_ms` timers. */
  readonly metrics?: { readonly prefix: string }
}) => {
  const encode = Schema.encodeSync(config.stateSchema)
  const decode = Schema.decodeUnknownSync(config.stateSchema)
  const idCol = config.idColumn ?? "entity_id"
  const meters = config.metrics
    ? {
        loadLatency: Metric.timer(`${config.metrics.prefix}.load.duration_ms`, "milliseconds"),
        saveLatency: Metric.timer(`${config.metrics.prefix}.save.duration_ms`, "milliseconds"),
        loadErrors: Metric.counter(`${config.metrics.prefix}.load.errors`, { incremental: true }),
        saveErrors: Metric.counter(`${config.metrics.prefix}.save.errors`, { incremental: true })
      }
    : undefined

  return {
    makeLive: <I>(
      tag: Context.Tag<I, SnapshotService<State>>
    ): Layer.Layer<I, never, SqlClient> =>
      Layer.effect(
        tag,
        Effect.gen(function* () {
          const sql = yield* SqlClient

          const rawLoad = (entityId: string) =>
            sql`
              SELECT state_json, revision
              FROM ${sql(config.table)}
              WHERE ${sql(idCol)} = ${entityId}
            `.pipe(
              Effect.map((rows) => {
                const row = rows[0] as { state_json: string; revision: number } | undefined
                if (!row) return Option.none<SnapshotEntry<State>>()
                return Option.some({ state: decode(JSON.parse(row.state_json)), revision: row.revision })
              })
            )

          const rawSave = (entityId: string, state: State, revision: number) =>
            sql`
              INSERT INTO ${sql(config.table)} (${sql(idCol)}, state_json, revision, saved_at)
              VALUES (${entityId}, ${JSON.stringify(encode(state))}, ${revision}, ${new Date().toISOString()})
              ON CONFLICT (${sql(idCol)}) DO UPDATE SET
                state_json = EXCLUDED.state_json,
                revision = EXCLUDED.revision,
                saved_at = EXCLUDED.saved_at
            `.pipe(Effect.asVoid)

          return {
            load: meters
              ? (entityId: string) => rawLoad(entityId).pipe(
                  Metric.trackDuration(meters.loadLatency),
                  Effect.tapError(() => Metric.increment(meters.loadErrors))
                )
              : rawLoad,
            save: meters
              ? (entityId: string, state: State, revision: number) => rawSave(entityId, state, revision).pipe(
                  Metric.trackDuration(meters.saveLatency),
                  Effect.tapError(() => Metric.increment(meters.saveErrors))
                )
              : rawSave
          } satisfies SnapshotService<State>
        })
      )
  }
}
