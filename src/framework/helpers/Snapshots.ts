/**
 * @since 1.0.0
 *
 * Generic snapshot service factory for event-sourced aggregates.
 * Provides SQL-based snapshot persistence using Schema codecs for state serialization.
 *
 * @example
 * ```ts
 * import { makeSnapshotService } from "n2/framework/helpers"
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
 * Creates a snapshot service factory for a given state schema and table name.
 * Call `.makeLive(tag)` to produce a Layer for the given Context.Tag.
 */
export const makeSnapshotService = <State, Encoded>(config: {
  readonly table: string
  readonly stateSchema: Schema.Schema<State, Encoded>
  readonly idColumn?: string
}) => {
  const encode = Schema.encodeSync(config.stateSchema)
  const decode = Schema.decodeUnknownSync(config.stateSchema)
  const idCol = config.idColumn ?? "entity_id"

  return {
    makeLive: <I>(
      tag: Context.Tag<I, SnapshotService<State>>
    ): Layer.Layer<I, never, SqlClient> =>
      Layer.effect(
        tag,
        Effect.gen(function* () {
          const sql = yield* SqlClient
          return {
            load: (entityId: string) =>
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
              ),
            save: (entityId: string, state: State, revision: number) =>
              sql`
                INSERT INTO ${sql(config.table)} (${sql(idCol)}, state_json, revision, saved_at)
                VALUES (${entityId}, ${JSON.stringify(encode(state))}, ${revision}, ${new Date().toISOString()})
                ON CONFLICT (${sql(idCol)}) DO UPDATE SET
                  state_json = EXCLUDED.state_json,
                  revision = EXCLUDED.revision,
                  saved_at = EXCLUDED.saved_at
              `.pipe(Effect.asVoid)
          } satisfies SnapshotService<State>
        })
      )
  }
}
