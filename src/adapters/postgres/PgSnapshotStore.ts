/**
 * @since 1.0.0
 * @module PgSnapshotStore
 *
 * SnapshotStore implementation using @effect/sql-pg.
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import type { EntityId } from "@effect/cluster/EntityId"
import { SqlClient } from "@effect/sql"
import {
  SnapshotStore,
  type SnapshotStoreService,
  type SnapshotData
} from "../../framework/runtime/SnapshotStore.js"
import { type Revision, make as makeRevision } from "../../framework/domain/Revision.js"

/**
 * @since 1.0.0
 * @category layers
 */
export const layer: Layer.Layer<SnapshotStore, never, SqlClient.SqlClient> =
  Layer.effect(
    SnapshotStore,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient

      const load = (
        aggregateId: EntityId
      ): Effect.Effect<Option.Option<SnapshotData>> =>
        Effect.gen(function*() {
          const rows: ReadonlyArray<any> = yield* sql`
            SELECT revision, state
            FROM n2_snapshots
            WHERE aggregate_id = ${aggregateId}
          `.pipe(Effect.orDie)

          if (rows.length === 0) return Option.none()
          const row = rows[0]
          return Option.some({
            state: row.state,
            revision: makeRevision(Number(row.revision))
          })
        })

      const save = (
        aggregateId: EntityId,
        state: unknown,
        revision: Revision
      ): Effect.Effect<void> =>
        sql`
          INSERT INTO n2_snapshots (aggregate_id, revision, state, snapshot_at)
          VALUES (${aggregateId}, ${revision}, ${JSON.stringify(state)}::jsonb, NOW())
          ON CONFLICT (aggregate_id)
          DO UPDATE SET revision = ${revision}, state = ${JSON.stringify(state)}::jsonb, snapshot_at = NOW()
        `.pipe(Effect.orDie, Effect.asVoid)

      return { load, save } satisfies SnapshotStoreService
    })
  )
