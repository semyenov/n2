/**
 * @since 1.0.0
 * @module PgCheckpointStore
 *
 * CheckpointStore implementation using @effect/sql-pg.
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import { SqlClient } from "@effect/sql"
import {
  CheckpointStore,
  type CheckpointStoreService
} from "../../framework/projection/CheckpointStore.js"

/**
 * @since 1.0.0
 * @category layers
 */
export const layer: Layer.Layer<CheckpointStore, never, SqlClient.SqlClient> =
  Layer.effect(
    CheckpointStore,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient

      const load = (
        projectorName: string,
        partition: number
      ): Effect.Effect<Option.Option<string>> =>
        Effect.gen(function*() {
          const rows: ReadonlyArray<any> = yield* sql`
            SELECT "offset"
            FROM n2_checkpoints
            WHERE projector_name = ${projectorName}
            AND partition = ${partition}
          `.pipe(Effect.orDie)

          if (rows.length === 0) return Option.none()
          return Option.some(String(rows[0].offset))
        })

      const save = (
        projectorName: string,
        partition: number,
        offset: string
      ): Effect.Effect<void> =>
        sql`
          INSERT INTO n2_checkpoints (projector_name, partition, "offset", updated_at)
          VALUES (${projectorName}, ${partition}, ${offset}, NOW())
          ON CONFLICT (projector_name, partition)
          DO UPDATE SET "offset" = ${offset}, updated_at = NOW()
        `.pipe(Effect.orDie, Effect.asVoid)

      return { load, save } satisfies CheckpointStoreService
    })
  )
