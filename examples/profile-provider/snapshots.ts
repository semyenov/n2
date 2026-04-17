/**
 * Profile provider snapshot service — saves and loads aggregate state to/from PostgreSQL.
 *
 * Snapshot persistence now relies on the ProfileState schema codec directly:
 * Schema.encodeSync(ProfileState) produces a JSON-safe object and
 * Schema.decodeUnknownSync(ProfileState) restores nested DateTime values and
 * typed profile documents on load.
 */
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import { SqlClient } from "@effect/sql/SqlClient"
import type { SqlError } from "@effect/sql/SqlError"
import { ProfileState } from "./contracts.js"

export const SNAPSHOT_EVERY = 25

const encodeState = Schema.encodeSync(ProfileState)
const decodeState = Schema.decodeUnknownSync(ProfileState)

export type SnapshotEntry = { readonly state: ProfileState; readonly revision: number }

export class ProfileProviderSnapshots extends Context.Tag("ProfileProviderSnapshots")<
  ProfileProviderSnapshots,
  {
    readonly load: (profileId: string) => Effect.Effect<Option.Option<SnapshotEntry>, SqlError>
    readonly save: (profileId: string, state: ProfileState, revision: number) => Effect.Effect<void, SqlError>
  }
>() {}

export const ProfileProviderSnapshotsLive = Layer.effect(
  ProfileProviderSnapshots,
  Effect.gen(function* () {
    const sql = yield* SqlClient
    return {
      load: (profileId) =>
        sql`
          SELECT state_json, revision
          FROM profile_provider_snapshots
          WHERE profile_id = ${profileId}
        `.pipe(
          Effect.map((rows) => {
            const row = rows[0] as { state_json: string; revision: number } | undefined
            if (!row) return Option.none<SnapshotEntry>()
            return Option.some({ state: decodeState(JSON.parse(row.state_json)), revision: row.revision })
          })
        ),
      save: (profileId, state, revision) =>
        sql`
          INSERT INTO profile_provider_snapshots (profile_id, state_json, revision, saved_at)
          VALUES (${profileId}, ${JSON.stringify(encodeState(state))}, ${revision}, ${new Date().toISOString()})
          ON CONFLICT (profile_id) DO UPDATE SET
            state_json = EXCLUDED.state_json,
            revision = EXCLUDED.revision,
            saved_at = EXCLUDED.saved_at
        `.pipe(Effect.asVoid)
    }
  })
)
