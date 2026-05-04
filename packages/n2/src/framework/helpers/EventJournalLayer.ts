import * as Layer from "effect/Layer"
import * as Effect from "effect/Effect"
import * as EventJournal from "@effect/experimental/EventJournal"
import * as SqlClient from "@effect/sql/SqlClient"
import * as SqlEventJournal from "@effect/sql/SqlEventJournal"

export interface EventJournalTableOptions {
  readonly entryTable?: string
  readonly remotesTable?: string
}

export const makeSqlEventJournalLayer = (options?: EventJournalTableOptions) => {
  const entryTable = options?.entryTable ?? "effect_event_journal"
  const remotesTable = options?.remotesTable ?? "effect_event_remotes"

  return Layer.effect(
    EventJournal.EventJournal,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient

      // SqlEventJournal's PG DDL uses UUID ids, but EventJournal.Entry ids are bytes.
      yield* sql.onDialectOrElse({
        pg: () =>
          Effect.all([
            sql`
              CREATE TABLE IF NOT EXISTS ${sql(entryTable)} (
                id BYTEA PRIMARY KEY,
                event TEXT NOT NULL,
                primary_key TEXT NOT NULL,
                payload BYTEA NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL
              )
            `.withoutTransform,
            sql`
              CREATE TABLE IF NOT EXISTS ${sql(remotesTable)} (
                remote_id BYTEA NOT NULL,
                entry_id BYTEA NOT NULL,
                sequence INT NOT NULL,
                PRIMARY KEY (remote_id, entry_id)
              )
            `.withoutTransform
          ], { discard: true }),
        orElse: () => Effect.void
      })

      return yield* SqlEventJournal.make({ entryTable, remotesTable })
    })
  )
}
