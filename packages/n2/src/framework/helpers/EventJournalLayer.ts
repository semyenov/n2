import * as Layer from "effect/Layer"
import * as EventJournal from "@effect/experimental/EventJournal"
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
    SqlEventJournal.make({ entryTable, remotesTable })
  )
}
