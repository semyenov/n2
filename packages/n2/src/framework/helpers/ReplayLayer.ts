import * as Config from "effect/Config"
import * as Layer from "effect/Layer"
import { PgClient } from "@effect/sql-pg"
import {
  makeSqlEventJournalLayer,
  type EventJournalTableOptions
} from "./EventJournalLayer.js"

export interface ReplayInfrastructureConfig<
  PSO, PSE, PSR,
  CHO = never, CHE = never, CHR = never,
  CHBO = never, CHBE = never, CHBR = never
> {
  readonly clickhouseLayer?: Layer.Layer<CHO, CHE, CHR>
  readonly clickhouseBootstrapLayer?: Layer.Layer<CHBO, CHBE, CHBR>
  readonly projectionStoreLayer: Layer.Layer<PSO, PSE, PSR>
  readonly sqlLayer?: PgSqlLayer
  readonly eventJournal?: EventJournalTableOptions
}

const defaultSqlLayer = () =>
  PgClient.layerConfig(
    Config.map(Config.redacted("DATABASE_URL"), (url) => ({ url }))
  )

type PgSqlLayer = ReturnType<typeof defaultSqlLayer>

/**
 * Compose the standard replay infrastructure: SQL + EventJournal + optional
 * ClickHouse bootstrap + projection store. The projection store receives both
 * SQL and ClickHouse layers so services can replay into either backend.
 */
export const makeReplayInfrastructureLayer = <
  PSO, PSE, PSR,
  CHO = never, CHE = never, CHR = never,
  CHBO = never, CHBE = never, CHBR = never
>(
  config: ReplayInfrastructureConfig<PSO, PSE, PSR, CHO, CHE, CHR, CHBO, CHBE, CHBR>
) => {
  const sqlLayer = config.sqlLayer ?? defaultSqlLayer()
  const clickhouseLayer = config.clickhouseLayer ?? Layer.empty
  const clickhouseBootstrapLayer = config.clickhouseBootstrapLayer ?? Layer.empty
  const clickhouseReadyLayer = Layer.merge(
    clickhouseLayer,
    Layer.provide(clickhouseBootstrapLayer, clickhouseLayer)
  )
  const projectionStoreRequirements = Layer.merge(sqlLayer, clickhouseReadyLayer)

  return Layer.mergeAll(
    sqlLayer,
    Layer.provide(makeSqlEventJournalLayer(config.eventJournal), sqlLayer),
    clickhouseReadyLayer,
    Layer.provide(config.projectionStoreLayer, projectionStoreRequirements)
  )
}
