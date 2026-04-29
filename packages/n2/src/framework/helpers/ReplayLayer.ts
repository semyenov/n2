import * as Config from "effect/Config"
import * as Layer from "effect/Layer"
import { PgClient } from "@effect/sql-pg"
import {
  makeSqlEventJournalLayer,
  type EventJournalTableOptions
} from "./EventJournalLayer.js"

export interface ReplayInfrastructureConfig<
  CHO, CHE, CHR,
  CHBO, CHBE, CHBR,
  PSO, PSE, PSR
> {
  readonly clickhouseLayer: Layer.Layer<CHO, CHE, CHR>
  readonly clickhouseBootstrapLayer: Layer.Layer<CHBO, CHBE, CHBR>
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
 * Compose the standard replay infrastructure: SQL + EventJournal + ClickHouse
 * (with one-time bootstrap) + projection store. The ClickHouse-ready layer is
 * built exactly once and provided to both the projection store and the merged
 * output, eliminating the duplication seen in hand-rolled replay scripts.
 */
export const makeReplayInfrastructureLayer = <
  CHO, CHE, CHR,
  CHBO, CHBE, CHBR,
  PSO, PSE, PSR
>(
  config: ReplayInfrastructureConfig<CHO, CHE, CHR, CHBO, CHBE, CHBR, PSO, PSE, PSR>
) => {
  const sqlLayer = config.sqlLayer ?? defaultSqlLayer()
  const clickhouseReadyLayer = Layer.merge(
    config.clickhouseLayer,
    Layer.provide(config.clickhouseBootstrapLayer, config.clickhouseLayer)
  )

  return Layer.mergeAll(
    Layer.provide(makeSqlEventJournalLayer(config.eventJournal), sqlLayer),
    clickhouseReadyLayer,
    Layer.provide(config.projectionStoreLayer, clickhouseReadyLayer)
  )
}
