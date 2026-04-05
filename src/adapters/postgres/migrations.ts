/**
 * @since 1.0.0
 * @module migrations
 *
 * SQL table definitions for Postgres adapters.
 */

/**
 * @since 1.0.0
 * @category sql
 */
export const createEventsTable = `
  CREATE TABLE IF NOT EXISTS n2_events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        TEXT NOT NULL UNIQUE,
    stream_id       TEXT NOT NULL,
    aggregate_id    TEXT NOT NULL,
    aggregate_type  TEXT NOT NULL,
    revision        INTEGER NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata        JSONB NOT NULL DEFAULT '{}',
    payload         JSONB NOT NULL,
    UNIQUE(stream_id, revision)
  );

  CREATE INDEX IF NOT EXISTS idx_n2_events_stream_id ON n2_events(stream_id);
  CREATE INDEX IF NOT EXISTS idx_n2_events_aggregate_type ON n2_events(aggregate_type);
`

/**
 * @since 1.0.0
 * @category sql
 */
export const createSnapshotsTable = `
  CREATE TABLE IF NOT EXISTS n2_snapshots (
    aggregate_id    TEXT PRIMARY KEY,
    revision        INTEGER NOT NULL,
    state           JSONB NOT NULL,
    snapshot_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );
`

/**
 * @since 1.0.0
 * @category sql
 */
export const createCheckpointsTable = `
  CREATE TABLE IF NOT EXISTS n2_checkpoints (
    projector_name  TEXT NOT NULL,
    partition       INTEGER NOT NULL,
    "offset"        TEXT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(projector_name, partition)
  );
`

/**
 * @since 1.0.0
 * @category sql
 */
export const allMigrations = [
  createEventsTable,
  createSnapshotsTable,
  createCheckpointsTable
]
