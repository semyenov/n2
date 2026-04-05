/**
 * @since 1.0.0
 * @module Metrics
 *
 * Application metrics using Effect's built-in Metric system.
 * Re-exports ClusterMetrics for entity/shard monitoring.
 */
import * as Metric from "effect/Metric"
import * as MetricBoundaries from "effect/MetricBoundaries"
import * as Duration from "effect/Duration"
import { ClusterMetrics } from "@effect/cluster"

// ---------------------------------------------------------------------------
// Command metrics
// ---------------------------------------------------------------------------

/**
 * Total commands processed (counter).
 * Tag with aggregate type for per-aggregate breakdown.
 *
 * @since 1.0.0
 * @category metrics
 */
export const commandsTotal = Metric.counter("n2.commands.total", {
  incremental: true
})

/**
 * Command processing duration (histogram).
 *
 * @since 1.0.0
 * @category metrics
 */
export const commandDuration = Metric.timer("n2.commands.duration")

/**
 * Command errors (counter).
 *
 * @since 1.0.0
 * @category metrics
 */
export const commandErrors = Metric.counter("n2.commands.errors", {
  incremental: true
})

// ---------------------------------------------------------------------------
// Projection metrics
// ---------------------------------------------------------------------------

/**
 * Events processed by projections (counter).
 *
 * @since 1.0.0
 * @category metrics
 */
export const projectionEventsProcessed = Metric.counter("n2.projection.events.processed", {
  incremental: true
})

// ---------------------------------------------------------------------------
// Outbox metrics
// ---------------------------------------------------------------------------

/**
 * Events published via outbox (counter).
 *
 * @since 1.0.0
 * @category metrics
 */
export const outboxPublished = Metric.counter("n2.outbox.published", {
  incremental: true
})

// ---------------------------------------------------------------------------
// Re-exports
// ---------------------------------------------------------------------------

/**
 * Cluster-level metrics (entities, singletons, runners, shards).
 *
 * @since 1.0.0
 * @category re-exports
 */
export { ClusterMetrics }
