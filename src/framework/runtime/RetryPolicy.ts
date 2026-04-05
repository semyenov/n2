/**
 * @since 1.0.0
 * @module RetryPolicy
 *
 * Standard retry policies for infrastructure operations.
 * Uses Effect's built-in Schedule combinators.
 */
import * as Schedule from "effect/Schedule"
import * as Duration from "effect/Duration"

/**
 * Kafka publish: 3 retries with exponential backoff (100ms, 200ms, 400ms).
 *
 * @since 1.0.0
 * @category policies
 */
export const kafkaPublish = Schedule.exponential(Duration.millis(100)).pipe(
  Schedule.intersect(Schedule.recurs(3))
)

/**
 * Database query: 2 retries with exponential backoff (50ms, 100ms).
 *
 * @since 1.0.0
 * @category policies
 */
export const database = Schedule.exponential(Duration.millis(50)).pipe(
  Schedule.intersect(Schedule.recurs(2))
)

/**
 * External service call: 5 retries with exponential backoff + jitter,
 * capped at 5 seconds between retries.
 *
 * @since 1.0.0
 * @category policies
 */
export const externalService = Schedule.exponential(Duration.millis(200)).pipe(
  Schedule.jittered,
  Schedule.intersect(Schedule.recurs(5)),
  Schedule.compose(Schedule.elapsed.pipe(
    Schedule.whileOutput(Duration.lessThanOrEqualTo(Duration.seconds(30)))
  ))
)

/**
 * Outbox poll: retry indefinitely with fixed 1s interval.
 *
 * @since 1.0.0
 * @category policies
 */
export const outboxPoll = Schedule.spaced(Duration.seconds(1))

/**
 * Projection subscription: 3 retries with exponential backoff.
 * After exhausting retries, the event goes to DLQ.
 *
 * @since 1.0.0
 * @category policies
 */
export const projection = Schedule.exponential(Duration.millis(100)).pipe(
  Schedule.intersect(Schedule.recurs(3))
)
