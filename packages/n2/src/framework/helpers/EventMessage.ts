import * as Schema from "effect/Schema"

export const EventMessageHeaders = Schema.Record({ key: Schema.String, value: Schema.Unknown })
const EventMessageRevision = Schema.Number.pipe(Schema.int())

export type EventMessageFields<EntityIdKey extends string> = {
  readonly id: typeof Schema.String
  readonly topic: typeof Schema.String
  readonly partitionKey: typeof Schema.String
  readonly eventType: typeof Schema.String
  readonly revision: Schema.Schema<number>
  readonly occurredAt: typeof Schema.String
  readonly payload: typeof Schema.Unknown
  readonly headers: typeof EventMessageHeaders
} & {
  readonly [K in EntityIdKey]: typeof Schema.UUID
}

const keyed = <const Key extends string, Value>(
  key: Key,
  value: Value
): { readonly [K in Key]: Value } =>
  // TypeScript cannot prove that a computed property creates this mapped type.
  ({ [key]: value }) as { readonly [K in Key]: Value }

export const makeEventMessageFields = <const EntityIdKey extends string>(
  entityIdKey: EntityIdKey
): EventMessageFields<EntityIdKey> => ({
  id: Schema.String,
  topic: Schema.String,
  partitionKey: Schema.String,
  eventType: Schema.String,
  revision: EventMessageRevision,
  occurredAt: Schema.String,
  payload: Schema.Unknown,
  headers: EventMessageHeaders,
  ...keyed(entityIdKey, Schema.UUID)
})

export interface EventMessageOptions<EntityIdKey extends string> {
  readonly topic: string
  readonly entityIdKey: EntityIdKey
  readonly entityId: string
  readonly revision: number
  readonly eventType: string
  readonly occurredAt: string
  readonly payload: unknown
  readonly headers?: Record<string, unknown>
}

export type EventMessagePayload<EntityIdKey extends string> = {
  readonly id: string
  readonly topic: string
  readonly partitionKey: string
  readonly eventType: string
  readonly revision: number
  readonly occurredAt: string
  readonly payload: unknown
  readonly headers: Record<string, unknown>
} & {
  readonly [K in EntityIdKey]: string
}

export const makeEventMessage = <const EntityIdKey extends string>(
  options: EventMessageOptions<EntityIdKey>
): EventMessagePayload<EntityIdKey> => ({
  id: `${options.entityId}:${options.revision}:${options.eventType}`,
  topic: options.topic,
  partitionKey: options.entityId,
  eventType: options.eventType,
  revision: options.revision,
  occurredAt: options.occurredAt,
  payload: options.payload,
  headers: {
    eventType: options.eventType,
    [options.entityIdKey]: options.entityId,
    revision: options.revision,
    ...(options.headers ?? {})
  },
  ...keyed(options.entityIdKey, options.entityId)
})
