import { makeEventMessage, type EventMessagePayload } from "./EventMessage.js"

export interface EventMessageFactoryConfig<EntityIdKey extends string, Message> {
  readonly topic: string
  readonly entityIdKey: EntityIdKey
  readonly schema: new (fields: EventMessagePayload<EntityIdKey>) => Message
}

export type EventMessageFactoryOptions<EntityIdKey extends string> = {
  readonly [K in EntityIdKey]: string
} & {
  readonly revision: number
  readonly eventType: string
  readonly occurredAt: string
  readonly payload: unknown
  readonly headers?: Record<string, unknown>
}

export const makeEventMessageFactory = <const EntityIdKey extends string, Message>(
  config: EventMessageFactoryConfig<EntityIdKey, Message>
) =>
(options: EventMessageFactoryOptions<EntityIdKey>): Message => {
  const entityId = (options as Record<string, unknown>)[config.entityIdKey] as string
  return new config.schema(makeEventMessage({
    topic: config.topic,
    entityIdKey: config.entityIdKey,
    entityId,
    revision: options.revision,
    eventType: options.eventType,
    occurredAt: options.occurredAt,
    payload: options.payload,
    headers: options.headers
  }))
}
