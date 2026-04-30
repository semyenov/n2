import type * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import type * as Layer from "effect/Layer"
import { EventLog } from "@effect/experimental"
import type { Event } from "@effect/experimental/Event"
import type { EventGroup } from "@effect/experimental/EventGroup"
import { wireProjectionHandler } from "./Projection.js"

type ProjectionStoreMethod<Store, EventInstance> = [Store] extends [never] ? string : Extract<{
  readonly [Method in keyof Store]: Store[Method] extends (
    event: EventInstance
  ) => Effect.Effect<void, unknown, unknown> ? Method : never
}[keyof Store], string>

export interface ProjectionHandlerEntry<EventInstance, Store = never> {
  readonly event: new (payload: never) => EventInstance
  readonly storeMethod: ProjectionStoreMethod<Store, EventInstance>
}

export type ProjectionHandlerMap<EventUnion extends { readonly _tag: string }, Store = never> = {
  readonly [Tag in EventUnion["_tag"]]: ProjectionHandlerEntry<Extract<EventUnion, { readonly _tag: Tag }>, Store>
}

type ProjectionHandlerRecord = Readonly<Record<
  string,
  {
    readonly event: new (payload: never) => { readonly _tag: string }
    readonly storeMethod: string
  }
>>

type ProjectionEventUnion<Handlers> = {
  readonly [Tag in keyof Handlers]: Handlers[Tag] extends {
    readonly event: new (payload: never) => infer EventInstance extends { readonly _tag: string }
  } ? EventInstance : never
}[keyof Handlers]

type ProjectionHandlerMapForStore<Handlers, Store> = {
  readonly [Tag in keyof Handlers]: Handlers[Tag] extends {
    readonly event: new (payload: never) => infer EventInstance extends { readonly _tag: string }
  } ? Extract<Tag, string> extends EventInstance["_tag"]
      ? EventInstance["_tag"] extends Extract<Tag, string>
        ? ProjectionHandlerEntry<EventInstance, Store>
        : never
      : never
    : never
}

type ProjectionHandlersForEventGroup<Events extends Event.Any> = {
  readonly [Tag in Event.Tag<Events>]: unknown
}

type ProjectionLayerInputConfig<
  Events extends Event.Any,
  StoreI,
  Store,
  OutboxI,
  Outbox,
  Message,
  Handlers extends ProjectionHandlerRecord
> = {
  readonly group: EventGroup<Events>
  readonly storeTag: Context.Tag<StoreI, Store>
  readonly outboxTag?: Context.Tag<OutboxI, Outbox>
  readonly makeMessage?: (event: ProjectionEventUnion<Handlers>) => Message
  readonly handlers: Handlers & ProjectionHandlerMapForStore<Handlers, Store> & ProjectionHandlersForEventGroup<Events>
}

export interface ProjectionLayerConfig<
  Events extends Event.Any,
  EventUnion extends { readonly _tag: string },
  StoreI,
  Store,
  OutboxI,
  Outbox,
  Message
> {
  readonly group: EventGroup<Events>
  readonly storeTag: Context.Tag<StoreI, Store>
  readonly outboxTag?: Context.Tag<OutboxI, Outbox>
  readonly makeMessage?: (event: EventUnion) => Message
  readonly handlers: ProjectionHandlerMap<EventUnion, Store>
}

type ProjectionLayerRequirements<Events extends Event.Any, StoreI, OutboxI> =
  | Event.Context<Events>
  | StoreI
  | OutboxI

type RuntimeProjectionEvent = { readonly _tag: string }

type RuntimeProjectionStore = Readonly<Record<
  string,
  (event: RuntimeProjectionEvent) => Effect.Effect<void, unknown, unknown>
>>

type RuntimeProjectionOutbox<Message> = {
  readonly enqueue: (message: Message) => Effect.Effect<void, unknown, unknown>
}

type RuntimeProjectionConfig<Message> = {
  readonly storeTag: Context.Tag<unknown, RuntimeProjectionStore>
  readonly outboxTag?: Context.Tag<unknown, RuntimeProjectionOutbox<Message>>
  readonly makeMessage?: (event: RuntimeProjectionEvent) => Message
  readonly handlers: Readonly<Record<string, ProjectionHandlerEntry<RuntimeProjectionEvent>>>
}

type HandlerChain = {
  readonly handle: (name: string, handler: unknown) => HandlerChain
}

const asRuntimeProjectionConfig = <
  Events extends Event.Any,
  StoreI,
  Store,
  OutboxI,
  Outbox,
  Message,
  Handlers extends ProjectionHandlerRecord
>(
  config: ProjectionLayerInputConfig<Events, StoreI, Store, OutboxI, Outbox, Message, Handlers>
): RuntimeProjectionConfig<Message> =>
  // EventLog.group exposes a dynamic handler builder. Keep that untyped edge
  // here so the public handler map can remain tag- and store-method-aware.
  config as unknown as RuntimeProjectionConfig<Message>

const asHandlerChain = (handlers: unknown): HandlerChain =>
  // The experimental EventLog handler builder is structurally a chained
  // `.handle(tag, handler)` API, but its concrete type is intentionally hidden.
  handlers as HandlerChain

const appendProjectionHandlers = <Message>(
  handlers: HandlerChain,
  config: RuntimeProjectionConfig<Message>
): HandlerChain => {
  let acc = handlers
  for (const [tag, entry] of Object.entries(config.handlers)) {
    const handler = wireProjectionHandler(
      config.storeTag,
      config.outboxTag,
      entry.event,
      entry.storeMethod,
      config.makeMessage
    )
    acc = acc.handle(tag, handler)
  }
  return acc
}

const asEventLogProjectionLayer = <
  Events extends Event.Any,
  Requirements
>(layer: unknown): Layer.Layer<Event.ToService<Events>, never, Requirements> =>
  // EventLog.group returns the right runtime layer, but the experimental API
  // does not retain the full generic relationship after dynamic handler loops.
  layer as Layer.Layer<Event.ToService<Events>, never, Requirements>

const asEventLogHandlerResult = (handlers: HandlerChain): never =>
  // EventLog.group expects the hidden builder return type. The handler chain is
  // complete at runtime; only the experimental return type is unavailable here.
  handlers as never

/**
 * Build an `EventLog.group` projection Layer from a declarative event-map.
 *
 * Each key in `handlers` must match a tag in `EventUnion`; missing tags are a
 * type error. Each entry binds an event constructor to a method name on the
 * projection store. Optional `outboxTag` + `makeMessage` mirror `wireProjectionHandler`'s
 * behaviour.
 */
export const makeProjectionLayer = <
  Events extends Event.Any,
  StoreI,
  Store,
  const Handlers extends ProjectionHandlerRecord,
  OutboxI = never,
  Outbox = never,
  Message = never
>(
  config: ProjectionLayerInputConfig<Events, StoreI, Store, OutboxI, Outbox, Message, Handlers>
): Layer.Layer<Event.ToService<Events>, never, ProjectionLayerRequirements<Events, StoreI, OutboxI>> =>
  asEventLogProjectionLayer<Events, ProjectionLayerRequirements<Events, StoreI, OutboxI>>(EventLog.group(
    config.group,
    (handlers) => {
      return asEventLogHandlerResult(appendProjectionHandlers(
        asHandlerChain(handlers),
        asRuntimeProjectionConfig(config)
      ))
    }
  ))
