import { it, expect } from "@effect/vitest"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import type * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { EventGroup } from "@effect/experimental"
import { makeProjectionLayer } from "./ProjectionLayer.js"

type LayerRequirements<T> = T extends Layer.Layer<infer _Out, infer _Error, infer Requirements> ? Requirements : never
type IsNever<T> = [T] extends [never] ? true : false
type Includes<Whole, Part> = Part extends Whole ? true : false
type Assert<T extends true> = T

const assertType = <_T extends true>() => undefined

class FooCreated extends Schema.TaggedClass<FooCreated>()("FooCreated", {
  id: Schema.String
}) {}

class FooUpdated extends Schema.TaggedClass<FooUpdated>()("FooUpdated", {
  id: Schema.String
}) {}

type FooEvent = FooCreated | FooUpdated

const FooEventGroup = EventGroup.empty
  .add({
    tag: "FooCreated",
    primaryKey: (p: { id: string }) => p.id,
    payload: Schema.Struct({ id: Schema.String })
  })
  .add({
    tag: "FooUpdated",
    primaryKey: (p: { id: string }) => p.id,
    payload: Schema.Struct({ id: Schema.String })
  })

interface FooStoreShape {
  readonly onFooCreated: (event: FooCreated) => Effect.Effect<void, never, never>
  readonly onFooUpdated: (event: FooUpdated) => Effect.Effect<void, never, never>
}

class FooStore extends Context.Tag("FooStore")<FooStore, FooStoreShape>() {}

interface FooOutboxShape {
  readonly enqueue: (message: { readonly id: string }) => Effect.Effect<void, never, never>
}

class FooOutbox extends Context.Tag("FooOutbox")<FooOutbox, FooOutboxShape>() {}

it("makeProjectionLayer composes a Layer with declarative handler map", () => {
  const layer = makeProjectionLayer({
    group: FooEventGroup,
    storeTag: FooStore,
    handlers: {
      FooCreated: { event: FooCreated, storeMethod: "onFooCreated" },
      FooUpdated: { event: FooUpdated, storeMethod: "onFooUpdated" }
    } satisfies import("./ProjectionLayer.js").ProjectionHandlerMap<FooEvent, FooStoreShape>
  })
  type Requirements = LayerRequirements<typeof layer>
  type StoreRequirementIsPreserved = Assert<Includes<Requirements, FooStore>>
  // @ts-expect-error projection layers must not erase their store requirement
  type StoreRequirementIsNotErased = Assert<IsNever<Requirements>>
  assertType<StoreRequirementIsPreserved>()
  expect(layer).toBeDefined()
})

it("makeProjectionLayer preserves optional outbox requirements", () => {
  const layer = makeProjectionLayer({
    group: FooEventGroup,
    storeTag: FooStore,
    outboxTag: FooOutbox,
    makeMessage: (event: FooEvent) => ({ id: event.id }),
    handlers: {
      FooCreated: { event: FooCreated, storeMethod: "onFooCreated" },
      FooUpdated: { event: FooUpdated, storeMethod: "onFooUpdated" }
    } satisfies import("./ProjectionLayer.js").ProjectionHandlerMap<FooEvent, FooStoreShape>
  })
  type Requirements = LayerRequirements<typeof layer>
  type StoreRequirementIsPreserved = Assert<Includes<Requirements, FooStore>>
  type OutboxRequirementIsPreserved = Assert<Includes<Requirements, FooOutbox>>
  assertType<StoreRequirementIsPreserved>()
  assertType<OutboxRequirementIsPreserved>()
  expect(layer).toBeDefined()
})

it("makeProjectionLayer rejects handler maps missing a tag", () => {
  const layer = makeProjectionLayer({
    group: FooEventGroup,
    storeTag: FooStore,
    // @ts-expect-error missing FooUpdated entry
    handlers: {
      FooCreated: { event: FooCreated, storeMethod: "onFooCreated" }
    }
  })
  expect(layer).toBeDefined()
})

it("makeProjectionLayer infers store methods from the store tag", () => {
  const layer = makeProjectionLayer({
    group: FooEventGroup,
    storeTag: FooStore,
    makeMessage: (event: FooEvent) => ({ id: event.id }),
    handlers: {
      FooCreated: { event: FooCreated, storeMethod: "onFooCreated" },
      // @ts-expect-error storeMethod must accept FooUpdated
      FooUpdated: { event: FooUpdated, storeMethod: "onFooCreated" }
    }
  })
  expect(layer).toBeDefined()
})
