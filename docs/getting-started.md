# Getting Started

## Prerequisites

- [Bun](https://bun.sh/) v1.3+
- TypeScript 5.5+ (strict mode)

## Setup

```bash
bun install
bunx tsc --noEmit    # type check
bun run test         # run all tests
```

## Your first aggregate

An aggregate is an event-sourced state machine. You need 4 files:

### 1. contracts.ts — schemas

Define events, commands, errors, and state using Effect Schema. Commands are `Schema.TaggedRequest` — they carry success/failure types and serve as both the domain command AND the RPC schema.

```ts
import * as Schema from "effect/Schema"
import * as N2 from "@semyenov/n2/helpers"

// Common event fields
const EventBase = { itemId: Schema.String, occurredAt: Schema.DateTimeUtc }

// Events
class ItemCreated extends Schema.TaggedClass<ItemCreated>()("ItemCreated", {
  ...EventBase, name: Schema.String
}) {}

// State
class ItemState extends Schema.Class<ItemState>("ItemState")({
  itemId: Schema.String, name: Schema.String, status: Schema.Literal("empty", "active")
}) {}

// Commands
class CreateItem extends Schema.TaggedRequest<CreateItem>("CreateItem")(
  "CreateItem",
  { failure: ItemError, success: ItemResult, payload: { itemId: Schema.String, name: Schema.String } }
) {}

// Derive entity from commands
const ItemCommands = N2.defineCommands(CreateItem, GetItem)
const ItemEntity = ItemCommands.toPersistedEntity("Item", (p) => p.itemId)
const ItemRpcs = ItemEntity.protocol
```

### 2. aggregate.ts — business logic

Use `N2.define()` for exhaustive compile-time checking. TypeScript ensures every event has an `evolve` handler and every command has a `decide` handler.

```ts
import * as N2 from "@semyenov/n2/helpers"

const Item = N2.define<ItemEvent, ItemCommand>()({
  initialState,
  commands: ItemCommands.constructors,
  evolve: {
    ItemCreated: (state, event) => ({ ...state, status: "active", name: event.name }),
  },
  decide: {
    CreateItem: (state, cmd) => Effect.gen(function* () {
      if (state.status !== "empty") return yield* new ItemError({ message: "exists" })
      const now = yield* DateTime.now
      return [new ItemCreated({ itemId: cmd.itemId, name: cmd.name, occurredAt: now })]
    }),
    GetItem: () => Effect.succeed([])  // read queries return no events
  }
})
```

### 3. entity.ts — infrastructure wiring

```ts
const ItemEntityLayer = Item.toEntityLayer(ItemEntity, {
  toResult: ({ entityId, state }) => new ItemResult({ itemId: entityId, name: state.name }),
  toError: (e) => e instanceof ItemError ? e : new ItemError({ message: String(e) }),
  overrides: {
    GetItem: (cmd, { getState }) => getState  // bypass dispatch for reads
  }
})
```

### 4. events.ts — EventGroup for projections

```ts
import { EventGroup } from "@effect/experimental"
import * as N2 from "@semyenov/n2/helpers"

const ItemEventGroup = EventGroup.empty
  .add({ tag: "ItemCreated", primaryKey: (p) => p.itemId, payload: N2.eventPayloadSchema(ItemCreated) })
```

### 5. Run tests

```ts
import { it, expect } from "@effect/vitest"

it.effect("create item", () => Effect.gen(function* () {
  const { state } = yield* Item.handle(initialState, new CreateItem({ itemId: "i1", name: "Widget" }))
  expect(state.status).toBe("active")
}))
```

## Next steps

- See [Framework API](framework-api.md) for complete reference
- See [Patterns](patterns.md) for projections, outbox, and workflows
- See [Examples](examples.md) for walkthroughs of order, inventory, and profile-provider
