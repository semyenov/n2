/**
 * Replay test: rebuild projection from EventLog.
 */
import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as HashMap from "effect/HashMap"
import * as DateTime from "effect/DateTime"
import * as Schema from "effect/Schema"
import { EntityId, EntityType } from "@effect/cluster"
import { EventLog } from "../runtime/EventLog.js"
import { EventEnvelope } from "../contracts/EventEnvelope.js"
import { make as makeRevision } from "../domain/Revision.js"
import { layerMemory as EventLogMemory } from "../runtime/EventJournalEventLog.js"
import * as Replay from "./Replay.js"
import * as ProjectorDefinition from "./ProjectorDefinition.js"

const TestProjector = ProjectorDefinition.define<HashMap.HashMap<string, number>>({
  name: "counter",
  initialState: HashMap.empty(),
  handle: (state, envelope) =>
    Effect.sync(() => {
      const payload = envelope.payload as { readonly _tag: string; readonly id: string }
      if (payload._tag === "Incremented") {
        const current = HashMap.get(state, payload.id)
        const count = current._tag === "Some" ? current.value + 1 : 1
        return HashMap.set(state, payload.id, count)
      }
      return state
    })
})

test("replay rebuilds projection from event log", async () => {
  await Effect.gen(function*() {
    const eventLog = yield* EventLog
    const now = DateTime.unsafeMake(0)
    const aggType = Schema.decodeSync(EntityType.EntityType)("Counter")

    const events = [1, 2, 3].map((i) =>
      new EventEnvelope({
        eventId: `evt-${i}`,
        streamId: "Counter-c1",
        aggregateId: EntityId.make("c1"),
        aggregateType: aggType,
        revision: i,
        occurredAt: now,
        payload: { _tag: "Incremented", id: "c1" }
      })
    )
    yield* eventLog.append("Counter-c1", events, makeRevision(0))

    const state = yield* Replay.replay(TestProjector)
    const count = HashMap.get(state, "c1")
    expect(count._tag).toBe("Some")
    if (count._tag === "Some") {
      expect(count.value).toBe(3)
    }
  }).pipe(
    Effect.provide(EventLogMemory),
    Effect.runPromise
  )
})
