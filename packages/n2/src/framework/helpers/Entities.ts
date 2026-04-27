import { ClusterSchema, Entity } from "@effect/cluster"
import type { CommandDefinition, Tagged } from "./Definitions.js"
import {
  type PayloadTypeOf,
  type RpcFromCommandDefinition,
  rpcListFromCommandDefinitions,
  type RpcTupleFromCommands,
} from "./EntityBuilder.js"

type AnyCommandDefinition = CommandDefinition<Tagged>

type CommandPayload<Command extends AnyCommandDefinition> = PayloadTypeOf<Command>

export const rpcListFromCommands = <
  Head extends AnyCommandDefinition,
  const Tail extends ReadonlyArray<AnyCommandDefinition>
>(
  primaryKey: (payload: CommandPayload<Head | Tail[number]>) => string,
  head: Head,
  ...tail: Tail
): readonly [RpcFromCommandDefinition<Head>, ...RpcTupleFromCommands<Tail>] =>
  rpcListFromCommandDefinitions(primaryKey, head, ...tail)

export const entityFromCommands = <
  const Type extends string,
  Head extends AnyCommandDefinition,
  const Tail extends ReadonlyArray<AnyCommandDefinition>
>(
  name: Type,
  primaryKey: (payload: CommandPayload<Head | Tail[number]>) => string,
  head: Head,
  ...tail: Tail
) =>
  Entity.make(name, rpcListFromCommands(primaryKey, head, ...tail))

export const persistedEntityFromCommands = <
  const Type extends string,
  Head extends AnyCommandDefinition,
  const Tail extends ReadonlyArray<AnyCommandDefinition>
>(
  name: Type,
  primaryKey: (payload: CommandPayload<Head | Tail[number]>) => string,
  head: Head,
  ...tail: Tail
) =>
  entityFromCommands(name, primaryKey, head, ...tail).annotateRpcs(ClusterSchema.Persisted, true)
