import { ClusterSchema, Entity } from "@effect/cluster"
import { type Rpc } from "@effect/rpc"
import type { CommandDefinition, Tagged } from "./Definitions.js"
import {
  rpcFromCommand,
  type PayloadTypeOf,
  type RpcFromCommandDefinition,
} from "./EntityBuilder.js"

type AnyCommandDefinition = CommandDefinition<Tagged>

type CommandPayload<Command extends AnyCommandDefinition> = PayloadTypeOf<Command>

type RpcTuple<Commands extends ReadonlyArray<AnyCommandDefinition>> = {
  readonly [K in keyof Commands]: RpcFromCommandDefinition<Commands[K]>
}

function buildRpcTuple<
  Head extends AnyCommandDefinition,
  const Tail extends ReadonlyArray<AnyCommandDefinition>
>(
  primaryKey: (payload: CommandPayload<Head | Tail[number]>) => string,
  head: Head,
  ...tail: Tail
): readonly [RpcFromCommandDefinition<Head>, ...RpcTuple<Tail>]
function buildRpcTuple(
  primaryKey: (payload: { readonly [x: string]: any }) => string,
  head: AnyCommandDefinition,
  ...tail: ReadonlyArray<AnyCommandDefinition>
): ReadonlyArray<Rpc.Any> {
  const rpc = rpcFromCommand(head, primaryKey)
  const [nextHead, ...nextTail] = tail

  if (nextHead === undefined) {
    return [rpc]
  }

  return [rpc, ...buildRpcTuple(primaryKey, nextHead, ...nextTail)]
}

export const rpcListFromCommands = <
  Head extends AnyCommandDefinition,
  const Tail extends ReadonlyArray<AnyCommandDefinition>
>(
  primaryKey: (payload: CommandPayload<Head | Tail[number]>) => string,
  head: Head,
  ...tail: Tail
): readonly [RpcFromCommandDefinition<Head>, ...RpcTuple<Tail>] =>
  buildRpcTuple(primaryKey, head, ...tail)

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
