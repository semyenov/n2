import * as Record from "effect/Record"
import * as Schema from "effect/Schema"
import { ClusterSchema, Entity } from "@effect/cluster"
import type { Rpc } from "@effect/rpc"
import { rpcFromCommand, type RpcFromCommandDefinition } from "./EntityBuilder.js"

type Tagged = { readonly _tag: string }

type TaggedSchema = Schema.Schema.All & {
  readonly _tag: string
}

type CommandFields = { readonly _tag: Schema.Struct.Field } & Schema.Struct.Fields

type TaggedConstructor<A extends Tagged> = TaggedSchema & {
  readonly _tag: A["_tag"]
  new (...args: ReadonlyArray<never>): A
}

type CommandDefinition<A extends Tagged> = TaggedConstructor<A> & {
  readonly fields: CommandFields
  readonly success: Schema.Schema.Any
  readonly failure: Schema.Schema.Any
}

type CommandInfoOf<Command extends CommandDefinition<Tagged>> =
  Command extends Schema.TaggedRequestClass<
    any,
    infer Tag extends string,
    infer Fields extends CommandFields,
    infer Success extends Schema.Schema.Any,
    infer Failure extends Schema.Schema.All
  >
    ? {
      readonly tag: Tag
      readonly fields: Fields
      readonly success: Success
      readonly failure: Failure
    }
    : never

type CommandFieldsOf<Command extends CommandDefinition<Tagged>> =
  CommandInfoOf<Command>["fields"]

type CommandPayloadFieldsOf<Command extends CommandDefinition<Tagged>> =
  Omit<CommandInfoOf<Command>["fields"], "_tag">

type CommandPayloadTypeOf<Command extends CommandDefinition<Tagged>> =
  Schema.Simplify<Schema.Struct.Type<NoInfer<CommandPayloadFieldsOf<Command>>>>

type CommandTagOf<Command extends CommandDefinition<Tagged>> =
  CommandInfoOf<Command>["tag"]

type CommandSuccessSchemaOf<Command extends CommandDefinition<Tagged>> =
  CommandInfoOf<Command>["success"]

type CommandFailureSchemaOf<Command extends CommandDefinition<Tagged>> =
  CommandInfoOf<Command>["failure"]

type TaggedConstructors<Members extends ReadonlyArray<TaggedSchema>> = {
  readonly [K in Members[number]["_tag"]]: Extract<Members[number], { readonly _tag: K }>
}

type SchemaUnion<Members extends [Schema.Schema.All, ...Array<Schema.Schema.All>]> =
  Members extends [infer Member extends Schema.Schema.All]
    ? Member
    : Members extends [
      infer First extends Schema.Schema.All,
      infer Second extends Schema.Schema.All,
      ...infer Rest extends Array<Schema.Schema.All>
    ]
      ? Schema.Union<[First, Second, ...Rest]>
      : never

type TaggedCollection<Members extends [TaggedSchema, ...Array<TaggedSchema>]> = {
  readonly schema: SchemaUnion<Members>
  readonly constructors: TaggedConstructors<Members>
}

type AnyCommandDefinition = CommandDefinition<Tagged>

type CommandPayload<Command extends AnyCommandDefinition> = CommandPayloadTypeOf<Command>

type RpcTuple<Commands extends ReadonlyArray<AnyCommandDefinition>> = {
  readonly [K in keyof Commands]: RpcFromCommandDefinition<Commands[K]>
}

type PrimaryKey<Members extends [CommandDefinition<Tagged>, ...Array<CommandDefinition<Tagged>>]> =
  (payload: CommandPayload<Members[number]>) => string

type EntityRpcs<Members extends [CommandDefinition<Tagged>, ...Array<CommandDefinition<Tagged>>]> =
  RpcTuple<Members>[number]

type CommandCollection<Members extends [CommandDefinition<Tagged>, ...Array<CommandDefinition<Tagged>>]> = {
  readonly schema: SchemaUnion<Members>
  readonly members: Members
  readonly constructors: TaggedConstructors<Members>
  /**
   * Derive a cluster Entity from the command set.
   * The `primaryKey` function extracts the entity ID from any command payload.
   */
  readonly toEntity: <const Type extends string>(
    name: Type,
    primaryKey: PrimaryKey<Members>
  ) => Entity.Entity<Type, EntityRpcs<Members>>
  /**
   * Derive a cluster Entity annotated with `ClusterSchema.Persisted`.
   * The `primaryKey` function extracts the entity ID from any command payload.
   */
  readonly toPersistedEntity: <const Type extends string>(
    name: Type,
    primaryKey: PrimaryKey<Members>
  ) => Entity.Entity<Type, EntityRpcs<Members>>
}

const constructorsByTag = <Members extends ReadonlyArray<TaggedSchema>>(
  members: Members
): TaggedConstructors<Members> =>
  Record.fromIterableWith(members, (member) => [member._tag, member]) as TaggedConstructors<Members>

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
  if (nextHead === undefined) return [rpc]
  return [rpc, ...buildRpcTuple(primaryKey, nextHead, ...nextTail)]
}

export type {
  CommandCollection,
  CommandDefinition,
  CommandFailureSchemaOf,
  CommandFields,
  CommandFieldsOf,
  CommandInfoOf,
  CommandPayloadFieldsOf,
  CommandPayloadTypeOf,
  CommandSuccessSchemaOf,
  CommandTagOf,
  SchemaUnion,
  Tagged,
  TaggedCollection,
  TaggedConstructor,
  TaggedConstructors,
  TaggedSchema
}

export const defineSchemaUnion = <const Members extends [Schema.Schema.All, ...Array<Schema.Schema.All>]>(
  ...members: Members
): SchemaUnion<Members> =>
  Schema.Union(...members) as SchemaUnion<Members>

export const defineTaggedConstructors = <const Members extends [TaggedSchema, ...Array<TaggedSchema>]>(
  ...members: Members
): TaggedConstructors<Members> =>
  constructorsByTag(members)

/**
 * Extract payload fields from a TaggedClass (strips `_tag`), returning a Schema.Struct
 * suitable for use as an EventGroup payload. This bridges domain events (TaggedClass)
 * to @effect/experimental EventLog payloads, keeping contracts.ts as the single source of truth.
 *
 * @example
 * ```ts
 * import { EventGroup } from "@effect/experimental"
 *
 * const MyEventGroup = EventGroup.empty
 *   .add({ tag: "OrderCreated", primaryKey: (p) => p.orderId, payload: eventPayloadSchema(OrderCreated) })
 * ```
 */
export const eventPayloadSchema = <F extends Schema.Struct.Fields & { _tag: any }>(cls: { fields: F }) => {
  const { _tag: _, ...rest } = cls.fields
  return Schema.Struct(rest as { [K in Exclude<keyof F, "_tag">]: F[K] })
}

export const defineEvents = <const Members extends [TaggedSchema, ...Array<TaggedSchema>]>(
  ...members: Members
): TaggedCollection<Members> => ({
  schema: defineSchemaUnion(...members),
  constructors: constructorsByTag(members)
})

export const defineErrors = <const Members extends [TaggedSchema, ...Array<TaggedSchema>]>(
  ...members: Members
): TaggedCollection<Members> => ({
  schema: defineSchemaUnion(...members),
  constructors: constructorsByTag(members)
})

/**
 * Define a set of commands, yielding a collection with `.schema`,
 * `.constructors`, `.toEntity(name, pk)` and `.toPersistedEntity(name, pk)`.
 *
 * The `primaryKey` function is supplied at entity derivation time rather than
 * here, avoiding circular inference issues and keeping entity naming separate
 * from domain command definitions.
 *
 * @example
 * ```ts
 * const OrderCommands = defineCommands(CreateOrder, AddItem, SubmitOrder)
 * const OrderEntity = OrderCommands.toPersistedEntity("Order", (p) => p.orderId)
 * ```
 */
export const defineCommands = <const Members extends [CommandDefinition<Tagged>, ...Array<CommandDefinition<Tagged>>]>(
  ...members: Members
): CommandCollection<Members> => {
  const buildEntity = (
    name: string,
    primaryKey: (payload: any) => string,
    persisted: boolean
  ) => {
    const [head, ...tail] = members
    const rpcs = buildRpcTuple(primaryKey, head!, ...tail) as readonly [Rpc.Any, ...ReadonlyArray<Rpc.Any>]
    const entity = Entity.make(name, rpcs)
    return persisted
      ? entity.annotateRpcs(ClusterSchema.Persisted, true)
      : entity
  }

  return {
    schema: defineSchemaUnion(...members),
    members,
    constructors: constructorsByTag(members),
    toEntity: <const Type extends string>(name: Type, primaryKey: PrimaryKey<Members>) =>
      buildEntity(name, primaryKey as (payload: any) => string, false) as unknown as Entity.Entity<Type, EntityRpcs<Members>>,
    toPersistedEntity: <const Type extends string>(name: Type, primaryKey: PrimaryKey<Members>) =>
      buildEntity(name, primaryKey as (payload: any) => string, true) as unknown as Entity.Entity<Type, EntityRpcs<Members>>
  }
}
