import * as Schema from "effect/Schema"

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

type CommandCollection<Members extends [CommandDefinition<Tagged>, ...Array<CommandDefinition<Tagged>>]> = {
  readonly schema: SchemaUnion<Members>
  readonly members: Members
  readonly constructors: TaggedConstructors<Members>
}

const constructorsByTag = <Members extends ReadonlyArray<TaggedSchema>>(
  members: Members
): TaggedConstructors<Members> =>
  Object.fromEntries(members.map((member) => [member._tag, member])) as TaggedConstructors<Members>

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

export const defineCommands = <const Members extends [CommandDefinition<Tagged>, ...Array<CommandDefinition<Tagged>>]>(
  ...members: Members
): CommandCollection<Members> => ({
  schema: defineSchemaUnion(...members),
  members,
  constructors: constructorsByTag(members)
})
