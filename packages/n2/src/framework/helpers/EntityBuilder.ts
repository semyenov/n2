/**
 * @since 1.0.0
 *
 * Derives an Rpc definition from a Schema.TaggedRequest command class,
 * eliminating the need to repeat payload/success/error schemas.
 */
import type * as Schema from "effect/Schema"
import { Rpc } from "@effect/rpc"
import type {
  CommandDefinition,
  CommandFields,
  CommandInfoOf,
  CommandPayloadFieldsOf,
  CommandPayloadTypeOf,
  Tagged,
} from "./Definitions.js"

type PayloadFields<Fields extends CommandFields> = Omit<Fields, "_tag">

type PayloadType<Fields extends CommandFields> = Schema.Simplify<
  Schema.Struct.Type<NoInfer<PayloadFields<Fields>>>
>

type PayloadFieldsOf<Command extends CommandDefinition<Tagged>> =
  CommandPayloadFieldsOf<Command>

type PayloadTypeOf<Command extends CommandDefinition<Tagged>> =
  CommandPayloadTypeOf<Command>

type RpcFromCommandDefinition<Command extends CommandDefinition<Tagged>> =
  CommandInfoOf<Command> extends {
    readonly tag: infer Tag extends string
    readonly fields: infer Fields extends CommandFields
    readonly success: infer Success extends Schema.Schema.Any
    readonly failure: infer Failure extends Schema.Schema.All
  }
    ? ReturnType<typeof Rpc.make<Tag, PayloadFields<Fields>, Success, Failure>>
    : never

const stripTagField = <Fields extends CommandFields>(
  fields: Fields
): PayloadFields<Fields> => {
  const { _tag: _ignored, ...payload } = fields
  return payload
}

type RpcMakePrimaryKey<Fields extends CommandFields> =
  [PayloadFields<Fields>] extends [Schema.Struct.Fields] ?
    (payload: PayloadType<Fields>) => string :
    never

type RpcPrimaryKeyOf<Command extends CommandDefinition<Tagged>> =
  (payload: PayloadTypeOf<Command>) => string

const makeRpcFromFields = <
  Tag extends string,
  Fields extends CommandFields,
  Success extends Schema.Schema.Any,
  Failure extends Schema.Schema.All
>(
  tag: Tag,
  fields: Fields,
  success: Success,
  failure: Failure,
  primaryKey: (payload: PayloadType<Fields>) => string
): ReturnType<typeof Rpc.make<Tag, PayloadFields<Fields>, Success, Failure>> =>
  Rpc.make<Tag, PayloadFields<Fields>, Success, Failure>(
    tag,
    {
      payload: stripTagField(fields),
      primaryKey: primaryKey as RpcMakePrimaryKey<Fields>,
      success,
      error: failure
    }
  )

/**
 * Derives an Rpc from a `Schema.TaggedRequest` command class.
 *
 * Extracts `_tag`, payload fields, `success`, and `failure` from the
 * command class's static properties, so you don't repeat them.
 *
 * Use inside a literal array passed to `Entity.make` to preserve tuple types.
 *
 * @example
 * ```ts
 * import { Entity, ClusterSchema } from "@effect/cluster"
 * import * as N2 from "n2/framework/helpers"
 *
 * const OrderEntity = Entity.make("Order", [
 *   N2.rpcFromCommand(CreateOrder, (p) => p.orderId),
 *   N2.rpcFromCommand(AddItem, (p) => p.orderId),
 * ]).annotateRpcs(ClusterSchema.Persisted, true)
 * ```
 *
 * @since 1.0.0
 */
export const rpcFromCommand = <
  Tag extends string,
  Fields extends CommandFields,
  Success extends Schema.Schema.Any,
  Failure extends Schema.Schema.All
>(
  command: {
    readonly _tag: Tag
    readonly fields: Fields
    readonly success: Success
    readonly failure: Failure
  },
  primaryKey: (payload: PayloadType<Fields>) => string
) =>
  makeRpcFromFields(
    command._tag,
    command.fields,
    command.success,
    command.failure,
    primaryKey
  )

export type { PayloadFieldsOf, PayloadTypeOf, RpcFromCommandDefinition, RpcPrimaryKeyOf }
