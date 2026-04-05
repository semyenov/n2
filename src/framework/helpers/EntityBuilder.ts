/**
 * @since 1.0.0
 *
 * Derives an Rpc definition from a Schema.TaggedRequest command class,
 * eliminating the need to repeat payload/success/error schemas.
 */
import type * as Schema from "effect/Schema"
import { Rpc } from "@effect/rpc"

type RpcWithPrimaryKeyOptions<
  Payload extends Schema.Struct.Fields,
  Success extends Schema.Schema.Any,
  Failure extends Schema.Schema.All
> = {
  readonly payload: Payload
  readonly primaryKey: [Payload] extends [Schema.Struct.Fields] ?
    (payload: Schema.Simplify<Schema.Struct.Type<Payload>>) => string :
    never
  readonly success: Success
  readonly error: Failure
}

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
  Fields extends { readonly _tag: Schema.Struct.Field } & Schema.Struct.Fields,
  Success extends Schema.Schema.Any,
  Failure extends Schema.Schema.All
>(
  Cmd: {
    readonly _tag: Tag
    readonly fields: Fields
    readonly success: Success
    readonly failure: Failure
  },
  primaryKey: (payload: Schema.Simplify<Schema.Struct.Type<Omit<Fields, "_tag">>>) => string
) => {
  const { _tag: _ignored, ...payload } = Cmd.fields
  const options = {
    payload,
    primaryKey,
    success: Cmd.success,
    error: Cmd.failure,
  } as unknown as RpcWithPrimaryKeyOptions<Omit<Fields, "_tag">, Success, Failure>

  return Rpc.make<Tag, Omit<Fields, "_tag">, Success, Failure>(Cmd._tag, options)
}
