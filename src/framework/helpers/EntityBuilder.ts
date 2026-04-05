/**
 * @since 1.0.0
 *
 * Derives an Entity definition from command constructors,
 * eliminating the need to repeat payload/success/error schemas.
 */
import { Rpc } from "@effect/rpc"
import { Entity, ClusterSchema } from "@effect/cluster"

type Tagged = { readonly _tag: string }

type CommandConstructors<Command extends Tagged> = {
  readonly [K in Extract<Command["_tag"], string>]:
    new (...args: ReadonlyArray<never>) => Extract<Command, { readonly _tag: K }>
}

/**
 * Derives an Entity from command constructors.
 *
 * Each command must be a `Schema.TaggedRequest` which exposes
 * `.fields`, `.success`, and `.failure` as static properties.
 *
 * @example
 * ```ts
 * const OrderEntity = makeEntity("Order", OrderCommands, {
 *   primaryKey: (p) => p.orderId,
 *   persisted: true
 * })
 * ```
 *
 * @since 1.0.0
 */
export const makeEntity = <
  Type extends string,
  Command extends Tagged
>(
  type: Type,
  commands: CommandConstructors<Command>,
  options: {
    readonly primaryKey: (payload: any) => string
    readonly persisted?: boolean
  }
) => {
  const rpcs = Object.keys(commands).map((tag) => {
    const Ctor = commands[tag as keyof typeof commands] as any
    const { _tag, ...payloadFields } = Ctor.fields
    return Rpc.make(tag, {
      payload: payloadFields,
      primaryKey: options.primaryKey,
      success: Ctor.success,
      error: Ctor.failure,
    })
  })

  const entity = Entity.make(type, rpcs as any)
  return options.persisted
    ? entity.annotateRpcs(ClusterSchema.Persisted, true)
    : entity
}
