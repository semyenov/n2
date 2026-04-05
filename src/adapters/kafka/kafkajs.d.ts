/**
 * Minimal type declaration for kafkajs dynamic import.
 * Install kafkajs for full types: `bun add kafkajs`
 */
declare module "kafkajs" {
  export class Kafka {
    constructor(config: { clientId?: string; brokers: string[] })
    producer(): any
    consumer(config: { groupId: string }): any
  }
}
