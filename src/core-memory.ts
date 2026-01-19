import type { Core, DataReader, DataWriter } from './core';
import { RandomAccessMemory } from './random-access-memory';

export class CoreMemory implements Core {
  public memory: RandomAccessMemory;

  constructor(memory: RandomAccessMemory) {
    this.memory = memory;
  }

  reader(): DataReader {
    return this.memory;
  }

  writer(): DataWriter {
    return this.memory;
  }

  async length(): Promise<bigint> {
    return BigInt(this.memory.size());
  }

  async seek(pos: bigint): Promise<void> {
    this.memory.seek(Number(pos));
  }

  position(): bigint {
    return BigInt(this.memory.getPosition());
  }

  async setLength(len: bigint): Promise<void> {
    this.memory.setLength(Number(len));
  }

  async flush(): Promise<void> {
    // no-op for in-memory
  }

  async sync(): Promise<void> {
    // no-op for in-memory
  }
}
