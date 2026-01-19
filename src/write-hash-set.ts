import { ReadHashSet } from './read-hash-set';
import { WriteCursor, WriteCursorIterator } from './write-cursor';
import { HashMapInit, HashMapGet, HashMapGetKey, HashMapRemove } from './database';
import type { WriteableData } from './writeable-data';
import { Bytes } from './writeable-data';
import { KeyNotFoundException } from './exceptions';

export class WriteHashSet extends ReadHashSet {
  protected constructor() {
    super();
  }

  static async create(cursor: WriteCursor): Promise<WriteHashSet> {
    const set = new WriteHashSet();
    const newCursor = await cursor.writePath([new HashMapInit(false, true)]);
    set.cursor = newCursor;
    return set;
  }

  override iterator(): WriteCursorIterator {
    return (this.cursor as WriteCursor).iterator();
  }

  override async *[Symbol.asyncIterator](): AsyncIterator<WriteCursor> {
    yield* this.cursor as WriteCursor;
  }

  // Methods that take a string key and hash it
  async putByString(key: string): Promise<void> {
    const bytes = new TextEncoder().encode(key);
    const hash = await this.cursor.db.hasher.digest(bytes);
    await this.put(hash, new Bytes(bytes));
  }

  async putCursorByString(key: string): Promise<WriteCursor> {
    const hash = await this.cursor.db.hasher.digest(new TextEncoder().encode(key));
    return this.putCursor(hash);
  }

  async removeByString(key: string): Promise<boolean> {
    const hash = await this.cursor.db.hasher.digest(new TextEncoder().encode(key));
    return this.remove(hash);
  }

  // Methods that take Bytes key and hash it
  async putByBytes(key: Bytes): Promise<void> {
    const hash = await this.cursor.db.hasher.digest(key.value);
    await this.put(hash, key);
  }

  async putCursorByBytes(key: Bytes): Promise<WriteCursor> {
    const hash = await this.cursor.db.hasher.digest(key.value);
    return this.putCursor(hash);
  }

  async removeByBytes(key: Bytes): Promise<boolean> {
    const hash = await this.cursor.db.hasher.digest(key.value);
    return this.remove(hash);
  }

  // Methods that take hash directly
  async put(hash: Uint8Array, data: WriteableData): Promise<void> {
    const cursor = await (this.cursor as WriteCursor).writePath([
      new HashMapGet(new HashMapGetKey(hash)),
    ]);
    await cursor.writeIfEmpty(data);
  }

  async putCursor(hash: Uint8Array): Promise<WriteCursor> {
    return (this.cursor as WriteCursor).writePath([
      new HashMapGet(new HashMapGetKey(hash)),
    ]);
  }

  async remove(hash: Uint8Array): Promise<boolean> {
    try {
      await (this.cursor as WriteCursor).writePath([new HashMapRemove(hash)]);
    } catch (e) {
      if (e instanceof KeyNotFoundException) {
        return false;
      }
      throw e;
    }
    return true;
  }
}
