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

  override async iterator(): Promise<WriteCursorIterator> {
    return (this.cursor as WriteCursor).iterator();
  }

  override async *[Symbol.asyncIterator](): AsyncIterator<WriteCursor> {
    yield* this.cursor as WriteCursor;
  }

  // put overloads (for sets, put takes only the key)
  async put(key: string): Promise<void>;
  async put(key: Bytes): Promise<void>;
  async put(hash: Uint8Array, data: WriteableData): Promise<void>;
  async put(key: string | Bytes | Uint8Array, data?: WriteableData): Promise<void> {
    if (typeof key === 'string') {
      const bytes = new TextEncoder().encode(key);
      const hash = await this.cursor.db.hasher.digest(bytes);
      await this.putInternal(hash, new Bytes(bytes));
    } else if (key instanceof Bytes) {
      const hash = await this.cursor.db.hasher.digest(key.value);
      await this.putInternal(hash, key);
    } else {
      await this.putInternal(key, data!);
    }
  }

  // putCursor overloads
  async putCursor(key: string): Promise<WriteCursor>;
  async putCursor(key: Bytes): Promise<WriteCursor>;
  async putCursor(hash: Uint8Array): Promise<WriteCursor>;
  async putCursor(key: string | Bytes | Uint8Array): Promise<WriteCursor> {
    const hash = await this.resolveHash(key);
    return this.putCursorInternal(hash);
  }

  // remove overloads
  async remove(key: string): Promise<boolean>;
  async remove(key: Bytes): Promise<boolean>;
  async remove(hash: Uint8Array): Promise<boolean>;
  async remove(key: string | Bytes | Uint8Array): Promise<boolean> {
    const hash = await this.resolveHash(key);
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

  // Internal methods that take hash directly
  private async putInternal(hash: Uint8Array, data: WriteableData): Promise<void> {
    const cursor = await (this.cursor as WriteCursor).writePath([
      new HashMapGet(new HashMapGetKey(hash)),
    ]);
    await cursor.writeIfEmpty(data);
  }

  private async putCursorInternal(hash: Uint8Array): Promise<WriteCursor> {
    return (this.cursor as WriteCursor).writePath([
      new HashMapGet(new HashMapGetKey(hash)),
    ]);
  }
}
