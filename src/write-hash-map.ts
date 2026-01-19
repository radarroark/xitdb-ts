import { ReadHashMap } from './read-hash-map';
import { WriteCursor, WriteCursorIterator, WriteKeyValuePairCursor } from './write-cursor';
import {
  HashMapInit,
  HashMapGet,
  HashMapGetValue,
  HashMapGetKey,
  HashMapRemove,
  WriteData,
} from './database';
import type { WriteableData } from './writeable-data';
import { Bytes } from './writeable-data';
import { KeyNotFoundException } from './exceptions';

export class WriteHashMap extends ReadHashMap {
  protected constructor() {
    super();
  }

  static async create(cursor: WriteCursor): Promise<WriteHashMap> {
    const map = new WriteHashMap();
    const newCursor = await cursor.writePath([new HashMapInit(false, false)]);
    map.cursor = newCursor;
    return map;
  }

  override iterator(): WriteCursorIterator {
    return (this.cursor as WriteCursor).iterator();
  }

  override async *[Symbol.asyncIterator](): AsyncIterator<WriteCursor> {
    yield* this.cursor as WriteCursor;
  }

  // Methods that take a string key and hash it
  async putByString(key: string, data: WriteableData): Promise<void> {
    const hash = await this.cursor.db.hasher.digest(new TextEncoder().encode(key));
    await this.putKey(hash, new Bytes(key));
    await this.put(hash, data);
  }

  async putCursorByString(key: string): Promise<WriteCursor> {
    const hash = await this.cursor.db.hasher.digest(new TextEncoder().encode(key));
    await this.putKey(hash, new Bytes(key));
    return this.putCursor(hash);
  }

  async putKeyByString(key: string, data: WriteableData): Promise<void> {
    const hash = await this.cursor.db.hasher.digest(new TextEncoder().encode(key));
    await this.putKey(hash, data);
  }

  async putKeyCursorByString(key: string): Promise<WriteCursor> {
    const hash = await this.cursor.db.hasher.digest(new TextEncoder().encode(key));
    return this.putKeyCursor(hash);
  }

  async removeByString(key: string): Promise<boolean> {
    const hash = await this.cursor.db.hasher.digest(new TextEncoder().encode(key));
    return this.remove(hash);
  }

  // Methods that take Bytes key and hash it
  async putByBytes(key: Bytes, data: WriteableData): Promise<void> {
    const hash = await this.cursor.db.hasher.digest(key.value);
    await this.putKey(hash, key);
    await this.put(hash, data);
  }

  async putCursorByBytes(key: Bytes): Promise<WriteCursor> {
    const hash = await this.cursor.db.hasher.digest(key.value);
    await this.putKey(hash, key);
    return this.putCursor(hash);
  }

  async putKeyByBytes(key: Bytes, data: WriteableData): Promise<void> {
    const hash = await this.cursor.db.hasher.digest(key.value);
    await this.putKey(hash, data);
  }

  async putKeyCursorByBytes(key: Bytes): Promise<WriteCursor> {
    const hash = await this.cursor.db.hasher.digest(key.value);
    return this.putKeyCursor(hash);
  }

  async removeByBytes(key: Bytes): Promise<boolean> {
    const hash = await this.cursor.db.hasher.digest(key.value);
    return this.remove(hash);
  }

  // Methods that take hash directly
  async put(hash: Uint8Array, data: WriteableData): Promise<void> {
    await (this.cursor as WriteCursor).writePath([
      new HashMapGet(new HashMapGetValue(hash)),
      new WriteData(data),
    ]);
  }

  async putCursor(hash: Uint8Array): Promise<WriteCursor> {
    return (this.cursor as WriteCursor).writePath([
      new HashMapGet(new HashMapGetValue(hash)),
    ]);
  }

  async putKey(hash: Uint8Array, data: WriteableData): Promise<void> {
    const cursor = await (this.cursor as WriteCursor).writePath([
      new HashMapGet(new HashMapGetKey(hash)),
    ]);
    await cursor.writeIfEmpty(data);
  }

  async putKeyCursor(hash: Uint8Array): Promise<WriteCursor> {
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
