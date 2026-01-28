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

  override async iterator(): Promise<WriteCursorIterator> {
    return (this.cursor as WriteCursor).iterator();
  }

  override async *[Symbol.asyncIterator](): AsyncIterator<WriteCursor> {
    yield* this.cursor as WriteCursor;
  }

  // put overloads
  async put(key: string, data: WriteableData): Promise<void>;
  async put(key: Bytes, data: WriteableData): Promise<void>;
  async put(hash: Uint8Array, data: WriteableData): Promise<void>;
  async put(key: string | Bytes | Uint8Array, data: WriteableData): Promise<void> {
    if (typeof key === 'string') {
      const hash = await this.cursor.db.hasher.digest(new TextEncoder().encode(key));
      await this.putKeyInternal(hash, new Bytes(key));
      await this.putInternal(hash, data);
    } else if (key instanceof Bytes) {
      const hash = await this.cursor.db.hasher.digest(key.value);
      await this.putKeyInternal(hash, key);
      await this.putInternal(hash, data);
    } else {
      await this.putInternal(key, data);
    }
  }

  // putCursor overloads
  async putCursor(key: string): Promise<WriteCursor>;
  async putCursor(key: Bytes): Promise<WriteCursor>;
  async putCursor(hash: Uint8Array): Promise<WriteCursor>;
  async putCursor(key: string | Bytes | Uint8Array): Promise<WriteCursor> {
    if (typeof key === 'string') {
      const hash = await this.cursor.db.hasher.digest(new TextEncoder().encode(key));
      await this.putKeyInternal(hash, new Bytes(key));
      return this.putCursorInternal(hash);
    } else if (key instanceof Bytes) {
      const hash = await this.cursor.db.hasher.digest(key.value);
      await this.putKeyInternal(hash, key);
      return this.putCursorInternal(hash);
    } else {
      return this.putCursorInternal(key);
    }
  }

  // putKey overloads
  async putKey(key: string, data: WriteableData): Promise<void>;
  async putKey(key: Bytes, data: WriteableData): Promise<void>;
  async putKey(hash: Uint8Array, data: WriteableData): Promise<void>;
  async putKey(key: string | Bytes | Uint8Array, data: WriteableData): Promise<void> {
    const hash = await this.resolveHash(key);
    await this.putKeyInternal(hash, data);
  }

  // putKeyCursor overloads
  async putKeyCursor(key: string): Promise<WriteCursor>;
  async putKeyCursor(key: Bytes): Promise<WriteCursor>;
  async putKeyCursor(hash: Uint8Array): Promise<WriteCursor>;
  async putKeyCursor(key: string | Bytes | Uint8Array): Promise<WriteCursor> {
    const hash = await this.resolveHash(key);
    return this.putKeyCursorInternal(hash);
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
    await (this.cursor as WriteCursor).writePath([
      new HashMapGet(new HashMapGetValue(hash)),
      new WriteData(data),
    ]);
  }

  private async putCursorInternal(hash: Uint8Array): Promise<WriteCursor> {
    return (this.cursor as WriteCursor).writePath([
      new HashMapGet(new HashMapGetValue(hash)),
    ]);
  }

  private async putKeyInternal(hash: Uint8Array, data: WriteableData): Promise<void> {
    const cursor = await (this.cursor as WriteCursor).writePath([
      new HashMapGet(new HashMapGetKey(hash)),
    ]);
    await cursor.writeIfEmpty(data);
  }

  private async putKeyCursorInternal(hash: Uint8Array): Promise<WriteCursor> {
    return (this.cursor as WriteCursor).writePath([
      new HashMapGet(new HashMapGetKey(hash)),
    ]);
  }
}
