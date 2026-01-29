import { Tag } from './tag';
import { Slot } from './slot';
import type { Slotted } from './slotted';
import { ReadCursor, CursorIterator, KeyValuePairCursor } from './read-cursor';
import { HashMapGet, HashMapGetValue, HashMapGetKey, HashMapGetKVPair } from './database';
import { UnexpectedTagException } from './exceptions';
import { Bytes } from './writeable-data';

export class ReadHashMap implements Slotted {
  public cursor!: ReadCursor;

  constructor();
  constructor(cursor: ReadCursor);
  constructor(cursor?: ReadCursor) {
    if (cursor) {
      switch (cursor.slotPtr.slot.tag) {
        case Tag.NONE:
        case Tag.HASH_MAP:
        case Tag.HASH_SET:
          this.cursor = cursor;
          break;
        default:
          throw new UnexpectedTagException();
      }
    }
  }

  slot(): Slot {
    return this.cursor.slot();
  }

  async iterator(): Promise<CursorIterator> {
    return this.cursor.iterator();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<ReadCursor> {
    yield* this.cursor;
  }

  // getCursor overloads
  async getCursor(key: string): Promise<ReadCursor | null>;
  async getCursor(key: Bytes): Promise<ReadCursor | null>;
  async getCursor(hash: Uint8Array): Promise<ReadCursor | null>;
  async getCursor(key: string | Bytes | Uint8Array): Promise<ReadCursor | null> {
    const hash = await this.resolveHash(key);
    return this.cursor.readPath([new HashMapGet(new HashMapGetValue(hash))]);
  }

  // getSlot overloads
  async getSlot(key: string): Promise<Slot | null>;
  async getSlot(key: Bytes): Promise<Slot | null>;
  async getSlot(hash: Uint8Array): Promise<Slot | null>;
  async getSlot(key: string | Bytes | Uint8Array): Promise<Slot | null> {
    const hash = await this.resolveHash(key);
    return this.cursor.readPathSlot([new HashMapGet(new HashMapGetValue(hash))]);
  }

  // getKeyCursor overloads
  async getKeyCursor(key: string): Promise<ReadCursor | null>;
  async getKeyCursor(key: Bytes): Promise<ReadCursor | null>;
  async getKeyCursor(hash: Uint8Array): Promise<ReadCursor | null>;
  async getKeyCursor(key: string | Bytes | Uint8Array): Promise<ReadCursor | null> {
    const hash = await this.resolveHash(key);
    return this.cursor.readPath([new HashMapGet(new HashMapGetKey(hash))]);
  }

  // getKeySlot overloads
  async getKeySlot(key: string): Promise<Slot | null>;
  async getKeySlot(key: Bytes): Promise<Slot | null>;
  async getKeySlot(hash: Uint8Array): Promise<Slot | null>;
  async getKeySlot(key: string | Bytes | Uint8Array): Promise<Slot | null> {
    const hash = await this.resolveHash(key);
    return this.cursor.readPathSlot([new HashMapGet(new HashMapGetKey(hash))]);
  }

  // getKeyValuePair overloads
  async getKeyValuePair(key: string): Promise<KeyValuePairCursor | null>;
  async getKeyValuePair(key: Bytes): Promise<KeyValuePairCursor | null>;
  async getKeyValuePair(hash: Uint8Array): Promise<KeyValuePairCursor | null>;
  async getKeyValuePair(key: string | Bytes | Uint8Array): Promise<KeyValuePairCursor | null> {
    const hash = await this.resolveHash(key);
    const cursor = await this.cursor.readPath([new HashMapGet(new HashMapGetKVPair(hash))]);
    if (cursor === null) {
      return null;
    } else {
      return cursor.readKeyValuePair();
    }
  }

  // Helper to resolve key to hash
  protected async resolveHash(key: string | Bytes | Uint8Array): Promise<Uint8Array> {
    if (key instanceof Uint8Array) {
      return key;
    } else if (typeof key === 'string') {
      return this.cursor.db.hasher.digest(new TextEncoder().encode(key));
    } else {
      return this.cursor.db.hasher.digest(key.value);
    }
  }
}
