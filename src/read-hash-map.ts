import { Tag } from './tag';
import { Slot } from './slot';
import type { Slotted } from './slotted';
import { ReadCursor, CursorIterator, KeyValuePairCursor } from './read-cursor';
import { HashMapGet, HashMapGetValue, HashMapGetKey, HashMapGetKVPair } from './database';
import { UnexpectedTagException } from './exceptions';
import { Bytes } from './writeable-data';

export class ReadHashMap implements Slotted {
  public cursor!: ReadCursor;

  protected constructor() {}

  static async create(cursor: ReadCursor): Promise<ReadHashMap> {
    const map = new ReadHashMap();
    switch (cursor.slotPtr.slot.tag) {
      case Tag.NONE:
      case Tag.HASH_MAP:
      case Tag.HASH_SET:
        map.cursor = cursor;
        break;
      default:
        throw new UnexpectedTagException();
    }
    return map;
  }

  slot(): Slot {
    return this.cursor.slot();
  }

  iterator(): CursorIterator {
    return this.cursor.iterator();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<ReadCursor> {
    yield* this.cursor;
  }

  // Methods that take a string key and hash it
  async getCursorByString(key: string): Promise<ReadCursor | null> {
    const hash = await this.cursor.db.hasher.digest(new TextEncoder().encode(key));
    return this.getCursor(hash);
  }

  async getSlotByString(key: string): Promise<Slot | null> {
    const hash = await this.cursor.db.hasher.digest(new TextEncoder().encode(key));
    return this.getSlot(hash);
  }

  async getKeyCursorByString(key: string): Promise<ReadCursor | null> {
    const hash = await this.cursor.db.hasher.digest(new TextEncoder().encode(key));
    return this.getKeyCursor(hash);
  }

  async getKeySlotByString(key: string): Promise<Slot | null> {
    const hash = await this.cursor.db.hasher.digest(new TextEncoder().encode(key));
    return this.getKeySlot(hash);
  }

  async getKeyValuePairByString(key: string): Promise<KeyValuePairCursor | null> {
    const hash = await this.cursor.db.hasher.digest(new TextEncoder().encode(key));
    return this.getKeyValuePair(hash);
  }

  // Methods that take Bytes key and hash it
  async getCursorByBytes(key: Bytes): Promise<ReadCursor | null> {
    const hash = await this.cursor.db.hasher.digest(key.value);
    return this.getCursor(hash);
  }

  async getSlotByBytes(key: Bytes): Promise<Slot | null> {
    const hash = await this.cursor.db.hasher.digest(key.value);
    return this.getSlot(hash);
  }

  async getKeyCursorByBytes(key: Bytes): Promise<ReadCursor | null> {
    const hash = await this.cursor.db.hasher.digest(key.value);
    return this.getKeyCursor(hash);
  }

  async getKeySlotByBytes(key: Bytes): Promise<Slot | null> {
    const hash = await this.cursor.db.hasher.digest(key.value);
    return this.getKeySlot(hash);
  }

  async getKeyValuePairByBytes(key: Bytes): Promise<KeyValuePairCursor | null> {
    const hash = await this.cursor.db.hasher.digest(key.value);
    return this.getKeyValuePair(hash);
  }

  // Methods that take hash directly
  async getCursor(hash: Uint8Array): Promise<ReadCursor | null> {
    return this.cursor.readPath([new HashMapGet(new HashMapGetValue(hash))]);
  }

  async getSlot(hash: Uint8Array): Promise<Slot | null> {
    return this.cursor.readPathSlot([new HashMapGet(new HashMapGetValue(hash))]);
  }

  async getKeyCursor(hash: Uint8Array): Promise<ReadCursor | null> {
    return this.cursor.readPath([new HashMapGet(new HashMapGetKey(hash))]);
  }

  async getKeySlot(hash: Uint8Array): Promise<Slot | null> {
    return this.cursor.readPathSlot([new HashMapGet(new HashMapGetKey(hash))]);
  }

  async getKeyValuePair(hash: Uint8Array): Promise<KeyValuePairCursor | null> {
    const cursor = await this.cursor.readPath([new HashMapGet(new HashMapGetKVPair(hash))]);
    if (cursor === null) {
      return null;
    } else {
      return cursor.readKeyValuePair();
    }
  }
}
