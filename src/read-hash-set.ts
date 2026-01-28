import { Tag } from './tag';
import { Slot } from './slot';
import type { Slotted } from './slotted';
import { ReadCursor, CursorIterator } from './read-cursor';
import { HashMapGet, HashMapGetKey } from './database';
import { UnexpectedTagException } from './exceptions';
import { Bytes } from './writeable-data';

export class ReadHashSet implements Slotted {
  public cursor!: ReadCursor;

  protected constructor() {}

  static async create(cursor: ReadCursor): Promise<ReadHashSet> {
    const set = new ReadHashSet();
    switch (cursor.slotPtr.slot.tag) {
      case Tag.NONE:
      case Tag.HASH_MAP:
      case Tag.HASH_SET:
        set.cursor = cursor;
        break;
      default:
        throw new UnexpectedTagException();
    }
    return set;
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
    return this.cursor.readPath([new HashMapGet(new HashMapGetKey(hash))]);
  }

  // getSlot overloads
  async getSlot(key: string): Promise<Slot | null>;
  async getSlot(key: Bytes): Promise<Slot | null>;
  async getSlot(hash: Uint8Array): Promise<Slot | null>;
  async getSlot(key: string | Bytes | Uint8Array): Promise<Slot | null> {
    const hash = await this.resolveHash(key);
    return this.cursor.readPathSlot([new HashMapGet(new HashMapGetKey(hash))]);
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
