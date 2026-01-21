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

  // Methods that take a string key and hash it
  async getCursorByString(key: string): Promise<ReadCursor | null> {
    const hash = await this.cursor.db.hasher.digest(new TextEncoder().encode(key));
    return this.getCursor(hash);
  }

  async getSlotByString(key: string): Promise<Slot | null> {
    const hash = await this.cursor.db.hasher.digest(new TextEncoder().encode(key));
    return this.getSlot(hash);
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

  // Methods that take hash directly
  async getCursor(hash: Uint8Array): Promise<ReadCursor | null> {
    return this.cursor.readPath([new HashMapGet(new HashMapGetKey(hash))]);
  }

  async getSlot(hash: Uint8Array): Promise<Slot | null> {
    return this.cursor.readPathSlot([new HashMapGet(new HashMapGetKey(hash))]);
  }
}
