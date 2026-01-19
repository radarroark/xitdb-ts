import { Tag } from './tag';
import { WriteHashSet } from './write-hash-set';
import { WriteCursor } from './write-cursor';
import { HashMapInit } from './database';
import { UnexpectedTagException } from './exceptions';

export class WriteCountedHashSet extends WriteHashSet {
  protected constructor() {
    super();
  }

  static override async create(cursor: WriteCursor): Promise<WriteCountedHashSet> {
    const set = new WriteCountedHashSet();
    switch (cursor.slotPtr.slot.tag) {
      case Tag.NONE:
      case Tag.COUNTED_HASH_MAP:
      case Tag.COUNTED_HASH_SET: {
        const newCursor = await cursor.writePath([new HashMapInit(true, true)]);
        set.cursor = newCursor;
        break;
      }
      default:
        throw new UnexpectedTagException();
    }
    return set;
  }

  async count(): Promise<bigint> {
    return this.cursor.count();
  }
}
