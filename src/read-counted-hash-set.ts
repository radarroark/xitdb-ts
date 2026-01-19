import { Tag } from './tag';
import { ReadHashSet } from './read-hash-set';
import { ReadCursor } from './read-cursor';
import { UnexpectedTagException } from './exceptions';

export class ReadCountedHashSet extends ReadHashSet {
  protected constructor() {
    super();
  }

  static override async create(cursor: ReadCursor): Promise<ReadCountedHashSet> {
    const set = new ReadCountedHashSet();
    switch (cursor.slotPtr.slot.tag) {
      case Tag.NONE:
      case Tag.COUNTED_HASH_MAP:
      case Tag.COUNTED_HASH_SET:
        set.cursor = cursor;
        break;
      default:
        throw new UnexpectedTagException();
    }
    return set;
  }

  async count(): Promise<bigint> {
    return this.cursor.count();
  }
}
