import { Tag } from './tag';
import { ReadHashMap } from './read-hash-map';
import { ReadCursor } from './read-cursor';
import { UnexpectedTagException } from './exceptions';

export class ReadCountedHashMap extends ReadHashMap {
  protected constructor() {
    super();
  }

  static override async create(cursor: ReadCursor): Promise<ReadCountedHashMap> {
    const map = new ReadCountedHashMap();
    switch (cursor.slotPtr.slot.tag) {
      case Tag.NONE:
      case Tag.COUNTED_HASH_MAP:
      case Tag.COUNTED_HASH_SET:
        map.cursor = cursor;
        break;
      default:
        throw new UnexpectedTagException();
    }
    return map;
  }

  async count(): Promise<number> {
    return this.cursor.count();
  }
}
