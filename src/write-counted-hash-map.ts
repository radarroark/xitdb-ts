import { Tag } from './tag';
import { WriteHashMap } from './write-hash-map';
import { WriteCursor } from './write-cursor';
import { HashMapInit } from './database';
import { UnexpectedTagException } from './exceptions';

export class WriteCountedHashMap extends WriteHashMap {
  protected constructor() {
    super();
  }

  static override async create(cursor: WriteCursor): Promise<WriteCountedHashMap> {
    const map = new WriteCountedHashMap();
    switch (cursor.slotPtr.slot.tag) {
      case Tag.NONE:
      case Tag.COUNTED_HASH_MAP:
      case Tag.COUNTED_HASH_SET: {
        const newCursor = await cursor.writePath([new HashMapInit(true, false)]);
        map.cursor = newCursor;
        break;
      }
      default:
        throw new UnexpectedTagException();
    }
    return map;
  }

  async count(): Promise<number> {
    return this.cursor.count();
  }
}
