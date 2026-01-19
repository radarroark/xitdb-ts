import { Tag } from './tag';
import { Slot } from './slot';
import type { Slotted } from './slotted';
import { ReadCursor, CursorIterator } from './read-cursor';
import { LinkedArrayListGet } from './database';
import { UnexpectedTagException } from './exceptions';

export class ReadLinkedArrayList implements Slotted {
  public cursor: ReadCursor;

  constructor(cursor: ReadCursor) {
    switch (cursor.slotPtr.slot.tag) {
      case Tag.NONE:
      case Tag.LINKED_ARRAY_LIST:
        this.cursor = cursor;
        break;
      default:
        throw new UnexpectedTagException();
    }
  }

  slot(): Slot {
    return this.cursor.slot();
  }

  async count(): Promise<bigint> {
    return this.cursor.count();
  }

  iterator(): CursorIterator {
    return this.cursor.iterator();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<ReadCursor> {
    yield* this.cursor;
  }

  async getCursor(index: bigint): Promise<ReadCursor | null> {
    return this.cursor.readPath([new LinkedArrayListGet(index)]);
  }

  async getSlot(index: bigint): Promise<Slot | null> {
    return this.cursor.readPathSlot([new LinkedArrayListGet(index)]);
  }
}
