import { Tag } from './tag';
import { Slot } from './slot';
import type { Slotted } from './slotted';
import { ReadCursor, CursorIterator } from './read-cursor';
import { ArrayListGet } from './database';
import { UnexpectedTagException } from './exceptions';

export class ReadArrayList implements Slotted {
  public cursor!: ReadCursor;

  constructor();
  constructor(cursor: ReadCursor);
  constructor(cursor?: ReadCursor) {
    if (cursor) {
      switch (cursor.slotPtr.slot.tag) {
        case Tag.NONE:
        case Tag.ARRAY_LIST:
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

  async count(): Promise<number> {
    return this.cursor.count();
  }

  async iterator(): Promise<CursorIterator> {
    return this.cursor.iterator();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<ReadCursor> {
    yield* this.cursor;
  }

  async getCursor(index: number): Promise<ReadCursor | null> {
    return this.cursor.readPath([new ArrayListGet(index)]);
  }

  async getSlot(index: number): Promise<Slot | null> {
    return this.cursor.readPathSlot([new ArrayListGet(index)]);
  }
}
