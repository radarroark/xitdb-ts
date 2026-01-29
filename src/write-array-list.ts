import { ReadArrayList } from './read-array-list';
import { WriteCursor, WriteCursorIterator } from './write-cursor';
import {
  ArrayListInit,
  ArrayListGet,
  ArrayListAppend,
  ArrayListSlice,
  WriteData,
  Context,
  type ContextFunction,
} from './database';
import type { WriteableData } from './writeable-data';

export class WriteArrayList extends ReadArrayList {
  protected constructor() {
    super();
  }

  static async create(cursor: WriteCursor): Promise<WriteArrayList> {
    const list = new WriteArrayList();
    const newCursor = await cursor.writePath([new ArrayListInit()]);
    list.cursor = newCursor;
    return list;
  }

  override async iterator(): Promise<WriteCursorIterator> {
    return (this.cursor as WriteCursor).iterator();
  }

  override async *[Symbol.asyncIterator](): AsyncIterator<WriteCursor> {
    yield* this.cursor as WriteCursor;
  }

  async put(index: number, data: WriteableData): Promise<void> {
    await (this.cursor as WriteCursor).writePath([
      new ArrayListGet(index),
      new WriteData(data),
    ]);
  }

  async putCursor(index: number): Promise<WriteCursor> {
    return (this.cursor as WriteCursor).writePath([new ArrayListGet(index)]);
  }

  async append(data: WriteableData): Promise<void> {
    await (this.cursor as WriteCursor).writePath([
      new ArrayListAppend(),
      new WriteData(data),
    ]);
  }

  async appendCursor(): Promise<WriteCursor> {
    return (this.cursor as WriteCursor).writePath([new ArrayListAppend()]);
  }

  async appendContext(data: WriteableData | null, fn: ContextFunction): Promise<void> {
    await (this.cursor as WriteCursor).writePath([
      new ArrayListAppend(),
      new WriteData(data),
      new Context(fn),
    ]);
  }

  async slice(size: number): Promise<void> {
    await (this.cursor as WriteCursor).writePath([new ArrayListSlice(size)]);
  }
}
