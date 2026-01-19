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
  constructor(cursor: WriteCursor) {
    super(cursor);
  }

  static async create(cursor: WriteCursor): Promise<WriteArrayList> {
    const newCursor = await cursor.writePath([new ArrayListInit()]);
    return new WriteArrayList(newCursor);
  }

  override iterator(): WriteCursorIterator {
    return (this.cursor as WriteCursor).iterator();
  }

  override async *[Symbol.asyncIterator](): AsyncIterator<WriteCursor> {
    yield* this.cursor as WriteCursor;
  }

  async put(index: bigint, data: WriteableData): Promise<void> {
    await (this.cursor as WriteCursor).writePath([
      new ArrayListGet(index),
      new WriteData(data),
    ]);
  }

  async putCursor(index: bigint): Promise<WriteCursor> {
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

  async slice(size: bigint): Promise<void> {
    await (this.cursor as WriteCursor).writePath([new ArrayListSlice(size)]);
  }
}
