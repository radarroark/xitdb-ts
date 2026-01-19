import { Slot } from './slot';
import { ReadLinkedArrayList } from './read-linked-array-list';
import { WriteCursor, WriteCursorIterator } from './write-cursor';
import {
  LinkedArrayListInit,
  LinkedArrayListGet,
  LinkedArrayListAppend,
  LinkedArrayListSlice,
  LinkedArrayListConcat,
  LinkedArrayListInsert,
  LinkedArrayListRemove,
  WriteData,
} from './database';
import type { WriteableData } from './writeable-data';

export class WriteLinkedArrayList extends ReadLinkedArrayList {
  constructor(cursor: WriteCursor) {
    super(cursor);
  }

  static async create(cursor: WriteCursor): Promise<WriteLinkedArrayList> {
    const newCursor = await cursor.writePath([new LinkedArrayListInit()]);
    return new WriteLinkedArrayList(newCursor);
  }

  override iterator(): WriteCursorIterator {
    return (this.cursor as WriteCursor).iterator();
  }

  override async *[Symbol.asyncIterator](): AsyncIterator<WriteCursor> {
    yield* this.cursor as WriteCursor;
  }

  async put(index: bigint, data: WriteableData): Promise<void> {
    await (this.cursor as WriteCursor).writePath([
      new LinkedArrayListGet(index),
      new WriteData(data),
    ]);
  }

  async putCursor(index: bigint): Promise<WriteCursor> {
    return (this.cursor as WriteCursor).writePath([new LinkedArrayListGet(index)]);
  }

  async append(data: WriteableData): Promise<void> {
    await (this.cursor as WriteCursor).writePath([
      new LinkedArrayListAppend(),
      new WriteData(data),
    ]);
  }

  async appendCursor(): Promise<WriteCursor> {
    return (this.cursor as WriteCursor).writePath([new LinkedArrayListAppend()]);
  }

  async slice(offset: bigint, size: bigint): Promise<void> {
    await (this.cursor as WriteCursor).writePath([
      new LinkedArrayListSlice(offset, size),
    ]);
  }

  async concat(list: Slot): Promise<void> {
    await (this.cursor as WriteCursor).writePath([new LinkedArrayListConcat(list)]);
  }

  async insert(index: bigint, data: WriteableData): Promise<void> {
    await (this.cursor as WriteCursor).writePath([
      new LinkedArrayListInsert(index),
      new WriteData(data),
    ]);
  }

  async insertCursor(index: bigint): Promise<WriteCursor> {
    return (this.cursor as WriteCursor).writePath([new LinkedArrayListInsert(index)]);
  }

  async remove(index: bigint): Promise<void> {
    await (this.cursor as WriteCursor).writePath([new LinkedArrayListRemove(index)]);
  }
}
