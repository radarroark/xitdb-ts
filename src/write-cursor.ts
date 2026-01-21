import { Tag } from './tag';
import { Slot } from './slot';
import { SlotPointer } from './slot-pointer';
import {
  Database,
  WriteMode,
  WriteData,
  type PathPart,
} from './database';
import {
  ReadCursor,
  KeyValuePairCursor,
  CursorIterator,
} from './read-cursor';
import {
  CursorNotWriteableException,
  EndOfStreamException,
  UnexpectedWriterPositionException,
} from './exceptions';
import type { WriteableData } from './writeable-data';

export class WriteKeyValuePairCursor extends KeyValuePairCursor {
  override valueCursor: WriteCursor;
  override keyCursor: WriteCursor;

  constructor(valueCursor: WriteCursor, keyCursor: WriteCursor, hash: Uint8Array) {
    super(valueCursor, keyCursor, hash);
    this.valueCursor = valueCursor;
    this.keyCursor = keyCursor;
  }
}

export class WriteCursor extends ReadCursor {
  constructor(slotPtr: SlotPointer, db: Database) {
    super(slotPtr, db);
  }

  async writePath(path: PathPart[]): Promise<WriteCursor> {
    const slotPtr = await this.db.readSlotPointer(WriteMode.READ_WRITE, path, 0, this.slotPtr);
    if (this.db.txStart === null) {
      await this.db.core.sync();
    }
    return new WriteCursor(slotPtr, this.db);
  }

  async write(data: WriteableData | null): Promise<void> {
    const cursor = await this.writePath([new WriteData(data)]);
    this.slotPtr = cursor.slotPtr;
  }

  async writeIfEmpty(data: WriteableData): Promise<void> {
    if (this.slotPtr.slot.empty()) {
      await this.write(data);
    }
  }

  override async readKeyValuePair(): Promise<WriteKeyValuePairCursor> {
    const kvPairCursor = await super.readKeyValuePair();
    return new WriteKeyValuePairCursor(
      new WriteCursor(kvPairCursor.valueCursor.slotPtr, this.db),
      new WriteCursor(kvPairCursor.keyCursor.slotPtr, this.db),
      kvPairCursor.hash
    );
  }

  async writer(): Promise<Writer> {
    const writer = this.db.core.writer();
    const ptrPos = await this.db.core.length();
    await this.db.core.seek(ptrPos);
    await writer.writeLong(0);
    const startPosition = await this.db.core.length();
    return new Writer(this, 0, new Slot(ptrPos, Tag.BYTES), startPosition, 0);
  }

  override async *[Symbol.asyncIterator](): AsyncIterator<WriteCursor> {
    const iterator = await this.iterator();
    while (await iterator.hasNext()) {
      const next = await iterator.next();
      if (next !== null) {
        yield next;
      }
    }
  }

  override async iterator(): Promise<WriteCursorIterator> {
    const iterator = new WriteCursorIterator(this);
    await iterator.init();
    return iterator;
  }
}

export class Writer {
  parent: WriteCursor;
  size: number;
  slot: Slot;
  startPosition: number;
  relativePosition: number;
  formatTag: Uint8Array | null = null;

  constructor(
    parent: WriteCursor,
    size: number,
    slot: Slot,
    startPosition: number,
    relativePosition: number
  ) {
    this.parent = parent;
    this.size = size;
    this.slot = slot;
    this.startPosition = startPosition;
    this.relativePosition = relativePosition;
  }

  async write(buffer: Uint8Array): Promise<void> {
    if (this.size < this.relativePosition) throw new EndOfStreamException();
    await this.parent.db.core.seek(this.startPosition + this.relativePosition);
    const writer = this.parent.db.core.writer();
    await writer.write(buffer);
    this.relativePosition += buffer.length;
    if (this.relativePosition > this.size) {
      this.size = this.relativePosition;
    }
  }

  async finish(): Promise<void> {
    const writer = this.parent.db.core.writer();

    if (this.formatTag !== null) {
      this.slot = this.slot.withFull(true);
      const formatTagPos = await this.parent.db.core.length();
      await this.parent.db.core.seek(formatTagPos);
      if (this.startPosition + this.size !== formatTagPos) throw new UnexpectedWriterPositionException();
      await writer.write(this.formatTag);
    }

    await this.parent.db.core.seek(Number(this.slot.value));
    await writer.writeLong(this.size);

    if (this.parent.slotPtr.position === null) throw new CursorNotWriteableException();
    const position = this.parent.slotPtr.position;
    await this.parent.db.core.seek(position);
    await writer.write(this.slot.toBytes());

    this.parent.slotPtr = this.parent.slotPtr.withSlot(this.slot);
  }

  seek(position: number): void {
    if (position <= this.size) {
      this.relativePosition = position;
    }
  }
}

export class WriteCursorIterator extends CursorIterator {
  constructor(cursor: WriteCursor) {
    super(cursor);
  }

  override async next(): Promise<WriteCursor | null> {
    const readCursor = await super.next();
    if (readCursor !== null) {
      return new WriteCursor(readCursor.slotPtr, readCursor.db);
    } else {
      return null;
    }
  }
}
