import { Tag } from './tag';
import { Slot } from './slot';
import { SlotPointer } from './slot-pointer';
import type { Slotted } from './slotted';
import {
  Database,
  WriteMode,
  ArrayListHeader,
  LinkedArrayListHeader,
  KeyValuePair,
  type PathPart,
  ArrayListGet,
  INDEX_BLOCK_SIZE,
  LINKED_ARRAY_LIST_INDEX_BLOCK_SIZE,
  SLOT_COUNT,
  LinkedArrayListSlot,
} from './database';
import {
  UnexpectedTagException,
  StreamTooLongException,
  EndOfStreamException,
  InvalidOffsetException,
  KeyNotFoundException,
  ExpectedUnsignedLongException,
} from './exceptions';
import { Bytes } from './writeable-data';

export class KeyValuePairCursor {
  constructor(
    public valueCursor: ReadCursor,
    public keyCursor: ReadCursor,
    public hash: Uint8Array
  ) {}
}

export class ReadCursor implements Slotted {
  public slotPtr: SlotPointer;
  public db: Database;

  constructor(slotPtr: SlotPointer, db: Database) {
    this.slotPtr = slotPtr;
    this.db = db;
  }

  slot(): Slot {
    return this.slotPtr.slot;
  }

  async readPath(path: PathPart[]): Promise<ReadCursor | null> {
    try {
      const slotPtr = await this.db.readSlotPointer(WriteMode.READ_ONLY, path, 0, this.slotPtr);
      return new ReadCursor(slotPtr, this.db);
    } catch (e) {
      if (e instanceof KeyNotFoundException) {
        return null;
      }
      throw e;
    }
  }

  async readPathSlot(path: PathPart[]): Promise<Slot | null> {
    try {
      const slotPtr = await this.db.readSlotPointer(WriteMode.READ_ONLY, path, 0, this.slotPtr);
      if (!slotPtr.slot.empty()) {
        return slotPtr.slot;
      } else {
        return null;
      }
    } catch (e) {
      if (e instanceof KeyNotFoundException) {
        return null;
      }
      throw e;
    }
  }

  readUint(): bigint {
    if (this.slotPtr.slot.tag !== Tag.UINT) {
      throw new UnexpectedTagException();
    }
    if (this.slotPtr.slot.value < 0n) throw new ExpectedUnsignedLongException();
    return this.slotPtr.slot.value;
  }

  readInt(): bigint {
    if (this.slotPtr.slot.tag !== Tag.INT) {
      throw new UnexpectedTagException();
    }
    return this.slotPtr.slot.value;
  }

  readFloat(): number {
    if (this.slotPtr.slot.tag !== Tag.FLOAT) {
      throw new UnexpectedTagException();
    }
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    view.setBigInt64(0, this.slotPtr.slot.value, false);
    return view.getFloat64(0, false);
  }

  async readBytes(maxSizeMaybe: bigint | null = null): Promise<Uint8Array> {
    const bytesObj = await this.readBytesObject(maxSizeMaybe);
    return bytesObj.value;
  }

  async readBytesObject(maxSizeMaybe: bigint | null = null): Promise<Bytes> {
    const reader = this.db.core.reader();

    switch (this.slotPtr.slot.tag) {
      case Tag.NONE:
        return new Bytes(new Uint8Array(0));
      case Tag.BYTES: {
        await this.db.core.seek(this.slotPtr.slot.value);
        const valueSize = await reader.readLong();

        if (maxSizeMaybe !== null && valueSize > maxSizeMaybe) {
          throw new StreamTooLongException();
        }

        const startPosition = await this.db.core.position();

        const value = new Uint8Array(Number(valueSize));
        await reader.readFully(value);

        let formatTag: Uint8Array | null = null;
        if (this.slotPtr.slot.full) {
          await this.db.core.seek(startPosition + valueSize);
          formatTag = new Uint8Array(2);
          await reader.readFully(formatTag);
        }

        return new Bytes(value, formatTag);
      }
      case Tag.SHORT_BYTES: {
        const buffer = new ArrayBuffer(8);
        const view = new DataView(buffer);
        view.setBigInt64(0, this.slotPtr.slot.value, false);
        const bytes = new Uint8Array(buffer);

        const totalSize = this.slotPtr.slot.full ? bytes.length - 2 : bytes.length;

        let valueSize = 0;
        for (const b of bytes) {
          if (b === 0 || valueSize === totalSize) break;
          valueSize += 1;
        }

        if (maxSizeMaybe !== null && BigInt(valueSize) > maxSizeMaybe) {
          throw new StreamTooLongException();
        }

        let formatTag: Uint8Array | null = null;
        if (this.slotPtr.slot.full) {
          formatTag = bytes.slice(totalSize, bytes.length);
        }

        return new Bytes(bytes.slice(0, valueSize), formatTag);
      }
      default:
        throw new UnexpectedTagException();
    }
  }

  async readKeyValuePair(): Promise<KeyValuePairCursor> {
    const reader = this.db.core.reader();

    if (this.slotPtr.slot.tag !== Tag.KV_PAIR) {
      throw new UnexpectedTagException();
    }

    await this.db.core.seek(this.slotPtr.slot.value);
    const kvPairBytes = new Uint8Array(KeyValuePair.length(this.db.header.hashSize));
    await reader.readFully(kvPairBytes);
    const kvPair = KeyValuePair.fromBytes(kvPairBytes, this.db.header.hashSize);

    const hashPos = this.slotPtr.slot.value;
    const keySlotPos = hashPos + BigInt(this.db.header.hashSize);
    const valueSlotPos = keySlotPos + BigInt(Slot.LENGTH);

    return new KeyValuePairCursor(
      new ReadCursor(new SlotPointer(valueSlotPos, kvPair.valueSlot), this.db),
      new ReadCursor(new SlotPointer(keySlotPos, kvPair.keySlot), this.db),
      kvPair.hash
    );
  }

  async reader(): Promise<Reader> {
    const reader = this.db.core.reader();

    switch (this.slotPtr.slot.tag) {
      case Tag.BYTES: {
        await this.db.core.seek(this.slotPtr.slot.value);
        const size = await reader.readLong();
        const startPosition = await this.db.core.position();
        return new Reader(this, size, startPosition, 0n);
      }
      case Tag.SHORT_BYTES: {
        const buffer = new ArrayBuffer(8);
        const view = new DataView(buffer);
        view.setBigInt64(0, this.slotPtr.slot.value, false);
        const bytes = new Uint8Array(buffer);

        const totalSize = this.slotPtr.slot.full ? bytes.length - 2 : bytes.length;

        let valueSize = 0;
        for (const b of bytes) {
          if (b === 0 || valueSize === totalSize) break;
          valueSize += 1;
        }

        const startPosition = this.slotPtr.position! + 1n;
        return new Reader(this, BigInt(valueSize), startPosition, 0n);
      }
      default:
        throw new UnexpectedTagException();
    }
  }

  async count(): Promise<bigint> {
    const reader = this.db.core.reader();
    switch (this.slotPtr.slot.tag) {
      case Tag.NONE:
        return 0n;
      case Tag.ARRAY_LIST: {
        await this.db.core.seek(this.slotPtr.slot.value);
        const headerBytes = new Uint8Array(ArrayListHeader.LENGTH);
        await reader.readFully(headerBytes);
        const header = ArrayListHeader.fromBytes(headerBytes);
        return header.size;
      }
      case Tag.LINKED_ARRAY_LIST: {
        await this.db.core.seek(this.slotPtr.slot.value);
        const headerBytes = new Uint8Array(LinkedArrayListHeader.LENGTH);
        await reader.readFully(headerBytes);
        const header = LinkedArrayListHeader.fromBytes(headerBytes);
        return header.size;
      }
      case Tag.BYTES: {
        await this.db.core.seek(this.slotPtr.slot.value);
        return reader.readLong();
      }
      case Tag.SHORT_BYTES: {
        const buffer = new ArrayBuffer(8);
        const view = new DataView(buffer);
        view.setBigInt64(0, this.slotPtr.slot.value, false);
        const bytes = new Uint8Array(buffer);

        const totalSize = this.slotPtr.slot.full ? bytes.length - 2 : bytes.length;

        let size = 0;
        for (const b of bytes) {
          if (b === 0 || size === totalSize) break;
          size += 1;
        }
        return BigInt(size);
      }
      case Tag.COUNTED_HASH_MAP:
      case Tag.COUNTED_HASH_SET: {
        await this.db.core.seek(this.slotPtr.slot.value);
        return reader.readLong();
      }
      default:
        throw new UnexpectedTagException();
    }
  }

  async *[Symbol.asyncIterator](): AsyncIterator<ReadCursor> {
    const iterator = new CursorIterator(this);
    await iterator.init();
    while (await iterator.hasNext()) {
      const next = await iterator.next();
      if (next !== null) {
        yield next;
      }
    }
  }

  iterator(): CursorIterator {
    return new CursorIterator(this);
  }
}

export class Reader {
  parent: ReadCursor;
  size: bigint;
  startPosition: bigint;
  relativePosition: bigint;

  constructor(parent: ReadCursor, size: bigint, startPosition: bigint, relativePosition: bigint) {
    this.parent = parent;
    this.size = size;
    this.startPosition = startPosition;
    this.relativePosition = relativePosition;
  }

  async read(buffer: Uint8Array): Promise<number> {
    if (this.size < this.relativePosition) throw new EndOfStreamException();
    await this.parent.db.core.seek(this.startPosition + this.relativePosition);
    const readSize = Math.min(buffer.length, Number(this.size - this.relativePosition));
    if (readSize === 0) return -1;
    const reader = this.parent.db.core.reader();
    const tempBuffer = new Uint8Array(readSize);
    await reader.readFully(tempBuffer);
    buffer.set(tempBuffer);
    this.relativePosition += BigInt(readSize);
    return readSize;
  }

  async readFully(buffer: Uint8Array): Promise<void> {
    if (this.size < this.relativePosition || this.size - this.relativePosition < BigInt(buffer.length)) {
      throw new EndOfStreamException();
    }
    await this.parent.db.core.seek(this.startPosition + this.relativePosition);
    const reader = this.parent.db.core.reader();
    await reader.readFully(buffer);
    this.relativePosition += BigInt(buffer.length);
  }

  async readByte(): Promise<number> {
    const bytes = new Uint8Array(1);
    await this.readFully(bytes);
    return bytes[0];
  }

  async readShort(): Promise<number> {
    const readSize = 2;
    const bytes = new Uint8Array(readSize);
    await this.readFully(bytes);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return view.getInt16(0, false);
  }

  async readInt(): Promise<number> {
    const readSize = 4;
    const bytes = new Uint8Array(readSize);
    await this.readFully(bytes);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return view.getInt32(0, false);
  }

  async readLong(): Promise<bigint> {
    const readSize = 8;
    const bytes = new Uint8Array(readSize);
    await this.readFully(bytes);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return view.getBigInt64(0, false);
  }

  seek(position: bigint): void {
    if (position > this.size) {
      throw new InvalidOffsetException();
    }
    this.relativePosition = position;
  }
}

class IteratorLevel {
  position: bigint;
  block: Slot[];
  index: number;

  constructor(position: bigint, block: Slot[], index: number) {
    this.position = position;
    this.block = block;
    this.index = index;
  }
}

export class CursorIterator {
  cursor: ReadCursor;
  size: bigint = 0n;
  index: bigint = 0n;
  private stack: IteratorLevel[] = [];
  private nextCursorMaybe: ReadCursor | null = null;
  private initialized: boolean = false;

  constructor(cursor: ReadCursor) {
    this.cursor = cursor;
  }

  async init(): Promise<void> {
    if (this.initialized) return;
    this.initialized = true;

    switch (this.cursor.slotPtr.slot.tag) {
      case Tag.NONE:
        this.size = 0n;
        this.index = 0n;
        this.stack = [];
        break;
      case Tag.ARRAY_LIST: {
        const position = this.cursor.slotPtr.slot.value;
        await this.cursor.db.core.seek(position);
        const reader = this.cursor.db.core.reader();
        const headerBytes = new Uint8Array(ArrayListHeader.LENGTH);
        await reader.readFully(headerBytes);
        const header = ArrayListHeader.fromBytes(headerBytes);
        this.size = await this.cursor.count();
        this.index = 0n;
        this.stack = await this.initStack(this.cursor, header.ptr, INDEX_BLOCK_SIZE);
        break;
      }
      case Tag.LINKED_ARRAY_LIST: {
        const position = this.cursor.slotPtr.slot.value;
        await this.cursor.db.core.seek(position);
        const reader = this.cursor.db.core.reader();
        const headerBytes = new Uint8Array(LinkedArrayListHeader.LENGTH);
        await reader.readFully(headerBytes);
        const header = LinkedArrayListHeader.fromBytes(headerBytes);
        this.size = await this.cursor.count();
        this.index = 0n;
        this.stack = await this.initStack(this.cursor, header.ptr, LINKED_ARRAY_LIST_INDEX_BLOCK_SIZE);
        break;
      }
      case Tag.HASH_MAP:
      case Tag.HASH_SET:
        this.size = 0n;
        this.index = 0n;
        this.stack = await this.initStack(this.cursor, this.cursor.slotPtr.slot.value, INDEX_BLOCK_SIZE);
        break;
      case Tag.COUNTED_HASH_MAP:
      case Tag.COUNTED_HASH_SET:
        this.size = 0n;
        this.index = 0n;
        this.stack = await this.initStack(this.cursor, this.cursor.slotPtr.slot.value + 8n, INDEX_BLOCK_SIZE);
        break;
      default:
        throw new UnexpectedTagException();
    }
  }

  private async initStack(cursor: ReadCursor, position: bigint, blockSize: number): Promise<IteratorLevel[]> {
    await cursor.db.core.seek(position);
    const reader = cursor.db.core.reader();
    const indexBlockBytes = new Uint8Array(blockSize);
    await reader.readFully(indexBlockBytes);

    const indexBlock: Slot[] = new Array(SLOT_COUNT);
    const slotSize = blockSize / SLOT_COUNT;
    for (let i = 0; i < SLOT_COUNT; i++) {
      const slotBytes = indexBlockBytes.slice(i * slotSize, i * slotSize + Slot.LENGTH);
      indexBlock[i] = Slot.fromBytes(slotBytes);
    }

    return [new IteratorLevel(position, indexBlock, 0)];
  }

  async hasNext(): Promise<boolean> {
    await this.init();

    switch (this.cursor.slotPtr.slot.tag) {
      case Tag.NONE:
        return false;
      case Tag.ARRAY_LIST:
        return this.index < this.size;
      case Tag.LINKED_ARRAY_LIST:
        return this.index < this.size;
      case Tag.HASH_MAP:
      case Tag.HASH_SET:
      case Tag.COUNTED_HASH_MAP:
      case Tag.COUNTED_HASH_SET:
        if (this.nextCursorMaybe === null) {
          this.nextCursorMaybe = await this.nextInternal(INDEX_BLOCK_SIZE);
        }
        return this.nextCursorMaybe !== null;
      default:
        return false;
    }
  }

  async next(): Promise<ReadCursor | null> {
    await this.init();

    switch (this.cursor.slotPtr.slot.tag) {
      case Tag.NONE:
        return null;
      case Tag.ARRAY_LIST:
        if (!(await this.hasNext())) return null;
        this.index += 1n;
        return this.nextInternal(INDEX_BLOCK_SIZE);
      case Tag.LINKED_ARRAY_LIST:
        if (!(await this.hasNext())) return null;
        this.index += 1n;
        return this.nextInternal(LINKED_ARRAY_LIST_INDEX_BLOCK_SIZE);
      case Tag.HASH_MAP:
      case Tag.HASH_SET:
      case Tag.COUNTED_HASH_MAP:
      case Tag.COUNTED_HASH_SET:
        if (this.nextCursorMaybe !== null) {
          const nextCursor = this.nextCursorMaybe;
          this.nextCursorMaybe = null;
          return nextCursor;
        } else {
          return this.nextInternal(INDEX_BLOCK_SIZE);
        }
      default:
        throw new UnexpectedTagException();
    }
  }

  private async nextInternal(blockSize: number): Promise<ReadCursor | null> {
    while (this.stack.length > 0) {
      const level = this.stack[this.stack.length - 1];
      if (level.index === level.block.length) {
        this.stack.pop();
        if (this.stack.length > 0) {
          this.stack[this.stack.length - 1].index += 1;
        }
        continue;
      } else {
        const nextSlot = level.block[level.index];
        if (nextSlot.tag === Tag.INDEX) {
          const nextPos = nextSlot.value;
          await this.cursor.db.core.seek(nextPos);
          const reader = this.cursor.db.core.reader();
          const indexBlockBytes = new Uint8Array(blockSize);
          await reader.readFully(indexBlockBytes);

          const indexBlock: Slot[] = new Array(SLOT_COUNT);
          const slotSize = blockSize / SLOT_COUNT;
          for (let i = 0; i < SLOT_COUNT; i++) {
            const slotBytes = indexBlockBytes.slice(i * slotSize, i * slotSize + Slot.LENGTH);
            indexBlock[i] = Slot.fromBytes(slotBytes);
          }

          this.stack.push(new IteratorLevel(nextPos, indexBlock, 0));
          continue;
        } else {
          this.stack[this.stack.length - 1].index += 1;
          if (!nextSlot.empty()) {
            const position = level.position + BigInt(level.index * Slot.LENGTH);
            return new ReadCursor(new SlotPointer(position, nextSlot), this.cursor.db);
          } else {
            continue;
          }
        }
      }
    }
    return null;
  }
}
