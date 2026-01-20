import type { Core, DataReader, DataWriter } from './core';

export class CoreMemory implements Core {
  public memory: RandomAccessMemory;

  constructor() {
    this.memory = new RandomAccessMemory();
  }

  reader(): DataReader {
    return this.memory;
  }

  writer(): DataWriter {
    return this.memory;
  }

  async length(): Promise<number> {
    return this.memory.size();
  }

  async seek(pos: number): Promise<void> {
    this.memory.seek(pos);
  }

  position(): number {
    return this.memory.getPosition();
  }

  async setLength(len: number): Promise<void> {
    this.memory.setLength(len);
  }

  async flush(): Promise<void> {
    // no-op for in-memory
  }

  async sync(): Promise<void> {
    // no-op for in-memory
  }
}

class RandomAccessMemory implements DataReader, DataWriter {
  private buffer: Uint8Array;
  private _position: number = 0;
  private _count: number = 0;

  constructor(initialSize: number = 1024) {
    this.buffer = new Uint8Array(initialSize);
  }

  private ensureCapacity(minCapacity: number): void {
    if (minCapacity > this.buffer.length) {
      let newCapacity = this.buffer.length * 2;
      if (newCapacity < minCapacity) {
        newCapacity = minCapacity;
      }
      const newBuffer = new Uint8Array(newCapacity);
      newBuffer.set(this.buffer.subarray(0, this._count));
      this.buffer = newBuffer;
    }
  }

  size(): number {
    return this._count;
  }

  seek(pos: number): void {
    if (pos > this._count) {
      this._position = this._count;
    } else {
      this._position = pos;
    }
  }

  getPosition(): number {
    return this._position;
  }

  setLength(len: number): void {
    if (len === 0) {
      this.reset();
    } else {
      if (len > this._count) throw new Error('Cannot extend length');
      this._count = len;
      if (this._position > len) {
        this._position = len;
      }
    }
  }

  reset(): void {
    this._count = 0;
    this._position = 0;
  }

  toByteArray(): Uint8Array {
    return this.buffer.slice(0, this._count);
  }

  // DataWriter interface
  async write(data: Uint8Array): Promise<void> {
    const pos = this._position;
    if (pos < this._count) {
      const bytesBeforeEnd = Math.min(data.length, this._count - pos);
      for (let i = 0; i < bytesBeforeEnd; i++) {
        this.buffer[pos + i] = data[i];
      }

      if (bytesBeforeEnd < data.length) {
        const bytesAfterEnd = data.length - bytesBeforeEnd;
        this.ensureCapacity(this._count + bytesAfterEnd);
        this.buffer.set(data.subarray(bytesBeforeEnd), this._count);
        this._count += bytesAfterEnd;
      }
    } else {
      this.ensureCapacity(this._count + data.length);
      this.buffer.set(data, this._count);
      this._count += data.length;
    }

    this._position = pos + data.length;
  }

  async writeByte(v: number): Promise<void> {
    await this.write(new Uint8Array([v & 0xff]));
  }

  async writeShort(v: number): Promise<void> {
    const buffer = new ArrayBuffer(2);
    const view = new DataView(buffer);
    view.setInt16(0, v, false); // big-endian
    await this.write(new Uint8Array(buffer));
  }

  async writeLong(v: number): Promise<void> {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    view.setBigInt64(0, BigInt(v), false);
    await this.write(new Uint8Array(buffer));
  }

  // DataReader interface
  async readFully(b: Uint8Array): Promise<void> {
    const pos = this._position;
    if (pos + b.length > this._count) {
      throw new Error('End of stream');
    }
    b.set(this.buffer.subarray(pos, pos + b.length));
    this._position = pos + b.length;
  }

  async readByte(): Promise<number> {
    const bytes = new Uint8Array(1);
    await this.readFully(bytes);
    return bytes[0];
  }

  async readShort(): Promise<number> {
    const bytes = new Uint8Array(2);
    await this.readFully(bytes);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return view.getInt16(0, false); // big-endian
  }

  async readInt(): Promise<number> {
    const bytes = new Uint8Array(4);
    await this.readFully(bytes);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return view.getInt32(0, false); // big-endian
  }

  async readLong(): Promise<number> {
    const bytes = new Uint8Array(8);
    await this.readFully(bytes);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return Number(view.getBigInt64(0, false));
  }
}