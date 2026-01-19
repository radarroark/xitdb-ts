import type { DataReader, DataWriter } from './core';
import { RandomAccessMemory } from './random-access-memory';

const DEFAULT_BUFFER_SIZE = 8 * 1024 * 1024; // 8MB

export class RandomAccessBufferedFile implements DataReader, DataWriter {
  private filePath: string;
  private memory: RandomAccessMemory;
  private bufferSize: number; // flushes when the memory is >= this size
  private filePos: number;
  private memoryPos: number;

  constructor(filePath: string, bufferSize: number = DEFAULT_BUFFER_SIZE) {
    this.filePath = filePath;
    this.memory = new RandomAccessMemory();
    this.bufferSize = bufferSize;
    this.filePos = 0;
    this.memoryPos = 0;
  }

  static async create(filePath: string, bufferSize: number = DEFAULT_BUFFER_SIZE): Promise<RandomAccessBufferedFile> {
    // Create file if it doesn't exist
    const file = Bun.file(filePath);
    if (!(await file.exists())) {
      await Bun.write(filePath, new Uint8Array(0));
    }
    return new RandomAccessBufferedFile(filePath, bufferSize);
  }

  async seek(pos: number): Promise<void> {
    // flush if we are going past the end of the in-memory buffer
    if (pos > this.memoryPos + this.memory.size()) {
      await this.flush();
    }

    this.filePos = pos;

    // if the buffer is empty, set its position to this offset as well
    if (this.memory.size() === 0) {
      this.memoryPos = pos;
    }
  }

  async length(): Promise<number> {
    const file = Bun.file(this.filePath);
    return Math.max(this.memoryPos + this.memory.size(), file.size);
  }

  position(): number {
    return this.filePos;
  }

  async setLength(len: number): Promise<void> {
    await this.flush();
    const file = Bun.file(this.filePath);
    const currentData = new Uint8Array(await file.arrayBuffer());
    const newData = currentData.slice(0, len);
    await Bun.write(this.filePath, newData);
    this.filePos = Math.min(len, this.filePos);
  }

  async flush(): Promise<void> {
    if (this.memory.size() > 0) {
      const file = Bun.file(this.filePath);
      const fileSize = file.size;
      const memoryData = this.memory.toByteArray();

      if (this.memoryPos >= fileSize) {
        // Append to file
        const existingData = fileSize > 0 ? new Uint8Array(await file.arrayBuffer()) : new Uint8Array(0);
        const newData = new Uint8Array(this.memoryPos + memoryData.length);
        newData.set(existingData);
        newData.set(memoryData, this.memoryPos);
        await Bun.write(this.filePath, newData);
      } else {
        // Overwrite in file
        const existingData = new Uint8Array(await file.arrayBuffer());
        const newSize = Math.max(existingData.length, this.memoryPos + memoryData.length);
        const newData = new Uint8Array(newSize);
        newData.set(existingData);
        newData.set(memoryData, this.memoryPos);
        await Bun.write(this.filePath, newData);
      }

      this.memoryPos = 0;
      this.memory.reset();
    }
  }

  async sync(): Promise<void> {
    await this.flush();
    // Bun.write is synchronous to disk
  }

  // DataWriter interface

  async write(buffer: Uint8Array): Promise<void> {
    if (this.memory.size() + buffer.length > this.bufferSize) {
      await this.flush();
    }

    if (this.filePos >= this.memoryPos && this.filePos <= this.memoryPos + this.memory.size()) {
      this.memory.seek(this.filePos - this.memoryPos);
      await this.memory.write(buffer);
    } else {
      // Write directly to file
      const file = Bun.file(this.filePath);
      const fileSize = file.size;

      if (this.filePos >= fileSize) {
        // Append to file
        const existingData = fileSize > 0 ? new Uint8Array(await file.arrayBuffer()) : new Uint8Array(0);
        const newData = new Uint8Array(this.filePos + buffer.length);
        newData.set(existingData);
        newData.set(buffer, this.filePos);
        await Bun.write(this.filePath, newData);
      } else {
        // Overwrite in file
        const existingData = new Uint8Array(await file.arrayBuffer());
        const newSize = Math.max(existingData.length, this.filePos + buffer.length);
        const newData = new Uint8Array(newSize);
        newData.set(existingData);
        newData.set(buffer, this.filePos);
        await Bun.write(this.filePath, newData);
      }
    }

    this.filePos += buffer.length;
  }

  async writeByte(v: number): Promise<void> {
    await this.write(new Uint8Array([v & 0xff]));
  }

  async writeShort(v: number): Promise<void> {
    const buffer = new ArrayBuffer(2);
    const view = new DataView(buffer);
    view.setInt16(0, v & 0xffff, false); // big-endian
    await this.write(new Uint8Array(buffer));
  }

  async writeLong(v: bigint): Promise<void> {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    view.setBigInt64(0, v, false); // big-endian
    await this.write(new Uint8Array(buffer));
  }

  // DataReader interface

  async readFully(buffer: Uint8Array): Promise<void> {
    let pos = 0;

    // read from the disk -- before the in-memory buffer
    if (this.filePos < this.memoryPos) {
      const sizeBeforeMem = Math.min(this.memoryPos - this.filePos, buffer.length);
      const file = Bun.file(this.filePath);
      const slice = file.slice(this.filePos, this.filePos + sizeBeforeMem);
      const data = await slice.arrayBuffer();
      buffer.set(new Uint8Array(data), pos);
      pos += sizeBeforeMem;
      this.filePos += sizeBeforeMem;
    }

    if (pos === buffer.length) return;

    // read from the in-memory buffer
    if (this.filePos >= this.memoryPos && this.filePos < this.memoryPos + this.memory.size()) {
      const memPos = this.filePos - this.memoryPos;
      const sizeInMem = Math.min(this.memory.size() - memPos, buffer.length - pos);
      this.memory.seek(memPos);
      const memBuffer = new Uint8Array(sizeInMem);
      await this.memory.readFully(memBuffer);
      buffer.set(memBuffer, pos);
      pos += sizeInMem;
      this.filePos += sizeInMem;
    }

    if (pos === buffer.length) return;

    // read from the disk -- after the in-memory buffer
    if (this.filePos >= this.memoryPos + this.memory.size()) {
      const sizeAfterMem = buffer.length - pos;
      const file = Bun.file(this.filePath);
      const slice = file.slice(this.filePos, this.filePos + sizeAfterMem);
      const data = await slice.arrayBuffer();
      buffer.set(new Uint8Array(data), pos);
      pos += sizeAfterMem;
      this.filePos += sizeAfterMem;
    }
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

  async readLong(): Promise<bigint> {
    const bytes = new Uint8Array(8);
    await this.readFully(bytes);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return view.getBigInt64(0, false); // big-endian
  }
}
