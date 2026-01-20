import type { Core, DataReader, DataWriter } from './core';
import { CoreFile } from './core-file';
import { CoreMemory } from './core-memory';

export class CoreBufferedFile implements Core {
  public file: RandomAccessBufferedFile;

  constructor(file: RandomAccessBufferedFile) {
    this.file = file;
  }

  static async create(filePath: string, bufferSize?: bigint): Promise<CoreBufferedFile> {
    const file = await RandomAccessBufferedFile.create(filePath, bufferSize);
    return new CoreBufferedFile(file);
  }

  reader(): DataReader {
    return this.file;
  }

  writer(): DataWriter {
    return this.file;
  }

  async length(): Promise<bigint> {
    return await this.file.length();
  }

  async seek(pos: bigint): Promise<void> {
    await this.file.seek(pos);
  }

  position(): bigint {
    return BigInt(this.file.position());
  }

  async setLength(len: bigint): Promise<void> {
    await this.file.setLength(len);
  }

  async flush(): Promise<void> {
    await this.file.flush();
  }

  async sync(): Promise<void> {
    await this.file.sync();
  }

  [Symbol.dispose]() {
    this.file.file[Symbol.dispose]();
  }
}

const DEFAULT_BUFFER_SIZE = BigInt(8 * 1024 * 1024); // 8MB

class RandomAccessBufferedFile implements DataReader, DataWriter {
  public file: CoreFile;
  private memory: CoreMemory;
  private bufferSize: bigint; // flushes when the memory is >= this size
  private filePos: bigint;
  private memoryPos: bigint;

  private constructor(file: CoreFile, bufferSize: bigint) {
    this.file = file;
    this.memory = new CoreMemory();
    this.bufferSize = bufferSize;
    this.filePos = 0n;
    this.memoryPos = 0n;
  }

  static async create(filePath: string, bufferSize: bigint = DEFAULT_BUFFER_SIZE): Promise<RandomAccessBufferedFile> {
    const file = await CoreFile.create(filePath);
    return new RandomAccessBufferedFile(file, bufferSize);
  }

  async seek(pos: bigint): Promise<void> {
    // flush if we are going past the end of the in-memory buffer
    if (pos > this.memoryPos + await this.memory.length()) {
      await this.flush();
    }

    this.filePos = pos;

    // if the buffer is empty, set its position to this offset as well
    if (await this.memory.length() === 0n) {
      this.memoryPos = pos;
    }
  }

  async length(): Promise<bigint> {
    return BigInt(Math.max(Number(this.memoryPos + await this.memory.length()), Number(await this.file.length())));
  }

  position(): bigint {
    return BigInt(this.filePos);
  }

  async setLength(len: bigint): Promise<void> {
    await this.flush();
    await this.file.setLength(len);
    this.filePos = BigInt(Math.min(Number(len), Number(this.filePos)));
  }

  async flush(): Promise<void> {
    if (await this.memory.length() > 0) {
      await this.file.seek(BigInt(this.memoryPos));
      await this.file.writer().write(this.memory.memory.toByteArray());

      this.memoryPos = 0n;
      this.memory.memory.reset();
    }
  }

  async sync(): Promise<void> {
    await this.flush();
    await this.file.sync();
  }

  // DataWriter interface

  async write(buffer: Uint8Array): Promise<void> {
    if (await this.memory.length() + BigInt(buffer.length) > this.bufferSize) {
      await this.flush();
    }

    if (this.filePos >= this.memoryPos && this.filePos <= this.memoryPos + await this.memory.length()) {
      this.memory.seek(this.filePos - this.memoryPos);
      await this.memory.memory.write(buffer);
    } else {
      // Write directly to file
      await this.file.seek(BigInt(this.filePos));
      await this.file.writer().write(buffer);
    }

    this.filePos += BigInt(buffer.length);
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
      const sizeBeforeMem = Math.min(Number(this.memoryPos - this.filePos), buffer.length);
      const tempBuffer = new Uint8Array(sizeBeforeMem);
      await this.file.seek(BigInt(this.filePos));
      await this.file.reader().readFully(tempBuffer);
      buffer.set(tempBuffer, pos);
      pos += sizeBeforeMem;
      this.filePos += BigInt(sizeBeforeMem);
    }

    if (pos === buffer.length) return;

    // read from the in-memory buffer
    if (this.filePos >= this.memoryPos && this.filePos < this.memoryPos + await this.memory.length()) {
      const memPos = this.filePos - this.memoryPos;
      const sizeInMem = Math.min(Number(await this.memory.length() - memPos), buffer.length - pos);
      this.memory.seek(memPos);
      const memBuffer = new Uint8Array(sizeInMem);
      await this.memory.memory.readFully(memBuffer);
      buffer.set(memBuffer, pos);
      pos += sizeInMem;
      this.filePos += BigInt(sizeInMem);
    }

    if (pos === buffer.length) return;

    // read from the disk -- after the in-memory buffer
    if (this.filePos >= this.memoryPos + await this.memory.length()) {
      const sizeAfterMem = buffer.length - pos;
      const tempBuffer = new Uint8Array(sizeAfterMem);
      await this.file.seek(BigInt(this.filePos));
      await this.file.reader().readFully(tempBuffer);
      buffer.set(tempBuffer, pos);
      pos += sizeAfterMem;
      this.filePos += BigInt(sizeAfterMem);
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