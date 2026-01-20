import type { Core, DataReader, DataWriter } from './core';
import * as fs from 'fs/promises';
import type { FileHandle } from 'fs/promises';

export class CoreFile implements Core {
  public filePath: string;
  private _position: bigint = 0n;
  public fileHandle: FileHandle;

  private constructor(filePath: string, fileHandle: FileHandle) {
    this.filePath = filePath;
    this.fileHandle = fileHandle;
  }

  static async create(filePath: string): Promise<CoreFile> {
    // Create file if it doesn't exist
    try {
      await fs.access(filePath);
    } catch {
      await fs.writeFile(filePath, new Uint8Array(0));
    }
    // Open file handle for reading and writing
    const fileHandle = await fs.open(filePath, 'r+');
    return new CoreFile(filePath, fileHandle);
  }

  reader(): DataReader {
    return new FileDataReader(this);
  }

  writer(): DataWriter {
    return new FileDataWriter(this);
  }

  async length(): Promise<bigint> {
    const stats = await this.fileHandle.stat();
    return BigInt(stats.size);
  }

  async seek(pos: bigint): Promise<void> {
    this._position = pos;
  }

  position(): bigint {
    return this._position;
  }

  async setLength(len: bigint): Promise<void> {
    await this.fileHandle.truncate(Number(len));
  }

  async flush(): Promise<void> {
  }

  async sync(): Promise<void> {
    await this.fileHandle.sync();
  }

  async close(): Promise<void> {
    await this.fileHandle.close();
  }
}

class FileDataReader implements DataReader {
  private core: CoreFile;

  constructor(core: CoreFile) {
    this.core = core;
  }

  async readFully(b: Uint8Array): Promise<void> {
    const position = this.core.position();
    await this.core.fileHandle.readv([b], Number(position));
    this.core.seek(position + BigInt(b.length));
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
    return view.getInt16(0, false);
  }

  async readInt(): Promise<number> {
    const bytes = new Uint8Array(4);
    await this.readFully(bytes);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return view.getInt32(0, false);
  }

  async readLong(): Promise<bigint> {
    const bytes = new Uint8Array(8);
    await this.readFully(bytes);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return view.getBigInt64(0, false);
  }
}

class FileDataWriter implements DataWriter {
  private core: CoreFile;

  constructor(core: CoreFile) {
    this.core = core;
  }

  async write(buffer: Uint8Array): Promise<void> {
    const position = this.core.position();
    await this.core.fileHandle.writev([buffer], Number(position));
    this.core.seek(position + BigInt(buffer.length));
  }

  async writeByte(v: number): Promise<void> {
    await this.write(new Uint8Array([v & 0xff]));
  }

  async writeShort(v: number): Promise<void> {
    const buffer = new ArrayBuffer(2);
    const view = new DataView(buffer);
    view.setInt16(0, v, false);
    await this.write(new Uint8Array(buffer));
  }

  async writeLong(v: bigint): Promise<void> {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    view.setBigInt64(0, v, false);
    await this.write(new Uint8Array(buffer));
  }
}