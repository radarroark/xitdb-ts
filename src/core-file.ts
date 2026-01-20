import type { Core, DataReader, DataWriter } from './core';
import * as fs from 'fs/promises';
import type { FileHandle } from 'fs/promises';

export class CoreFile implements Core {
  public filePath: string;
  private _position: number = 0;
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

  async length(): Promise<number> {
    const stats = await this.fileHandle.stat();
    return stats.size;
  }

  async seek(pos: number): Promise<void> {
    this._position = pos;
  }

  position(): number {
    return this._position;
  }

  async setLength(len: number): Promise<void> {
    await this.fileHandle.truncate(len);
  }

  async flush(): Promise<void> {
  }

  async sync(): Promise<void> {
    await this.fileHandle.sync();
  }

  [Symbol.dispose]() {
    import("fs").then(fs => {
      fs.closeSync(this.fileHandle.fd);
    });
  }
}

class FileDataReader implements DataReader {
  private core: CoreFile;

  constructor(core: CoreFile) {
    this.core = core;
  }

  async readFully(b: Uint8Array): Promise<void> {
    const position = this.core.position();
    await this.core.fileHandle.readv([b], position);
    this.core.seek(position + b.length);
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

  async readLong(): Promise<number> {
    const bytes = new Uint8Array(8);
    await this.readFully(bytes);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return Number(view.getBigInt64(0, false));
  }
}

class FileDataWriter implements DataWriter {
  private core: CoreFile;

  constructor(core: CoreFile) {
    this.core = core;
  }

  async write(buffer: Uint8Array): Promise<void> {
    const position = this.core.position();
    await this.core.fileHandle.writev([buffer], position);
    this.core.seek(position + buffer.length);
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

  async writeLong(v: number): Promise<void> {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    view.setBigInt64(0, BigInt(v), false);
    await this.write(new Uint8Array(buffer));
  }
}