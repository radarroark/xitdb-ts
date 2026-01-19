import type { Core, DataReader, DataWriter } from './core';

class FileDataReader implements DataReader {
  private file: Bun.FileBlob;
  private position: number;
  private buffer: Uint8Array | null = null;
  private bufferStart: number = 0;
  private bufferEnd: number = 0;

  constructor(file: Bun.FileBlob, position: number) {
    this.file = file;
    this.position = position;
  }

  setPosition(pos: number): void {
    this.position = pos;
  }

  getPosition(): number {
    return this.position;
  }

  async readFully(b: Uint8Array): Promise<void> {
    const slice = this.file.slice(this.position, this.position + b.length);
    const data = await slice.arrayBuffer();
    b.set(new Uint8Array(data));
    this.position += b.length;
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
  private filePath: string;
  private position: number;
  private pendingWrites: { position: number; data: Uint8Array }[] = [];

  constructor(filePath: string, position: number) {
    this.filePath = filePath;
    this.position = position;
  }

  setPosition(pos: number): void {
    this.position = pos;
  }

  getPosition(): number {
    return this.position;
  }

  async write(buffer: Uint8Array): Promise<void> {
    const file = Bun.file(this.filePath);
    const currentSize = file.size;

    if (this.position >= currentSize) {
      // Append to file
      const existingData = currentSize > 0 ? new Uint8Array(await file.arrayBuffer()) : new Uint8Array(0);
      const newData = new Uint8Array(this.position + buffer.length);
      newData.set(existingData);
      newData.set(buffer, this.position);
      await Bun.write(this.filePath, newData);
    } else {
      // Overwrite in file
      const existingData = new Uint8Array(await file.arrayBuffer());
      const newSize = Math.max(existingData.length, this.position + buffer.length);
      const newData = new Uint8Array(newSize);
      newData.set(existingData);
      newData.set(buffer, this.position);
      await Bun.write(this.filePath, newData);
    }

    this.position += buffer.length;
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

export class CoreFile implements Core {
  private filePath: string;
  private _position: number = 0;
  private _reader: FileDataReader;
  private _writer: FileDataWriter;

  constructor(filePath: string) {
    this.filePath = filePath;
    this._reader = new FileDataReader(Bun.file(filePath), 0);
    this._writer = new FileDataWriter(filePath, 0);
  }

  static async create(filePath: string): Promise<CoreFile> {
    // Create file if it doesn't exist
    const file = Bun.file(filePath);
    if (!await file.exists()) {
      await Bun.write(filePath, new Uint8Array(0));
    }
    return new CoreFile(filePath);
  }

  reader(): DataReader {
    return this._reader;
  }

  writer(): DataWriter {
    return this._writer;
  }

  async length(): Promise<bigint> {
    const file = Bun.file(this.filePath);
    return BigInt(file.size);
  }

  async seek(pos: bigint): Promise<void> {
    this._position = Number(pos);
    this._reader.setPosition(this._position);
    this._writer.setPosition(this._position);
  }

  async position(): Promise<bigint> {
    return BigInt(this._reader.getPosition());
  }

  async setLength(len: bigint): Promise<void> {
    const file = Bun.file(this.filePath);
    const currentData = new Uint8Array(await file.arrayBuffer());
    const newData = currentData.slice(0, Number(len));
    await Bun.write(this.filePath, newData);
  }

  async flush(): Promise<void> {
    // Bun.write is synchronous to disk
  }

  async sync(): Promise<void> {
    // Bun.write is synchronous to disk
  }
}
