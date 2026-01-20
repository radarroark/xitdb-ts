import type { Core, DataReader, DataWriter } from './core';
import { RandomAccessBufferedFile } from './random-access-buffered-file';

export class CoreBufferedFile implements Core {
  public file: RandomAccessBufferedFile;

  constructor(file: RandomAccessBufferedFile) {
    this.file = file;
  }

  static async create(filePath: string, bufferSize?: number): Promise<CoreBufferedFile> {
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
    await this.file.seek(Number(pos));
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
}
