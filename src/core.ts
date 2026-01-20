export interface DataReader {
  readFully(buffer: Uint8Array): Promise<void>;
  readByte(): Promise<number>;
  readShort(): Promise<number>;
  readInt(): Promise<number>;
  readLong(): Promise<number>;
}

export interface DataWriter {
  write(buffer: Uint8Array): Promise<void>;
  writeByte(v: number): Promise<void>;
  writeShort(v: number): Promise<void>;
  writeLong(v: number): Promise<void>;
}

export interface Core {
  reader(): DataReader;
  writer(): DataWriter;
  length(): Promise<number>;
  seek(pos: number): Promise<void>;
  position(): number;
  setLength(len: number): Promise<void>;
  flush(): Promise<void>;
  sync(): Promise<void>;
}
