export interface DataReader {
  readFully(buffer: Uint8Array): Promise<void>;
  readByte(): Promise<number>;
  readShort(): Promise<number>;
  readInt(): Promise<number>;
  readLong(): Promise<bigint>;
}

export interface DataWriter {
  write(buffer: Uint8Array): Promise<void>;
  writeByte(v: number): Promise<void>;
  writeShort(v: number): Promise<void>;
  writeLong(v: bigint): Promise<void>;
}

export interface Core {
  reader(): DataReader;
  writer(): DataWriter;
  length(): Promise<bigint>;
  seek(pos: bigint): Promise<void>;
  position(): Promise<bigint>;
  setLength(len: bigint): Promise<void>;
  flush(): Promise<void>;
  sync(): Promise<void>;
}
