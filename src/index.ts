// Tag
export { Tag, tagValueOf } from './tag';

// Slot
export { Slot } from './slot';
export { SlotPointer } from './slot-pointer';
export type { Slotted } from './slotted';

// Writeable Data
export { Uint, Int, Float, Bytes, type WriteableData } from './writeable-data';

// Exceptions
export {
  DatabaseException,
  NotImplementedException,
  UnreachableException,
  InvalidDatabaseException,
  InvalidVersionException,
  InvalidHashSizeException,
  KeyNotFoundException,
  WriteNotAllowedException,
  UnexpectedTagException,
  CursorNotWriteableException,
  ExpectedTxStartException,
  KeyOffsetExceededException,
  PathPartMustBeAtEndException,
  StreamTooLongException,
  EndOfStreamException,
  InvalidOffsetException,
  InvalidTopLevelTypeException,
  ExpectedUnsignedLongException,
  NoAvailableSlotsException,
  MustSetNewSlotsToFullException,
  EmptySlotException,
  ExpectedRootNodeException,
  InvalidFormatTagSizeException,
  UnexpectedWriterPositionException,
  MaxShiftExceededException,
} from './exceptions';

// Core
export type { Core, DataReader, DataWriter } from './core';
export { CoreMemory } from './core-memory';
export { CoreFile } from './core-file';
export { CoreBufferedFile } from './core-buffered-file';
export { Hasher } from './hasher';

// Database
export {
  Database,
  WriteMode,
  Header,
  ArrayListHeader,
  TopLevelArrayListHeader,
  LinkedArrayListHeader,
  KeyValuePair,
  LinkedArrayListSlot,
  LinkedArrayListSlotPointer,
  LinkedArrayListBlockInfo,
  VERSION,
  MAGIC_NUMBER,
  BIT_COUNT,
  SLOT_COUNT,
  MASK,
  INDEX_BLOCK_SIZE,
  LINKED_ARRAY_LIST_INDEX_BLOCK_SIZE,
  MAX_BRANCH_LENGTH,
  // PathParts
  type PathPart,
  ArrayListInit,
  ArrayListGet,
  ArrayListAppend,
  ArrayListSlice,
  LinkedArrayListInit,
  LinkedArrayListGet,
  LinkedArrayListAppend,
  LinkedArrayListSlice,
  LinkedArrayListConcat,
  LinkedArrayListInsert,
  LinkedArrayListRemove,
  HashMapInit,
  HashMapGet,
  HashMapRemove,
  WriteData,
  Context,
  // HashMapGetTarget
  type HashMapGetTarget,
  HashMapGetKVPair,
  HashMapGetKey,
  HashMapGetValue,
  type ContextFunction,
} from './database';

// Cursors
export { ReadCursor, Reader, CursorIterator, KeyValuePairCursor } from './read-cursor';
export { WriteCursor, Writer, WriteCursorIterator, WriteKeyValuePairCursor } from './write-cursor';

// Collections
export { ReadArrayList } from './read-array-list';
export { WriteArrayList } from './write-array-list';
export { ReadHashMap } from './read-hash-map';
export { WriteHashMap } from './write-hash-map';
export { ReadHashSet } from './read-hash-set';
export { WriteHashSet } from './write-hash-set';
export { ReadLinkedArrayList } from './read-linked-array-list';
export { WriteLinkedArrayList } from './write-linked-array-list';
export { ReadCountedHashMap } from './read-counted-hash-map';
export { WriteCountedHashMap } from './write-counted-hash-map';
export { ReadCountedHashSet } from './read-counted-hash-set';
export { WriteCountedHashSet } from './write-counted-hash-set';
