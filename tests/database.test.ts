import { expect, test, describe } from 'bun:test';
import {
  Database,
  Tag,
  Hasher,
  CoreMemory,
  CoreFile,
  CoreBufferedFile,
  ReadArrayList,
  WriteArrayList,
  ReadHashMap,
  WriteHashMap,
  ReadHashSet,
  WriteHashSet,
  ReadLinkedArrayList,
  WriteLinkedArrayList,
  ReadCountedHashMap,
  WriteCountedHashMap,
  ReadCountedHashSet,
  WriteCountedHashSet,
  Bytes,
  Uint,
  Int,
  Float,
  Slot,
  SlotPointer,
  InvalidTopLevelTypeException,
  InvalidDatabaseException,
  InvalidVersionException,
  KeyNotFoundException,
  EndOfStreamException,
  ArrayListInit,
  ArrayListGet,
  ArrayListAppend,
  ArrayListSlice,
  HashMapInit,
  HashMapGet,
  HashMapGetValue,
  HashMapGetKey,
  HashMapRemove,
  LinkedArrayListInit,
  LinkedArrayListGet,
  LinkedArrayListAppend,
  LinkedArrayListSlice,
  LinkedArrayListConcat,
  LinkedArrayListInsert,
  LinkedArrayListRemove,
  WriteData,
  Context,
  VERSION,
  SLOT_COUNT,
  MASK,
  type Core,
  type WriteableData,
} from '../src';
import { WriteCursor } from '../src/write-cursor';
import { tmpdir } from 'os';
import { join } from 'path';
import { mkdtemp, rm } from 'fs/promises';

const MAX_READ_BYTES = 1024;

describe('Database High Level API', () => {
  test('high level API with in-memory storage', async () => {
    const core = new CoreMemory();
    const hasher = new Hasher('SHA-1');
    await testHighLevelApi(core, hasher, null);
  });

  test('high level API with file storage', async () => {
    const tmpDir = await mkdtemp(join(tmpdir(), 'xitdb-'));
    const filePath = join(tmpDir, 'test.db');
    try {
      using core = await CoreFile.create(filePath);
      const hasher = new Hasher('SHA-1');
      await testHighLevelApi(core, hasher, filePath);
    } finally {
      await rm(tmpDir, { recursive: true });
    }
  });

  test('high level API with buffered file storage', async () => {
    const tmpDir = await mkdtemp(join(tmpdir(), 'xitdb-'));
    const filePath = join(tmpDir, 'test.db');
    try {
      using core = await CoreBufferedFile.create(filePath);
      const hasher = new Hasher('SHA-1');
      await testHighLevelApi(core, hasher, filePath);
    } finally {
      await rm(tmpDir, { recursive: true });
    }
  });

  test('not using array list at top level - hash map', async () => {
    const core = new CoreMemory();
    const hasher = new Hasher('SHA-1');
    const db = await Database.create(core, hasher);

    const map = await WriteHashMap.create(await db.rootCursor());
    await map.putByString('foo', new Bytes('foo'));
    await map.putByString('bar', new Bytes('bar'));

    // init inner map
    {
      const innerMapCursor = await map.putCursorByString('inner-map');
      await WriteHashMap.create(innerMapCursor);
    }

    // re-init inner map
    {
      const innerMapCursor = await map.putCursorByString('inner-map');
      await WriteHashMap.create(innerMapCursor);
    }
  });

  test('not using array list at top level - linked array list throws', async () => {
    const core = new CoreMemory();
    const hasher = new Hasher('SHA-1');
    const db = await Database.create(core, hasher);

    await expect(WriteLinkedArrayList.create(await db.rootCursor())).rejects.toThrow(
      InvalidTopLevelTypeException
    );
  });

  test('read database from fixture', async () => {
    const filePath = new URL('./fixtures/test.db', import.meta.url).pathname;
    using core = await CoreFile.create(filePath);
    const hasher = new Hasher('SHA-1');
    const db = await Database.create(core, hasher);
    const history = new ReadArrayList(await db.rootCursor());

    // First moment
    {
      const momentCursor = await history.getCursor(0);
      expect(momentCursor).not.toBeNull();
      const moment = await ReadHashMap.create(momentCursor!);

      const fooCursor = await moment.getCursorByString('foo');
      expect(fooCursor).not.toBeNull();
      const fooValue = await fooCursor!.readBytes(MAX_READ_BYTES);
      expect(new TextDecoder().decode(fooValue)).toBe('foo');

      const fooSlot = await moment.getSlotByString('foo');
      expect(fooSlot?.tag).toBe(Tag.SHORT_BYTES);
      const barSlot = await moment.getSlotByString('bar');
      expect(barSlot?.tag).toBe(Tag.SHORT_BYTES);

      const fruitsCursor = await moment.getCursorByString('fruits');
      expect(fruitsCursor).not.toBeNull();
      const fruits = new ReadArrayList(fruitsCursor!);
      expect(await fruits.count()).toBe(3);

      const appleCursor = await fruits.getCursor(0);
      expect(appleCursor).not.toBeNull();
      const appleValue = await appleCursor!.readBytes(MAX_READ_BYTES);
      expect(new TextDecoder().decode(appleValue)).toBe('apple');

      const peopleCursor = await moment.getCursorByString('people');
      expect(peopleCursor).not.toBeNull();
      const people = new ReadArrayList(peopleCursor!);
      expect(await people.count()).toBe(2);

      const aliceCursor = await people.getCursor(0);
      expect(aliceCursor).not.toBeNull();
      const alice = await ReadHashMap.create(aliceCursor!);
      const aliceAgeCursor = await alice.getCursorByString('age');
      expect(aliceAgeCursor).not.toBeNull();
      expect(aliceAgeCursor!.readUint()).toBe(25);

      const todosCursor = await moment.getCursorByString('todos');
      expect(todosCursor).not.toBeNull();
      const todos = new ReadLinkedArrayList(todosCursor!);
      expect(await todos.count()).toBe(3);

      const todoCursor = await todos.getCursor(0);
      expect(todoCursor).not.toBeNull();
      const todoValue = await todoCursor!.readBytes(MAX_READ_BYTES);
      expect(new TextDecoder().decode(todoValue)).toBe('Pay the bills');

      // Test iterating over people
      const peopleIter = people.iterator();
      await peopleIter.init();
      while (await peopleIter.hasNext()) {
        const personCursor = await peopleIter.next();
        expect(personCursor).not.toBeNull();
        const person = await ReadHashMap.create(personCursor!);
        const personIter = person.iterator();
        await personIter.init();
        while (await personIter.hasNext()) {
          const kvPairCursor = await personIter.next();
          expect(kvPairCursor).not.toBeNull();
          await kvPairCursor!.readKeyValuePair();
        }
      }

      // Counted hash map
      {
        const lettersCountedMapCursor = await moment.getCursorByString('letters-counted-map');
        expect(lettersCountedMapCursor).not.toBeNull();
        const lettersCountedMap = await ReadCountedHashMap.create(lettersCountedMapCursor!);
        expect(await lettersCountedMap.count()).toBe(2);

        const iter = lettersCountedMap.iterator();
        await iter.init();
        let count = 0;
        while (await iter.hasNext()) {
          const kvPairCursor = await iter.next();
          expect(kvPairCursor).not.toBeNull();
          const kvPair = await kvPairCursor!.readKeyValuePair();
          await kvPair.keyCursor.readBytes(MAX_READ_BYTES);
          count += 1;
        }
        expect(count).toBe(2);
      }

      // Hash set
      {
        const lettersSetCursor = await moment.getCursorByString('letters-set');
        expect(lettersSetCursor).not.toBeNull();
        const lettersSet = await ReadHashSet.create(lettersSetCursor!);
        expect(await lettersSet.getCursorByString('a')).not.toBeNull();
        expect(await lettersSet.getCursorByString('c')).not.toBeNull();

        const iter = lettersSet.iterator();
        await iter.init();
        let count = 0;
        while (await iter.hasNext()) {
          const kvPairCursor = await iter.next();
          expect(kvPairCursor).not.toBeNull();
          const kvPair = await kvPairCursor!.readKeyValuePair();
          await kvPair.keyCursor.readBytes(MAX_READ_BYTES);
          count += 1;
        }
        expect(count).toBe(2);
      }

      // Counted hash set
      {
        const lettersCountedSetCursor = await moment.getCursorByString('letters-counted-set');
        expect(lettersCountedSetCursor).not.toBeNull();
        const lettersCountedSet = await ReadCountedHashSet.create(lettersCountedSetCursor!);
        expect(await lettersCountedSet.count()).toBe(2);

        const iter = lettersCountedSet.iterator();
        await iter.init();
        let count = 0;
        while (await iter.hasNext()) {
          const kvPairCursor = await iter.next();
          expect(kvPairCursor).not.toBeNull();
          const kvPair = await kvPairCursor!.readKeyValuePair();
          await kvPair.keyCursor.readBytes(MAX_READ_BYTES);
          count += 1;
        }
        expect(count).toBe(2);
      }
    }

    // Second moment
    {
      const momentCursor = await history.getCursor(1);
      expect(momentCursor).not.toBeNull();
      const moment = await ReadHashMap.create(momentCursor!);

      expect(await moment.getCursorByString('bar')).toBeNull();

      const fruitsKeyCursor = await moment.getKeyCursorByString('fruits');
      expect(fruitsKeyCursor).not.toBeNull();
      const fruitsKeyValue = await fruitsKeyCursor!.readBytes(MAX_READ_BYTES);
      expect(new TextDecoder().decode(fruitsKeyValue)).toBe('fruits');

      const fruitsCursor = await moment.getCursorByString('fruits');
      expect(fruitsCursor).not.toBeNull();
      const fruits = new ReadArrayList(fruitsCursor!);
      expect(await fruits.count()).toBe(2);

      const fruitsKVCursor = await moment.getKeyValuePairByString('fruits');
      expect(fruitsKVCursor).not.toBeNull();
      expect(fruitsKVCursor!.keyCursor.slotPtr.slot.tag).toBe(Tag.SHORT_BYTES);
      expect(fruitsKVCursor!.valueCursor.slotPtr.slot.tag).toBe(Tag.ARRAY_LIST);

      const lemonCursor = await fruits.getCursor(0);
      expect(lemonCursor).not.toBeNull();
      const lemonValue = await lemonCursor!.readBytes(MAX_READ_BYTES);
      expect(new TextDecoder().decode(lemonValue)).toBe('lemon');

      const peopleCursor = await moment.getCursorByString('people');
      expect(peopleCursor).not.toBeNull();
      const people = new ReadArrayList(peopleCursor!);
      expect(await people.count()).toBe(2);

      const aliceCursor = await people.getCursor(0);
      expect(aliceCursor).not.toBeNull();
      const alice = await ReadHashMap.create(aliceCursor!);
      const aliceAgeCursor = await alice.getCursorByString('age');
      expect(aliceAgeCursor).not.toBeNull();
      expect(aliceAgeCursor!.readUint()).toBe(26);

      const todosCursor = await moment.getCursorByString('todos');
      expect(todosCursor).not.toBeNull();
      const todos = new ReadLinkedArrayList(todosCursor!);
      expect(await todos.count()).toBe(1);

      const todoCursor = await todos.getCursor(0);
      expect(todoCursor).not.toBeNull();
      const todoValue = await todoCursor!.readBytes(MAX_READ_BYTES);
      expect(new TextDecoder().decode(todoValue)).toBe('Wash the car');

      const lettersCountedMapCursor = await moment.getCursorByString('letters-counted-map');
      expect(lettersCountedMapCursor).not.toBeNull();
      const lettersCountedMap = await ReadCountedHashMap.create(lettersCountedMapCursor!);
      expect(await lettersCountedMap.count()).toBe(1);

      const lettersSetCursor = await moment.getCursorByString('letters-set');
      expect(lettersSetCursor).not.toBeNull();
      const lettersSet = await ReadHashSet.create(lettersSetCursor!);
      expect(await lettersSet.getCursorByString('a')).not.toBeNull();
      expect(await lettersSet.getCursorByString('c')).toBeNull();

      const lettersCountedSetCursor = await moment.getCursorByString('letters-counted-set');
      expect(lettersCountedSetCursor).not.toBeNull();
      const lettersCountedSet = await ReadCountedHashSet.create(lettersCountedSetCursor!);
      expect(await lettersCountedSet.count()).toBe(1);
    }
  });

  test('low level memory operations', async () => {
    const core = new CoreMemory();
    const hasher = new Hasher('SHA-1');
    const db = await Database.create(core, hasher);

    const map = await WriteHashMap.create(await db.rootCursor());
    const textCursor = await map.putCursorByString('text');

    const writer = await textCursor.writer();
    await writer.write(new TextEncoder().encode('goodbye, world!'));
    writer.seek(9);
    await writer.write(new TextEncoder().encode('cruel world!'));
    await writer.finish();

    const reader = await textCursor.reader();
    const allBytes = new Uint8Array(Number(await textCursor.count()));
    await reader.readFully(allBytes);
    expect(new TextDecoder().decode(allBytes)).toBe('goodbye, cruel world!');
  });
});

describe('Database Low Level API', () => {
  test('low level API with in-memory storage', async () => {
    const core = new CoreMemory();
    const hasher = new Hasher('SHA-1');
    await testLowLevelApi(core, hasher);
  });

  test('low level API with file storage', async () => {
    const tmpDir = await mkdtemp(join(tmpdir(), 'xitdb-'));
    const filePath = join(tmpDir, 'test.db');
    try {
      using core = await CoreFile.create(filePath);
      const hasher = new Hasher('SHA-1');
      await testLowLevelApi(core, hasher);
    } finally {
      await rm(tmpDir, { recursive: true });
    }
  }, 20000);

  test('low level API with buffered file storage', async () => {
    const tmpDir = await mkdtemp(join(tmpdir(), 'xitdb-'));
    const filePath = join(tmpDir, 'test.db');
    try {
      using core = await CoreBufferedFile.create(filePath);
      const hasher = new Hasher('SHA-1');
      await testLowLevelApi(core, hasher);
    } finally {
      await rm(tmpDir, { recursive: true });
    }
  }, 20000);
});

// Helper function for high level API tests
async function testHighLevelApi(core: Core, hasher: Hasher, filePath: string | null): Promise<void> {
  // init the db
  await core.setLength(0);
  let db = await Database.create(core, hasher);

  // First transaction
  {
    const history = await WriteArrayList.create(await db.rootCursor());
    await history.appendContext(await history.getSlot(-1), async (cursor) => {
      const moment = await WriteHashMap.create(cursor);

      await moment.putByString('foo', new Bytes('foo'));
      await moment.putByString('bar', new Bytes('bar'));

      const fruitsCursor = await moment.putCursorByString('fruits');
      const fruits = await WriteArrayList.create(fruitsCursor);
      await fruits.append(new Bytes('apple'));
      await fruits.append(new Bytes('pear'));
      await fruits.append(new Bytes('grape'));

      const peopleCursor = await moment.putCursorByString('people');
      const people = await WriteArrayList.create(peopleCursor);

      const aliceCursor = await people.appendCursor();
      const alice = await WriteHashMap.create(aliceCursor);
      await alice.putByString('name', new Bytes('Alice'));
      await alice.putByString('age', new Uint(25));

      const bobCursor = await people.appendCursor();
      const bob = await WriteHashMap.create(bobCursor);
      await bob.putByString('name', new Bytes('Bob'));
      await bob.putByString('age', new Uint(42));

      const todosCursor = await moment.putCursorByString('todos');
      const todos = await WriteLinkedArrayList.create(todosCursor);
      await todos.append(new Bytes('Pay the bills'));
      await todos.append(new Bytes('Get an oil change'));
      await todos.insert(1, new Bytes('Wash the car'));

      // make sure insertCursor works as well
      const todoCursor = await todos.insertCursor(1);
      await WriteHashMap.create(todoCursor);
      await todos.remove(1);

      const lettersCountedMapCursor = await moment.putCursorByString('letters-counted-map');
      const lettersCountedMap = await WriteCountedHashMap.create(lettersCountedMapCursor);
      await lettersCountedMap.putByString('a', new Uint(1));
      await lettersCountedMap.putByString('a', new Uint(2));
      await lettersCountedMap.putByString('c', new Uint(2));

      const lettersSetCursor = await moment.putCursorByString('letters-set');
      const lettersSet = await WriteHashSet.create(lettersSetCursor);
      await lettersSet.putByString('a');
      await lettersSet.putByString('a');
      await lettersSet.putByString('c');

      const lettersCountedSetCursor = await moment.putCursorByString('letters-counted-set');
      const lettersCountedSet = await WriteCountedHashSet.create(lettersCountedSetCursor);
      await lettersCountedSet.putByString('a');
      await lettersCountedSet.putByString('a');
      await lettersCountedSet.putByString('c');

      // big int with format tag
      const bigIntBytes = new Uint8Array(32);
      bigIntBytes.fill(42); // deterministic bytes
      await moment.putByString('big-number', new Bytes(bigIntBytes, new TextEncoder().encode('bi')));

      // long text using writer
      const longTextCursor = await moment.putCursorByString('long-text');
      const cursorWriter = await longTextCursor.writer();
      for (let i = 0; i < 50; i++) {
        await cursorWriter.write(new TextEncoder().encode('hello, world\n'));
      }
      await cursorWriter.finish();
    });

    // Verify first transaction
    const momentCursor = await history.getCursor(-1);
    const moment = await ReadHashMap.create(momentCursor!);

    const fooCursor = await moment.getCursorByString('foo');
    const fooValue = await fooCursor!.readBytes(MAX_READ_BYTES);
    expect(new TextDecoder().decode(fooValue)).toBe('foo');

    expect((await moment.getSlotByString('foo'))?.tag).toBe(Tag.SHORT_BYTES);
    expect((await moment.getSlotByString('bar'))?.tag).toBe(Tag.SHORT_BYTES);

    const fruitsCursor = await moment.getCursorByString('fruits');
    const fruits = new ReadArrayList(fruitsCursor!);
    expect(await fruits.count()).toBe(3);

    const appleCursor = await fruits.getCursor(0);
    const appleValue = await appleCursor!.readBytes(MAX_READ_BYTES);
    expect(new TextDecoder().decode(appleValue)).toBe('apple');

    const peopleCursor = await moment.getCursorByString('people');
    const people = new ReadArrayList(peopleCursor!);
    expect(await people.count()).toBe(2);

    const aliceCursor = await people.getCursor(0);
    const alice = await ReadHashMap.create(aliceCursor!);
    const aliceAgeCursor = await alice.getCursorByString('age');
    expect(aliceAgeCursor!.readUint()).toBe(25);

    const todosCursor = await moment.getCursorByString('todos');
    const todos = new ReadLinkedArrayList(todosCursor!);
    expect(await todos.count()).toBe(3);

    const todoCursor = await todos.getCursor(0);
    const todoValue = await todoCursor!.readBytes(MAX_READ_BYTES);
    expect(new TextDecoder().decode(todoValue)).toBe('Pay the bills');

    // iterate over people
    const peopleIter = people.iterator();
    await peopleIter.init();
    while (await peopleIter.hasNext()) {
      const personCursor = await peopleIter.next();
      const person = await ReadHashMap.create(personCursor!);
      const personIter = person.iterator();
      await personIter.init();
      while (await personIter.hasNext()) {
        const kvPairCursor = await personIter.next();
        const kvPair = await kvPairCursor!.readKeyValuePair();
        await kvPair.keyCursor.readBytes(MAX_READ_BYTES);

        switch (kvPair.valueCursor.slot().tag) {
          case Tag.SHORT_BYTES:
          case Tag.BYTES:
            await kvPair.valueCursor.readBytes(MAX_READ_BYTES);
            break;
          case Tag.UINT:
            kvPair.valueCursor.readUint();
            break;
          case Tag.INT:
            kvPair.valueCursor.readInt();
            break;
          case Tag.FLOAT:
            kvPair.valueCursor.readFloat();
            break;
        }
      }
    }

    // iterate over fruits
    const fruitsIter = fruits.iterator();
    await fruitsIter.init();
    while (await fruitsIter.hasNext()) {
      await fruitsIter.next();
    }

    // Counted hash map
    {
      const lettersCountedMapCursor = await moment.getCursorByString('letters-counted-map');
      const lettersCountedMap = await ReadCountedHashMap.create(lettersCountedMapCursor!);
      expect(await lettersCountedMap.count()).toBe(2);

      const iter = lettersCountedMap.iterator();
      await iter.init();
      let count = 0;
      while (await iter.hasNext()) {
        const kvPairCursor = await iter.next();
        const kvPair = await kvPairCursor!.readKeyValuePair();
        await kvPair.keyCursor.readBytes(MAX_READ_BYTES);
        count += 1;
      }
      expect(count).toBe(2);
    }

    // Hash set
    {
      const lettersSetCursor = await moment.getCursorByString('letters-set');
      const lettersSet = await ReadHashSet.create(lettersSetCursor!);
      expect(await lettersSet.getCursorByString('a')).not.toBeNull();
      expect(await lettersSet.getCursorByString('c')).not.toBeNull();

      const iter = lettersSet.iterator();
      await iter.init();
      let count = 0;
      while (await iter.hasNext()) {
        const kvPairCursor = await iter.next();
        const kvPair = await kvPairCursor!.readKeyValuePair();
        await kvPair.keyCursor.readBytes(MAX_READ_BYTES);
        count += 1;
      }
      expect(count).toBe(2);
    }

    // Counted hash set
    {
      const lettersCountedSetCursor = await moment.getCursorByString('letters-counted-set');
      const lettersCountedSet = await ReadCountedHashSet.create(lettersCountedSetCursor!);
      expect(await lettersCountedSet.count()).toBe(2);

      const iter = lettersCountedSet.iterator();
      await iter.init();
      let count = 0;
      while (await iter.hasNext()) {
        const kvPairCursor = await iter.next();
        const kvPair = await kvPairCursor!.readKeyValuePair();
        await kvPair.keyCursor.readBytes(MAX_READ_BYTES);
        count += 1;
      }
      expect(count).toBe(2);
    }

    // big number with format tag
    {
      const bigNumberCursor = await moment.getCursorByString('big-number');
      const bigNumber = await bigNumberCursor!.readBytesObject(MAX_READ_BYTES);
      expect(bigNumber.value.length).toBe(32);
      expect(bigNumber.value[0]).toBe(42);
      expect(new TextDecoder().decode(bigNumber.formatTag!)).toBe('bi');
    }

    // long text
    {
      const longTextCursor = await moment.getCursorByString('long-text');
      const cursorReader = await longTextCursor!.reader();
      const content = new Uint8Array(Number(await longTextCursor!.count()));
      await cursorReader.readFully(content);
      const lines = new TextDecoder().decode(content).split('\n').filter(l => l.length > 0);
      expect(lines.length).toBe(50);
    }
  }

  // Second transaction - modify data
  {
    const history = await WriteArrayList.create(await db.rootCursor());
    await history.appendContext(await history.getSlot(-1), async (cursor) => {
      const moment = await WriteHashMap.create(cursor);

      expect(await moment.removeByString('bar')).toBe(true);
      expect(await moment.removeByString("doesn't exist")).toBe(false);

      const fruitsCursor = await moment.putCursorByString('fruits');
      const fruits = await WriteArrayList.create(fruitsCursor);
      await fruits.put(0, new Bytes('lemon'));
      await fruits.slice(2);

      const peopleCursor = await moment.putCursorByString('people');
      const people = await WriteArrayList.create(peopleCursor);
      const aliceCursor = await people.putCursor(0);
      const alice = await WriteHashMap.create(aliceCursor);
      await alice.putByString('age', new Uint(26));

      const todosCursor = await moment.putCursorByString('todos');
      const todos = await WriteLinkedArrayList.create(todosCursor);
      await todos.concat(todosCursor.slot());
      await todos.slice(1, 2);
      await todos.remove(1);

      const lettersCountedMapCursor = await moment.putCursorByString('letters-counted-map');
      const lettersCountedMap = await WriteCountedHashMap.create(lettersCountedMapCursor);
      await lettersCountedMap.removeByString('b');
      await lettersCountedMap.removeByString('c');

      const lettersSetCursor = await moment.putCursorByString('letters-set');
      const lettersSet = await WriteHashSet.create(lettersSetCursor);
      await lettersSet.removeByString('b');
      await lettersSet.removeByString('c');

      const lettersCountedSetCursor = await moment.putCursorByString('letters-counted-set');
      const lettersCountedSet = await WriteCountedHashSet.create(lettersCountedSetCursor);
      await lettersCountedSet.removeByString('b');
      await lettersCountedSet.removeByString('c');
    });

    // Verify second transaction
    const momentCursor = await history.getCursor(-1);
    const moment = await ReadHashMap.create(momentCursor!);

    expect(await moment.getCursorByString('bar')).toBeNull();

    const fruitsKeyCursor = await moment.getKeyCursorByString('fruits');
    const fruitsKeyValue = await fruitsKeyCursor!.readBytes(MAX_READ_BYTES);
    expect(new TextDecoder().decode(fruitsKeyValue)).toBe('fruits');

    const fruitsCursor = await moment.getCursorByString('fruits');
    const fruits = new ReadArrayList(fruitsCursor!);
    expect(await fruits.count()).toBe(2);

    const fruitsKVCursor = await moment.getKeyValuePairByString('fruits');
    expect(fruitsKVCursor!.keyCursor.slotPtr.slot.tag).toBe(Tag.SHORT_BYTES);
    expect(fruitsKVCursor!.valueCursor.slotPtr.slot.tag).toBe(Tag.ARRAY_LIST);

    const lemonCursor = await fruits.getCursor(0);
    const lemonValue = await lemonCursor!.readBytes(MAX_READ_BYTES);
    expect(new TextDecoder().decode(lemonValue)).toBe('lemon');

    const peopleCursor = await moment.getCursorByString('people');
    const people = new ReadArrayList(peopleCursor!);
    expect(await people.count()).toBe(2);

    const aliceCursor = await people.getCursor(0);
    const alice = await ReadHashMap.create(aliceCursor!);
    const aliceAgeCursor = await alice.getCursorByString('age');
    expect(aliceAgeCursor!.readUint()).toBe(26);

    const todosCursor = await moment.getCursorByString('todos');
    const todos = new ReadLinkedArrayList(todosCursor!);
    expect(await todos.count()).toBe(1);

    const todoCursor = await todos.getCursor(0);
    const todoValue = await todoCursor!.readBytes(MAX_READ_BYTES);
    expect(new TextDecoder().decode(todoValue)).toBe('Wash the car');

    const lettersCountedMapCursor = await moment.getCursorByString('letters-counted-map');
    const lettersCountedMap = await ReadCountedHashMap.create(lettersCountedMapCursor!);
    expect(await lettersCountedMap.count()).toBe(1);

    const lettersSetCursor = await moment.getCursorByString('letters-set');
    const lettersSet = await ReadHashSet.create(lettersSetCursor!);
    expect(await lettersSet.getCursorByString('a')).not.toBeNull();
    expect(await lettersSet.getCursorByString('c')).toBeNull();

    const lettersCountedSetCursor = await moment.getCursorByString('letters-counted-set');
    const lettersCountedSet = await ReadCountedHashSet.create(lettersCountedSetCursor!);
    expect(await lettersCountedSet.count()).toBe(1);
  }

  // The old data hasn't changed
  {
    const history = await WriteArrayList.create(await db.rootCursor());
    const momentCursor = await history.getCursor(0);
    const moment = await ReadHashMap.create(momentCursor!);

    const fooCursor = await moment.getCursorByString('foo');
    const fooValue = await fooCursor!.readBytes(MAX_READ_BYTES);
    expect(new TextDecoder().decode(fooValue)).toBe('foo');

    expect((await moment.getSlotByString('foo'))?.tag).toBe(Tag.SHORT_BYTES);
    expect((await moment.getSlotByString('bar'))?.tag).toBe(Tag.SHORT_BYTES);

    const fruitsCursor = await moment.getCursorByString('fruits');
    const fruits = new ReadArrayList(fruitsCursor!);
    expect(await fruits.count()).toBe(3);

    const appleCursor = await fruits.getCursor(0);
    const appleValue = await appleCursor!.readBytes(MAX_READ_BYTES);
    expect(new TextDecoder().decode(appleValue)).toBe('apple');

    const peopleCursor = await moment.getCursorByString('people');
    const people = new ReadArrayList(peopleCursor!);
    expect(await people.count()).toBe(2);

    const aliceCursor = await people.getCursor(0);
    const alice = await ReadHashMap.create(aliceCursor!);
    const aliceAgeCursor = await alice.getCursorByString('age');
    expect(aliceAgeCursor!.readUint()).toBe(25);

    const todosCursor = await moment.getCursorByString('todos');
    const todos = new ReadLinkedArrayList(todosCursor!);
    expect(await todos.count()).toBe(3);

    const todoCursor = await todos.getCursor(0);
    const todoValue = await todoCursor!.readBytes(MAX_READ_BYTES);
    expect(new TextDecoder().decode(todoValue)).toBe('Pay the bills');
  }

  // Remove the last transaction with slice
  {
    const history = await WriteArrayList.create(await db.rootCursor());
    await history.slice(1);

    const momentCursor = await history.getCursor(-1);
    const moment = await ReadHashMap.create(momentCursor!);

    const fooCursor = await moment.getCursorByString('foo');
    const fooValue = await fooCursor!.readBytes(MAX_READ_BYTES);
    expect(new TextDecoder().decode(fooValue)).toBe('foo');

    expect((await moment.getSlotByString('foo'))?.tag).toBe(Tag.SHORT_BYTES);
    expect((await moment.getSlotByString('bar'))?.tag).toBe(Tag.SHORT_BYTES);

    const fruitsCursor = await moment.getCursorByString('fruits');
    const fruits = new ReadArrayList(fruitsCursor!);
    expect(await fruits.count()).toBe(3);

    const appleCursor = await fruits.getCursor(0);
    const appleValue = await appleCursor!.readBytes(MAX_READ_BYTES);
    expect(new TextDecoder().decode(appleValue)).toBe('apple');

    const peopleCursor = await moment.getCursorByString('people');
    const people = new ReadArrayList(peopleCursor!);
    expect(await people.count()).toBe(2);

    const aliceCursor = await people.getCursor(0);
    const alice = await ReadHashMap.create(aliceCursor!);
    const aliceAgeCursor = await alice.getCursorByString('age');
    expect(aliceAgeCursor!.readUint()).toBe(25);

    const todosCursor = await moment.getCursorByString('todos');
    const todos = new ReadLinkedArrayList(todosCursor!);
    expect(await todos.count()).toBe(3);

    const todoCursor = await todos.getCursor(0);
    const todoValue = await todoCursor!.readBytes(MAX_READ_BYTES);
    expect(new TextDecoder().decode(todoValue)).toBe('Pay the bills');
  }

  // The db size remains the same after writing junk data and then reinitializing the db
  {
    await core.seek(await core.length());
    const sizeBefore = await core.length();

    const writer = core.writer();
    await writer.write(new TextEncoder().encode('this is junk data that will be deleted during init'));

    db = await Database.create(core, hasher);

    const sizeAfter = await core.length();
    expect(sizeBefore).toBe(sizeAfter);
  }

  // Cloning
  {
    const history = await WriteArrayList.create(await db.rootCursor());
    await history.appendContext(await history.getSlot(-1), async (cursor) => {
      const moment = await WriteHashMap.create(cursor);

      const fruitsCursor = await moment.getCursorByString('fruits');
      const fruits = new ReadArrayList(fruitsCursor!);

      // create a new key called "food" whose initial value is based on the "fruits" list
      const foodCursor = await moment.putCursorByString('food');
      await foodCursor.write(fruits.slot());

      const food = await WriteArrayList.create(foodCursor);
      await food.append(new Bytes('eggs'));
      await food.append(new Bytes('rice'));
      await food.append(new Bytes('fish'));
    });

    const momentCursor = await history.getCursor(-1);
    const moment = await ReadHashMap.create(momentCursor!);

    // the food list includes the fruits
    const foodCursor = await moment.getCursorByString('food');
    const food = new ReadArrayList(foodCursor!);
    expect(await food.count()).toBe(6);

    // ...but the fruits list hasn't been changed
    const fruitsCursor = await moment.getCursorByString('fruits');
    const fruits = new ReadArrayList(fruitsCursor!);
    expect(await fruits.count()).toBe(3);
  }

  // Accidental mutation when cloning inside a transaction
  {
    const history = await WriteArrayList.create(await db.rootCursor());
    const historyIndex = (await history.count()) - 1;

    await history.appendContext(await history.getSlot(-1), async (cursor) => {
      const moment = await WriteHashMap.create(cursor);

      const bigCitiesCursor = await moment.putCursorByString('big-cities');
      const bigCities = await WriteArrayList.create(bigCitiesCursor);
      await bigCities.append(new Bytes('New York, NY'));
      await bigCities.append(new Bytes('Los Angeles, CA'));

      // create a new key called "cities" whose initial value is based on the "big-cities" list
      const citiesCursor = await moment.putCursorByString('cities');
      await citiesCursor.write(bigCities.slot());

      const cities = await WriteArrayList.create(citiesCursor);
      await cities.append(new Bytes('Charleston, SC'));
      await cities.append(new Bytes('Louisville, KY'));
    });

    const momentCursor = await history.getCursor(-1);
    const moment = await ReadHashMap.create(momentCursor!);

    // the cities list contains all four
    const citiesCursor = await moment.getCursorByString('cities');
    const cities = new ReadArrayList(citiesCursor!);
    expect(await cities.count()).toBe(4);

    // ..but so does big-cities! we did not intend to mutate this
    const bigCitiesCursor = await moment.getCursorByString('big-cities');
    const bigCities = new ReadArrayList(bigCitiesCursor!);
    expect(await bigCities.count()).toBe(4);

    // revert that change
    await history.append((await history.getSlot(historyIndex))!);
  }

  // Preventing accidental mutation with freezing
  {
    const history = await WriteArrayList.create(await db.rootCursor());
    await history.appendContext(await history.getSlot(-1), async (cursor) => {
      const moment = await WriteHashMap.create(cursor);

      const bigCitiesCursor = await moment.putCursorByString('big-cities');
      const bigCities = await WriteArrayList.create(bigCitiesCursor);
      await bigCities.append(new Bytes('New York, NY'));
      await bigCities.append(new Bytes('Los Angeles, CA'));

      // freeze here, so big-cities won't be mutated
      cursor.db.freeze();

      // create a new key called "cities" whose initial value is based on the "big-cities" list
      const citiesCursor = await moment.putCursorByString('cities');
      await citiesCursor.write(bigCities.slot());

      const cities = await WriteArrayList.create(citiesCursor);
      await cities.append(new Bytes('Charleston, SC'));
      await cities.append(new Bytes('Louisville, KY'));
    });

    const momentCursor = await history.getCursor(-1);
    const moment = await ReadHashMap.create(momentCursor!);

    // the cities list contains all four
    const citiesCursor = await moment.getCursorByString('cities');
    const cities = new ReadArrayList(citiesCursor!);
    expect(await cities.count()).toBe(4);

    // and big-cities only contains the original two
    const bigCitiesCursor = await moment.getCursorByString('big-cities');
    const bigCities = new ReadArrayList(bigCitiesCursor!);
    expect(await bigCities.count()).toBe(2);
  }
}

// Helper function for low level API tests
async function testLowLevelApi(core: Core, hasher: Hasher): Promise<void> {
  // open and re-open database
  {
    // make empty database
    await core.setLength(0);
    await Database.create(core, hasher);

    // re-open without error
    let db = await Database.create(core, hasher);
    const writer = db.core.writer();
    await db.core.seek(0);
    await writer.writeByte('g'.charCodeAt(0));

    // re-open with error
    await expect(Database.create(core, hasher)).rejects.toThrow(InvalidDatabaseException);

    // modify the version
    await db.core.seek(0);
    await writer.writeByte('x'.charCodeAt(0));
    await db.core.seek(4);
    await writer.writeShort(VERSION + 1);

    // re-open with error
    await expect(Database.create(core, hasher)).rejects.toThrow(InvalidVersionException);
  }

  // save hash id in header
  {
    const hashId = Hasher.stringToId('sha1');
    const hasherWithHashId = new Hasher('SHA-1', hashId);

    // make empty database
    await core.setLength(0);
    const db = await Database.create(core, hasherWithHashId);

    // verify hash id was stored
    expect(db.hasher.id).toBe(hashId);
    expect(Hasher.idToString(db.hasher.id)).toBe('sha1');
  }

  // array_list of hash_maps
  {
    await core.setLength(0);
    const db = await Database.create(core, hasher);
    const rootCursor = await db.rootCursor();

    // write foo -> bar with a writer
    const fooKey = await db.hasher.digest(new TextEncoder().encode('foo'));
    await rootCursor.writePath([
      new ArrayListInit(),
      new ArrayListAppend(),
      new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
      new HashMapInit(false, false),
      new HashMapGet(new HashMapGetValue(fooKey)),
      new Context(async (cursor) => {
        expect(cursor.slot().tag).toBe(Tag.NONE);
        const writer = await cursor.writer();
        await writer.write(new TextEncoder().encode('bar'));
        await writer.finish();
      }),
    ]);

    // read foo
    {
      const barCursor = await rootCursor.readPath([
        new ArrayListGet(-1),
        new HashMapGet(new HashMapGetValue(fooKey)),
      ]);
      expect(await barCursor!.count()).toBe(3);
      const barValue = await barCursor!.readBytes(MAX_READ_BYTES);
      expect(new TextDecoder().decode(barValue)).toBe('bar');
    }

    // read foo from ctx
    await rootCursor.writePath([
      new ArrayListInit(),
      new ArrayListAppend(),
      new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
      new HashMapInit(false, false),
      new HashMapGet(new HashMapGetValue(fooKey)),
      new Context(async (cursor) => {
        expect(cursor.slot().tag).not.toBe(Tag.NONE);

        const value = await cursor.readBytes(MAX_READ_BYTES);
        expect(new TextDecoder().decode(value)).toBe('bar');

        const barReader = await cursor.reader();

        // read into buffer
        const barBytes = new Uint8Array(10);
        const barSize = await barReader.read(barBytes);
        expect(new TextDecoder().decode(barBytes.slice(0, barSize))).toBe('bar');
        barReader.seek(0);
        expect(await barReader.read(barBytes)).toBe(3);
        expect(new TextDecoder().decode(barBytes.slice(0, 3))).toBe('bar');

        // read one char at a time
        {
          const ch = new Uint8Array(1);
          barReader.seek(0);

          await barReader.readFully(ch);
          expect(new TextDecoder().decode(ch)).toBe('b');

          await barReader.readFully(ch);
          expect(new TextDecoder().decode(ch)).toBe('a');

          await barReader.readFully(ch);
          expect(new TextDecoder().decode(ch)).toBe('r');

          await expect(barReader.readFully(ch)).rejects.toThrow(EndOfStreamException);

          barReader.seek(1);
          expect(String.fromCharCode(await barReader.readByte())).toBe('a');

          barReader.seek(0);
          expect(String.fromCharCode(await barReader.readByte())).toBe('b');
        }
      }),
    ]);

    // overwrite foo -> baz
    await rootCursor.writePath([
      new ArrayListInit(),
      new ArrayListAppend(),
      new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
      new HashMapInit(false, false),
      new HashMapGet(new HashMapGetValue(fooKey)),
      new Context(async (cursor) => {
        expect(cursor.slot().tag).not.toBe(Tag.NONE);

        const writer = await cursor.writer();
        await writer.write(new TextEncoder().encode('x'));
        await writer.write(new TextEncoder().encode('x'));
        await writer.write(new TextEncoder().encode('x'));
        writer.seek(0);
        await writer.write(new TextEncoder().encode('b'));
        writer.seek(2);
        await writer.write(new TextEncoder().encode('z'));
        writer.seek(1);
        await writer.write(new TextEncoder().encode('a'));
        await writer.finish();

        const value = await cursor.readBytes(MAX_READ_BYTES);
        expect(new TextDecoder().decode(value)).toBe('baz');
      }),
    ]);

    // if error in ctx, db doesn't change
    {
      const sizeBefore = await core.length();

      try {
        await rootCursor.writePath([
          new ArrayListInit(),
          new ArrayListAppend(),
          new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
          new HashMapInit(false, false),
          new HashMapGet(new HashMapGetValue(fooKey)),
          new Context(async (cursor) => {
            const writer = await cursor.writer();
            await writer.write(new TextEncoder().encode("this value won't be visible"));
            await writer.finish();
            throw new Error();
          }),
        ]);
      } catch (e) {}

      // read foo
      const valueCursor = await rootCursor.readPath([
        new ArrayListGet(-1),
        new HashMapGet(new HashMapGetValue(fooKey)),
      ]);
      const value = await valueCursor!.readBytes();
      expect(new TextDecoder().decode(value)).toBe('baz');

      // verify that the db is properly truncated back to its original size after error
      const sizeAfter = await core.length();
      expect(sizeBefore).toBe(sizeAfter);
    }

    // write bar -> longstring
    const barKey = await db.hasher.digest(new TextEncoder().encode('bar'));
    {
      const barCursor = await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new HashMapInit(false, false),
        new HashMapGet(new HashMapGetValue(barKey)),
      ]);
      await barCursor.write(new Bytes('longstring'));

      // the slot tag is BYTES because the byte array is > 8 bytes long
      expect(barCursor.slot().tag).toBe(Tag.BYTES);

      // writing again returns the same slot
      {
        const nextBarCursor = await rootCursor.writePath([
          new ArrayListInit(),
          new ArrayListAppend(),
          new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
          new HashMapInit(false, false),
          new HashMapGet(new HashMapGetValue(barKey)),
        ]);
        await nextBarCursor.writeIfEmpty(new Bytes('longstring'));
        expect(barCursor.slot().value).toBe(nextBarCursor.slot().value);
      }

      // writing with write returns a new slot
      {
        const nextBarCursor = await rootCursor.writePath([
          new ArrayListInit(),
          new ArrayListAppend(),
          new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
          new HashMapInit(false, false),
          new HashMapGet(new HashMapGetValue(barKey)),
        ]);
        await nextBarCursor.write(new Bytes('longstring'));
        expect(barCursor.slot().value).not.toBe(nextBarCursor.slot().value);
      }
    }

    // read bar
    {
      const readBarCursor = await rootCursor.readPath([
        new ArrayListGet(-1),
        new HashMapGet(new HashMapGetValue(barKey)),
      ]);
      const barValue = await readBarCursor!.readBytes(MAX_READ_BYTES);
      expect(new TextDecoder().decode(barValue)).toBe('longstring');
    }

    // write bar -> shortstr
    {
      const barCursor = await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new HashMapInit(false, false),
        new HashMapGet(new HashMapGetValue(barKey)),
      ]);
      await barCursor.write(new Bytes('shortstr'));

      // the slot tag is SHORT_BYTES because the byte array is <= 8 bytes long
      expect(barCursor.slot().tag).toBe(Tag.SHORT_BYTES);
      expect(await barCursor.count()).toBe(8);

      // make sure that SHORT_BYTES can be read with a reader
      const barReader = await barCursor.reader();
      const barValue = new Uint8Array(Number(await barCursor.count()));
      await barReader.readFully(barValue);
      expect(new TextDecoder().decode(barValue)).toBe('shortstr');
    }

    // write bytes with a format tag - shortstr
    {
      const barCursor = await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new HashMapInit(false, false),
        new HashMapGet(new HashMapGetValue(barKey)),
      ]);
      await barCursor.write(new Bytes('shortstr', new TextEncoder().encode('st')));

      // the slot tag is BYTES because the byte array is > 8 bytes long including the format tag
      expect(barCursor.slot().tag).toBe(Tag.BYTES);
      expect(await barCursor.count()).toBe(8);

      // read bar
      const readBarCursor = await rootCursor.readPath([
        new ArrayListGet(-1),
        new HashMapGet(new HashMapGetValue(barKey)),
      ]);
      const barBytes = await readBarCursor!.readBytesObject(MAX_READ_BYTES);
      expect(new TextDecoder().decode(barBytes.value)).toBe('shortstr');
      expect(new TextDecoder().decode(barBytes.formatTag!)).toBe('st');

      // make sure that BYTES can be read with a reader
      const barReader = await barCursor.reader();
      const barValue = new Uint8Array(Number(await barCursor.count()));
      await barReader.readFully(barValue);
      expect(new TextDecoder().decode(barValue)).toBe('shortstr');
    }

    // write bytes with a format tag - shorts
    {
      const barCursor = await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new HashMapInit(false, false),
        new HashMapGet(new HashMapGetValue(barKey)),
      ]);
      await barCursor.write(new Bytes('shorts', new TextEncoder().encode('st')));

      // the slot tag is SHORT_BYTES because the byte array is <= 8 bytes long including the format tag
      expect(barCursor.slot().tag).toBe(Tag.SHORT_BYTES);
      expect(await barCursor.count()).toBe(6);

      // read bar
      const readBarCursor = await rootCursor.readPath([
        new ArrayListGet(-1),
        new HashMapGet(new HashMapGetValue(barKey)),
      ]);
      const barBytes = await readBarCursor!.readBytesObject(MAX_READ_BYTES);
      expect(new TextDecoder().decode(barBytes.value)).toBe('shorts');
      expect(new TextDecoder().decode(barBytes.formatTag!)).toBe('st');

      // make sure that SHORT_BYTES can be read with a reader
      const barReader = await barCursor.reader();
      const barValue = new Uint8Array(Number(await barCursor.count()));
      await barReader.readFully(barValue);
      expect(new TextDecoder().decode(barValue)).toBe('shorts');
    }

    // write bytes with a format tag - short
    {
      const barCursor = await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new HashMapInit(false, false),
        new HashMapGet(new HashMapGetValue(barKey)),
      ]);
      await barCursor.write(new Bytes('short', new TextEncoder().encode('st')));

      // the slot tag is SHORT_BYTES because the byte array is <= 8 bytes long including the format tag
      expect(barCursor.slot().tag).toBe(Tag.SHORT_BYTES);
      expect(await barCursor.count()).toBe(5);

      // read bar
      const readBarCursor = await rootCursor.readPath([
        new ArrayListGet(-1),
        new HashMapGet(new HashMapGetValue(barKey)),
      ]);
      const barBytes = await readBarCursor!.readBytesObject(MAX_READ_BYTES);
      expect(new TextDecoder().decode(barBytes.value)).toBe('short');
      expect(new TextDecoder().decode(barBytes.formatTag!)).toBe('st');

      // make sure that SHORT_BYTES can be read with a reader
      const barReader = await barCursor.reader();
      const barValue = new Uint8Array(Number(await barCursor.count()));
      await barReader.readFully(barValue);
      expect(new TextDecoder().decode(barValue)).toBe('short');
    }

    // read foo into buffer
    {
      const barCursor = await rootCursor.readPath([
        new ArrayListGet(-1),
        new HashMapGet(new HashMapGetValue(fooKey)),
      ]);
      const barBufferValue = await barCursor!.readBytes(MAX_READ_BYTES);
      expect(new TextDecoder().decode(barBufferValue)).toBe('baz');
    }

    // write bar and get a pointer to it
    const barSlot = (
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new HashMapInit(false, false),
        new HashMapGet(new HashMapGetValue(barKey)),
        new WriteData(new Bytes('bar')),
      ])
    ).slot();

    // overwrite foo -> bar using the bar pointer
    await rootCursor.writePath([
      new ArrayListInit(),
      new ArrayListAppend(),
      new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
      new HashMapInit(false, false),
      new HashMapGet(new HashMapGetValue(fooKey)),
      new WriteData(barSlot),
    ]);
    const barCursor = await rootCursor.readPath([
      new ArrayListGet(-1),
      new HashMapGet(new HashMapGetValue(fooKey)),
    ]);
    const barValue = await barCursor!.readBytes(MAX_READ_BYTES);
    expect(new TextDecoder().decode(barValue)).toBe('bar');

    // can still read the old value
    const bazCursor = await rootCursor.readPath([
      new ArrayListGet(-2),
      new HashMapGet(new HashMapGetValue(fooKey)),
    ]);
    const bazValue = await bazCursor!.readBytes(MAX_READ_BYTES);
    expect(new TextDecoder().decode(bazValue)).toBe('baz');

    // key not found
    const notFoundKey = await db.hasher.digest(new TextEncoder().encode("this doesn't exist"));
    expect(
      await rootCursor.readPath([new ArrayListGet(-2), new HashMapGet(new HashMapGetValue(notFoundKey))])
    ).toBeNull();

    // write key that conflicts with foo the first two bytes
    const smallConflictKey = await db.hasher.digest(new TextEncoder().encode('small conflict'));
    smallConflictKey[smallConflictKey.length - 1] = fooKey[fooKey.length - 1];
    smallConflictKey[smallConflictKey.length - 2] = fooKey[fooKey.length - 2];
    await rootCursor.writePath([
      new ArrayListInit(),
      new ArrayListAppend(),
      new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
      new HashMapInit(false, false),
      new HashMapGet(new HashMapGetValue(smallConflictKey)),
      new WriteData(new Bytes('small')),
    ]);

    // write key that conflicts with foo the first four bytes
    const conflictKey = await db.hasher.digest(new TextEncoder().encode('conflict'));
    conflictKey[conflictKey.length - 1] = fooKey[fooKey.length - 1];
    conflictKey[conflictKey.length - 2] = fooKey[fooKey.length - 2];
    conflictKey[conflictKey.length - 3] = fooKey[fooKey.length - 3];
    conflictKey[conflictKey.length - 4] = fooKey[fooKey.length - 4];
    await rootCursor.writePath([
      new ArrayListInit(),
      new ArrayListAppend(),
      new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
      new HashMapInit(false, false),
      new HashMapGet(new HashMapGetValue(conflictKey)),
      new WriteData(new Bytes('hello')),
    ]);

    // read conflicting key
    const helloCursor = await rootCursor.readPath([
      new ArrayListGet(-1),
      new HashMapGet(new HashMapGetValue(conflictKey)),
    ]);
    const helloValue = await helloCursor!.readBytes(MAX_READ_BYTES);
    expect(new TextDecoder().decode(helloValue)).toBe('hello');

    // we can still read foo
    const barCursor2 = await rootCursor.readPath([
      new ArrayListGet(-1),
      new HashMapGet(new HashMapGetValue(fooKey)),
    ]);
    const barValue2 = await barCursor2!.readBytes(MAX_READ_BYTES);
    expect(new TextDecoder().decode(barValue2)).toBe('bar');

    // overwrite conflicting key
    await rootCursor.writePath([
      new ArrayListInit(),
      new ArrayListAppend(),
      new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
      new HashMapInit(false, false),
      new HashMapGet(new HashMapGetValue(conflictKey)),
      new WriteData(new Bytes('goodbye')),
    ]);
    const goodbyeCursor = await rootCursor.readPath([
      new ArrayListGet(-1),
      new HashMapGet(new HashMapGetValue(conflictKey)),
    ]);
    const goodbyeValue = await goodbyeCursor!.readBytes(MAX_READ_BYTES);
    expect(new TextDecoder().decode(goodbyeValue)).toBe('goodbye');

    // we can still read the old conflicting key
    const helloCursor2 = await rootCursor.readPath([
      new ArrayListGet(-2),
      new HashMapGet(new HashMapGetValue(conflictKey)),
    ]);
    const helloValue2 = await helloCursor2!.readBytes(MAX_READ_BYTES);
    expect(new TextDecoder().decode(helloValue2)).toBe('hello');

    // remove the conflicting keys
    {
      // foo's slot is an INDEX slot due to the conflict
      {
        const mapCursor = await rootCursor.readPath([new ArrayListGet(-1)]);
        expect(mapCursor!.slot().tag).toBe(Tag.HASH_MAP);

        const i = Number(BigInt.asUintN(64, bytesToBigInt(fooKey)) & MASK);
        const slotPos = Number(mapCursor!.slot().value) + Slot.LENGTH * i;
        await core.seek(slotPos);
        const reader = core.reader();
        const slotBytes = new Uint8Array(Slot.LENGTH);
        await reader.readFully(slotBytes);
        const slot = Slot.fromBytes(slotBytes);

        expect(slot.tag).toBe(Tag.INDEX);
      }

      // remove the small conflict key
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new HashMapInit(false, false),
        new HashMapRemove(smallConflictKey),
      ]);

      // the conflict key still exists in history
      expect(
        await rootCursor.readPath([new ArrayListGet(-2), new HashMapGet(new HashMapGetValue(smallConflictKey))])
      ).not.toBeNull();

      // the conflict key doesn't exist in the latest moment
      expect(
        await rootCursor.readPath([new ArrayListGet(-1), new HashMapGet(new HashMapGetValue(smallConflictKey))])
      ).toBeNull();

      // the other conflict key still exists
      expect(
        await rootCursor.readPath([new ArrayListGet(-1), new HashMapGet(new HashMapGetValue(conflictKey))])
      ).not.toBeNull();

      // foo's slot is still an INDEX slot due to the other conflicting key
      {
        const mapCursor = await rootCursor.readPath([new ArrayListGet(-1)]);
        expect(mapCursor!.slot().tag).toBe(Tag.HASH_MAP);

        const i = Number(BigInt.asUintN(64, bytesToBigInt(fooKey)) & MASK);
        const slotPos = Number(mapCursor!.slot().value) + Slot.LENGTH * i;
        await core.seek(slotPos);
        const reader = core.reader();
        const slotBytes = new Uint8Array(Slot.LENGTH);
        await reader.readFully(slotBytes);
        const slot = Slot.fromBytes(slotBytes);

        expect(slot.tag).toBe(Tag.INDEX);
      }

      // remove the conflict key
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new HashMapInit(false, false),
        new HashMapRemove(conflictKey),
      ]);

      // the conflict keys don't exist in the latest moment
      expect(
        await rootCursor.readPath([new ArrayListGet(-1), new HashMapGet(new HashMapGetValue(smallConflictKey))])
      ).toBeNull();
      expect(
        await rootCursor.readPath([new ArrayListGet(-1), new HashMapGet(new HashMapGetValue(conflictKey))])
      ).toBeNull();

      // foo's slot is now a KV_PAIR slot, because the branch was shortened
      {
        const mapCursor = await rootCursor.readPath([new ArrayListGet(-1)]);
        expect(mapCursor!.slot().tag).toBe(Tag.HASH_MAP);

        const i = Number(BigInt.asUintN(64, bytesToBigInt(fooKey)) & MASK);
        const slotPos = Number(mapCursor!.slot().value) + Slot.LENGTH * i;
        await core.seek(slotPos);
        const reader = core.reader();
        const slotBytes = new Uint8Array(Slot.LENGTH);
        await reader.readFully(slotBytes);
        const slot = Slot.fromBytes(slotBytes);

        expect(slot.tag).toBe(Tag.KV_PAIR);
      }
    }

    // overwrite foo with uint, int, float
    {
      // overwrite foo with a uint
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new HashMapInit(false, false),
        new HashMapGet(new HashMapGetValue(fooKey)),
        new WriteData(new Uint(42)),
      ]);

      // read foo
      const uintValue = (
        await rootCursor.readPath([new ArrayListGet(-1), new HashMapGet(new HashMapGetValue(fooKey))])
      )!.readUint();
      expect(uintValue).toBe(42);
    }

    {
      // overwrite foo with an int
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new HashMapInit(false, false),
        new HashMapGet(new HashMapGetValue(fooKey)),
        new WriteData(new Int(-42)),
      ]);

      // read foo
      const intValue = (
        await rootCursor.readPath([new ArrayListGet(-1), new HashMapGet(new HashMapGetValue(fooKey))])
      )!.readInt();
      expect(intValue).toBe(-42);
    }

    {
      // overwrite foo with a float
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new HashMapInit(false, false),
        new HashMapGet(new HashMapGetValue(fooKey)),
        new WriteData(new Float(42.5)),
      ]);

      // read foo
      const floatValue = (
        await rootCursor.readPath([new ArrayListGet(-1), new HashMapGet(new HashMapGetValue(fooKey))])
      )!.readFloat();
      expect(floatValue).toBe(42.5);
    }

    // remove foo
    await rootCursor.writePath([
      new ArrayListInit(),
      new ArrayListAppend(),
      new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
      new HashMapInit(false, false),
      new HashMapRemove(fooKey),
    ]);

    // remove key that does not exist
    await expect(
      rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new HashMapInit(false, false),
        new HashMapRemove(await db.hasher.digest(new TextEncoder().encode("doesn't exist"))),
      ])
    ).rejects.toThrow(KeyNotFoundException);

    // make sure foo doesn't exist anymore
    expect(
      await rootCursor.readPath([new ArrayListGet(-1), new HashMapGet(new HashMapGetValue(fooKey))])
    ).toBeNull();

    // non-top-level list
    {
      const fruitsKey = await db.hasher.digest(new TextEncoder().encode('fruits'));

      // write apple
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new HashMapInit(false, false),
        new HashMapGet(new HashMapGetValue(fruitsKey)),
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(new Bytes('apple')),
      ]);

      // read apple
      const appleCursor = await rootCursor.readPath([
        new ArrayListGet(-1),
        new HashMapGet(new HashMapGetValue(fruitsKey)),
        new ArrayListGet(-1),
      ]);
      const appleValue = await appleCursor!.readBytes(MAX_READ_BYTES);
      expect(new TextDecoder().decode(appleValue)).toBe('apple');

      // write banana
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new HashMapInit(false, false),
        new HashMapGet(new HashMapGetValue(fruitsKey)),
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(new Bytes('banana')),
      ]);

      // read banana
      const bananaCursor = await rootCursor.readPath([
        new ArrayListGet(-1),
        new HashMapGet(new HashMapGetValue(fruitsKey)),
        new ArrayListGet(-1),
      ]);
      const bananaValue = await bananaCursor!.readBytes(MAX_READ_BYTES);
      expect(new TextDecoder().decode(bananaValue)).toBe('banana');

      // can't read banana in older array_list
      expect(
        await rootCursor.readPath([
          new ArrayListGet(-2),
          new HashMapGet(new HashMapGetValue(fruitsKey)),
          new ArrayListGet(1),
        ])
      ).toBeNull();

      // write pear
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new HashMapInit(false, false),
        new HashMapGet(new HashMapGetValue(fruitsKey)),
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(new Bytes('pear')),
      ]);

      // write grape
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new HashMapInit(false, false),
        new HashMapGet(new HashMapGetValue(fruitsKey)),
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(new Bytes('grape')),
      ]);

      // read pear
      const pearCursor = await rootCursor.readPath([
        new ArrayListGet(-1),
        new HashMapGet(new HashMapGetValue(fruitsKey)),
        new ArrayListGet(-2),
      ]);
      const pearValue = await pearCursor!.readBytes(MAX_READ_BYTES);
      expect(new TextDecoder().decode(pearValue)).toBe('pear');

      // read grape
      const grapeCursor = await rootCursor.readPath([
        new ArrayListGet(-1),
        new HashMapGet(new HashMapGetValue(fruitsKey)),
        new ArrayListGet(-1),
      ]);
      const grapeValue = await grapeCursor!.readBytes(MAX_READ_BYTES);
      expect(new TextDecoder().decode(grapeValue)).toBe('grape');
    }
  }

  // append to top-level array_list many times, filling up the array_list until a root overflow occurs
  {
    await core.setLength(0);
    const db = await Database.create(core, hasher);
    const rootCursor = await db.rootCursor();

    const watKey = await db.hasher.digest(new TextEncoder().encode('wat'));

    for (let i = 0; i < SLOT_COUNT + 1; i++) {
      const value = `wat${i}`;
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new HashMapInit(false, false),
        new HashMapGet(new HashMapGetValue(watKey)),
        new WriteData(new Bytes(value)),
      ]);
    }

    // verify all values
    for (let i = 0; i < SLOT_COUNT + 1; i++) {
      const value = `wat${i}`;
      const cursor = await rootCursor.readPath([
        new ArrayListGet(i),
        new HashMapGet(new HashMapGetValue(watKey)),
      ]);
      const value2 = new TextDecoder().decode(await cursor!.readBytes(MAX_READ_BYTES));
      expect(value).toBe(value2);
    }

    // add more slots to cause a new index block to be created.
    // during that transaction, return an error so the transaction is cancelled,
    // causing truncation to happen. this test ensures that the new index block
    // is NOT truncated.
    for (let i = SLOT_COUNT + 1; i < SLOT_COUNT * 2 + 1; i++) {
      const value = `wat${i}`;
      const index = i;

      try {
        await rootCursor.writePath([
          new ArrayListInit(),
          new ArrayListAppend(),
          new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
          new HashMapInit(false, false),
          new HashMapGet(new HashMapGetValue(watKey)),
          new WriteData(new Bytes(value)),
          new Context(async () => {
            if (index === 32) {
              throw new Error('intentional error');
            }
          }),
        ]);
      } catch (e) {
        // expected error
      }
    }

    // try another append to make sure we still can.
    // if truncation destroyed the index block, this would fail.
    await rootCursor.writePath([
      new ArrayListInit(),
      new ArrayListAppend(),
      new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
      new HashMapInit(false, false),
      new HashMapGet(new HashMapGetValue(watKey)),
      new WriteData(new Bytes('wat32')),
    ]);

    // slice so it contains exactly SLOT_COUNT, so we have the old root again
    await rootCursor.writePath([new ArrayListInit(), new ArrayListSlice(SLOT_COUNT)]);

    // we can iterate over the remaining slots
    for (let i = 0; i < SLOT_COUNT; i++) {
      const value = `wat${i}`;
      const cursor = await rootCursor.readPath([
        new ArrayListGet(i),
        new HashMapGet(new HashMapGetValue(watKey)),
      ]);
      const value2 = new TextDecoder().decode(await cursor!.readBytes(MAX_READ_BYTES));
      expect(value).toBe(value2);
    }

    // but we can't get the value that we sliced out of the array list
    expect(await rootCursor.readPath([new ArrayListGet(SLOT_COUNT + 1)])).toBeNull();
  }

  // append to inner array_list many times, filling up the array_list until a root overflow occurs
  {
    await core.setLength(0);
    const db = await Database.create(core, hasher);
    const rootCursor = await db.rootCursor();

    for (let i = 0; i < SLOT_COUNT + 1; i++) {
      const value = `wat${i}`;
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(new Bytes(value)),
      ]);
    }

    // verify all values
    for (let i = 0; i < SLOT_COUNT + 1; i++) {
      const value = `wat${i}`;
      const cursor = await rootCursor.readPath([new ArrayListGet(-1), new ArrayListGet(i)]);
      const value2 = new TextDecoder().decode(await cursor!.readBytes(MAX_READ_BYTES));
      expect(value).toBe(value2);
    }

    // slice the inner array list so it contains exactly SLOT_COUNT, so we have the old root again
    await rootCursor.writePath([
      new ArrayListInit(),
      new ArrayListGet(-1),
      new ArrayListInit(),
      new ArrayListSlice(SLOT_COUNT),
    ]);

    // we can iterate over the remaining slots
    for (let i = 0; i < SLOT_COUNT; i++) {
      const value = `wat${i}`;
      const cursor = await rootCursor.readPath([new ArrayListGet(-1), new ArrayListGet(i)]);
      const value2 = new TextDecoder().decode(await cursor!.readBytes(MAX_READ_BYTES));
      expect(value).toBe(value2);
    }

    // but we can't get the value that we sliced out of the array list
    expect(await rootCursor.readPath([new ArrayListGet(-1), new ArrayListGet(SLOT_COUNT + 1)])).toBeNull();

    // overwrite the last value with hello
    await rootCursor.writePath([
      new ArrayListInit(),
      new ArrayListAppend(),
      new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
      new ArrayListInit(),
      new ArrayListGet(-1),
      new WriteData(new Bytes('hello')),
    ]);

    // read last value
    {
      const cursor = await rootCursor.readPath([new ArrayListGet(-1), new ArrayListGet(-1)]);
      const value = new TextDecoder().decode(await cursor!.readBytes(MAX_READ_BYTES));
      expect(value).toBe('hello');
    }

    // overwrite the last value with goodbye
    await rootCursor.writePath([
      new ArrayListInit(),
      new ArrayListAppend(),
      new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
      new ArrayListInit(),
      new ArrayListGet(-1),
      new WriteData(new Bytes('goodbye')),
    ]);

    // read last value
    {
      const cursor = await rootCursor.readPath([new ArrayListGet(-1), new ArrayListGet(-1)]);
      const value = new TextDecoder().decode(await cursor!.readBytes(MAX_READ_BYTES));
      expect(value).toBe('goodbye');
    }

    // previous last value is still hello
    {
      const cursor = await rootCursor.readPath([new ArrayListGet(-2), new ArrayListGet(-1)]);
      const value = new TextDecoder().decode(await cursor!.readBytes(MAX_READ_BYTES));
      expect(value).toBe('hello');
    }
  }

  // iterate over inner array_list
  {
    await core.setLength(0);
    const db = await Database.create(core, hasher);
    const rootCursor = await db.rootCursor();

    // add wats
    for (let i = 0; i < 10; i++) {
      const value = `wat${i}`;
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(new Bytes(value)),
      ]);

      const cursor = await rootCursor.readPath([new ArrayListGet(-1), new ArrayListGet(-1)]);
      const value2 = new TextDecoder().decode(await cursor!.readBytes(MAX_READ_BYTES));
      expect(value).toBe(value2);
    }

    // iterate over array_list
    {
      const innerCursor = await rootCursor.readPath([new ArrayListGet(-1)]);
      const iter = innerCursor!.iterator();
      await iter.init();
      let i = 0;
      while (await iter.hasNext()) {
        const nextCursor = await iter.next();
        const value = `wat${i}`;
        const value2 = new TextDecoder().decode(await nextCursor!.readBytes(MAX_READ_BYTES));
        expect(value).toBe(value2);
        i += 1;
      }
      expect(i).toBe(10);
    }

    // set first slot to .none and make sure iteration still works
    {
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListGet(-1),
        new ArrayListInit(),
        new ArrayListGet(0),
        new WriteData(null),
      ]);
      const innerCursor = await rootCursor.readPath([new ArrayListGet(-1)]);
      const iter = innerCursor!.iterator();
      await iter.init();
      let i = 0;
      while (await iter.hasNext()) {
        await iter.next();
        i += 1;
      }
      expect(i).toBe(10);
    }

    // get list slot
    const listCursor = await rootCursor.readPath([new ArrayListGet(-1)]);
    expect(await listCursor!.count()).toBe(10);
  }

  // iterate over inner hash_map
  {
    await core.setLength(0);
    const db = await Database.create(core, hasher);
    const rootCursor = await db.rootCursor();

    // add wats
    for (let i = 0; i < 10; i++) {
      const value = `wat${i}`;
      const watKey = await db.hasher.digest(new TextEncoder().encode(value));
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new HashMapInit(false, false),
        new HashMapGet(new HashMapGetValue(watKey)),
        new WriteData(new Bytes(value)),
      ]);

      const cursor = await rootCursor.readPath([
        new ArrayListGet(-1),
        new HashMapGet(new HashMapGetValue(watKey)),
      ]);
      const value2 = new TextDecoder().decode(await cursor!.readBytes(MAX_READ_BYTES));
      expect(value).toBe(value2);
    }

    // add foo
    const fooKey = await db.hasher.digest(new TextEncoder().encode('foo'));
    await rootCursor.writePath([
      new ArrayListInit(),
      new ArrayListAppend(),
      new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
      new HashMapInit(false, false),
      new HashMapGet(new HashMapGetKey(fooKey)),
      new WriteData(new Bytes('foo')),
    ]);
    await rootCursor.writePath([
      new ArrayListInit(),
      new ArrayListAppend(),
      new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
      new HashMapInit(false, false),
      new HashMapGet(new HashMapGetValue(fooKey)),
      new WriteData(new Uint(42)),
    ]);

    // remove a wat
    await rootCursor.writePath([
      new ArrayListInit(),
      new ArrayListAppend(),
      new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
      new HashMapInit(false, false),
      new HashMapRemove(await db.hasher.digest(new TextEncoder().encode('wat0'))),
    ]);

    // iterate over hash_map
    {
      const innerCursor = await rootCursor.readPath([new ArrayListGet(-1)]);
      const iter = innerCursor!.iterator();
      await iter.init();
      let i = 0;
      while (await iter.hasNext()) {
        const kvPairCursor = await iter.next();
        const kvPair = await kvPairCursor!.readKeyValuePair();
        if (arraysEqual(kvPair.hash, fooKey)) {
          const key = new TextDecoder().decode(await kvPair.keyCursor.readBytes(MAX_READ_BYTES));
          expect(key).toBe('foo');
          expect(kvPair.valueCursor.slotPtr.slot.value).toBe(42n);
        } else {
          const value = await kvPair.valueCursor.readBytes(MAX_READ_BYTES);
          const hash = await db.hasher.digest(value);
          expect(arraysEqual(kvPair.hash, hash)).toBe(true);
        }
        i += 1;
      }
      expect(i).toBe(10);
    }

    // iterate over hash_map with writeable cursor
    {
      const innerCursor = await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
      ]);
      const iter = (innerCursor as WriteCursor).iterator();
      await iter.init();
      let i = 0;
      while (await iter.hasNext()) {
        const kvPairCursor = await iter.next();
        const kvPair = await kvPairCursor!.readKeyValuePair();
        if (arraysEqual(kvPair.hash, fooKey)) {
          await (kvPair.keyCursor as WriteCursor).write(new Bytes('bar'));
        }
        i += 1;
      }
      expect(i).toBe(10);
    }
  }

  {
    // slice linked_array_list
    await testSlice(core, hasher, SLOT_COUNT * 5 + 1, 10, 5);
    await testSlice(core, hasher, SLOT_COUNT * 5 + 1, 0, SLOT_COUNT * 2);
    await testSlice(core, hasher, SLOT_COUNT * 5, SLOT_COUNT * 3, SLOT_COUNT);
    await testSlice(core, hasher, SLOT_COUNT * 5, SLOT_COUNT * 3, SLOT_COUNT * 2);
    await testSlice(core, hasher, SLOT_COUNT * 2, 10, SLOT_COUNT);
    await testSlice(core, hasher, 2, 0, 2);
    await testSlice(core, hasher, 2, 1, 1);
    await testSlice(core, hasher, 1, 0, 0);

    // concat linked_array_list
    await testConcat(core, hasher, SLOT_COUNT * 5 + 1, SLOT_COUNT + 1);
    await testConcat(core, hasher, SLOT_COUNT, SLOT_COUNT);
    await testConcat(core, hasher, 1, 1);
    await testConcat(core, hasher, 0, 0);

    // insert linked_array_list
    await testInsertAndRemove(core, hasher, 1, 0);
    await testInsertAndRemove(core, hasher, 10, 0);
    await testInsertAndRemove(core, hasher, 10, 5);
    await testInsertAndRemove(core, hasher, 10, 9);
    await testInsertAndRemove(core, hasher, SLOT_COUNT * 5, SLOT_COUNT * 2);
  }

  // concat linked_array_list multiple times
  {
    await core.setLength(0);
    const db = await Database.create(core, hasher);
    const rootCursor = await db.rootCursor();

    const evenKey = await db.hasher.digest(new TextEncoder().encode('even'));
    const comboKey = await db.hasher.digest(new TextEncoder().encode('combo'));

    await rootCursor.writePath([
      new ArrayListInit(),
      new ArrayListAppend(),
      new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
      new HashMapInit(false, false),
      new Context(async (cursor) => {
        // create list
        for (let i = 0; i < SLOT_COUNT + 1; i++) {
          const n = i * 2;
          await cursor.writePath([
            new HashMapGet(new HashMapGetValue(evenKey)),
            new LinkedArrayListInit(),
            new LinkedArrayListAppend(),
            new WriteData(new Uint(n)),
          ]);
        }

        // get list slot
        const evenListCursor = await cursor.readPath([new HashMapGet(new HashMapGetValue(evenKey))]);
        expect(await evenListCursor!.count()).toBe(SLOT_COUNT + 1);

        // check all values in the new slice with an iterator
        {
          const innerCursor = await cursor.readPath([new HashMapGet(new HashMapGetValue(evenKey))]);
          const iter = innerCursor!.iterator();
          await iter.init();
          let i = 0;
          while (await iter.hasNext()) {
            await iter.next();
            i += 1;
          }
          expect(i).toBe(SLOT_COUNT + 1);
        }

        // concat the list with itself multiple times.
        // since each list has 17 items, each concat will create a gap, causing a root overflow
        // before a normal array list would've.
        let comboListCursor = await cursor.writePath([
          new HashMapGet(new HashMapGetValue(comboKey)),
          new WriteData(evenListCursor!.slotPtr.slot),
          new LinkedArrayListInit(),
        ]);
        for (let i = 0; i < 16; i++) {
          comboListCursor = await comboListCursor.writePath([
            new LinkedArrayListConcat(evenListCursor!.slotPtr.slot),
          ]);
        }

        // append to the new list
        await cursor.writePath([
          new HashMapGet(new HashMapGetValue(comboKey)),
          new LinkedArrayListAppend(),
          new WriteData(new Uint(3)),
        ]);

        // read the new value from the list
        expect(
          (await cursor.readPath([new HashMapGet(new HashMapGetValue(comboKey)), new LinkedArrayListGet(-1)]))!.readUint()
        ).toBe(3);

        // append more to the new list
        for (let i = 0; i < 500; i++) {
          await cursor.writePath([
            new HashMapGet(new HashMapGetValue(comboKey)),
            new LinkedArrayListAppend(),
            new WriteData(new Uint(1)),
          ]);
        }
      }),
    ]);
  }

  // append items to linked_array_list without setting their value
  {
    await core.setLength(0);
    const db = await Database.create(core, hasher);
    const rootCursor = await db.rootCursor();

    // appending without setting any value should work
    for (let i = 0; i < 8; i++) {
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new LinkedArrayListInit(),
        new LinkedArrayListAppend(),
      ]);
    }

    // explicitly writing a null slot should also work
    for (let i = 0; i < 8; i++) {
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new LinkedArrayListInit(),
        new LinkedArrayListAppend(),
        new WriteData(null),
      ]);
    }
  }

  // insert at beginning of linked_array_list many times
  {
    await core.setLength(0);
    const db = await Database.create(core, hasher);
    const rootCursor = await db.rootCursor();

    await rootCursor.writePath([
      new ArrayListInit(),
      new ArrayListAppend(),
      new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
      new LinkedArrayListInit(),
      new LinkedArrayListAppend(),
      new WriteData(new Uint(42)),
    ]);

    for (let i = 0; i < 1000; i++) {
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new LinkedArrayListInit(),
        new LinkedArrayListInsert(0),
        new WriteData(new Uint(i)),
      ]);
    }
  }

  // insert at end of linked_array_list many times
  {
    await core.setLength(0);
    const db = await Database.create(core, hasher);
    const rootCursor = await db.rootCursor();

    await rootCursor.writePath([
      new ArrayListInit(),
      new ArrayListAppend(),
      new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
      new LinkedArrayListInit(),
      new LinkedArrayListAppend(),
      new WriteData(new Uint(42)),
    ]);

    for (let i = 0; i < 1000; i++) {
      await rootCursor.writePath([
        new ArrayListInit(),
        new ArrayListAppend(),
        new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
        new LinkedArrayListInit(),
        new LinkedArrayListInsert(i),
        new WriteData(new Uint(i)),
      ]);
    }
  }
}

// Helper function to compare Uint8Arrays
function arraysEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

// Helper function to convert bytes to BigInt
function bytesToBigInt(bytes: Uint8Array): bigint {
  let result = 0n;
  for (let i = 0; i < bytes.length; i++) {
    result = (result << 8n) | BigInt(bytes[i]);
  }
  return result;
}

async function testSlice(
  core: Core,
  hasher: Hasher,
  originalSize: number,
  sliceOffset: number,
  sliceSize: number
): Promise<void> {
  await core.setLength(0);
  const db = await Database.create(core, hasher);
  const rootCursor = await db.rootCursor();

  const evenKey = await db.hasher.digest(new TextEncoder().encode('even'));
  const evenSliceKey = await db.hasher.digest(new TextEncoder().encode('even-slice'));
  const comboKey = await db.hasher.digest(new TextEncoder().encode('combo'));

  await rootCursor.writePath([
    new ArrayListInit(),
    new ArrayListAppend(),
    new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
    new HashMapInit(false, false),
    new Context(async (cursor) => {
      const values: number[] = [];

      // create list
      for (let i = 0; i < originalSize; i++) {
        const n = i * 2;
        values.push(n);
        await cursor.writePath([
          new HashMapGet(new HashMapGetValue(evenKey)),
          new LinkedArrayListInit(),
          new LinkedArrayListAppend(),
          new WriteData(new Uint(n)),
        ]);
      }

      // slice list
      const evenListCursor = await cursor.readPath([new HashMapGet(new HashMapGetValue(evenKey))]);
      const evenListSliceCursor = await cursor.writePath([
        new HashMapGet(new HashMapGetValue(evenSliceKey)),
        new WriteData(evenListCursor!.slotPtr.slot),
        new LinkedArrayListInit(),
        new LinkedArrayListSlice(sliceOffset, sliceSize),
      ]);

      // check all the values in the new slice
      for (let i = 0; i < sliceSize; i++) {
        const val = values[sliceOffset + i];
        const n = (
          await cursor.readPath([new HashMapGet(new HashMapGetValue(evenSliceKey)), new LinkedArrayListGet(i)])
        )!.readUint();
        expect(val).toBe(n);
      }

      // check all values in the new slice with an iterator
      {
        const iter = evenListSliceCursor.iterator();
        await iter.init();
        let i = 0;
        while (await iter.hasNext()) {
          const numCursor = await iter.next();
          expect(values[sliceOffset + i]).toBe(numCursor!.readUint());
          i += 1;
        }
        expect(sliceSize).toBe(i);
      }

      // there are no extra items
      expect(
        await cursor.readPath([new HashMapGet(new HashMapGetValue(evenSliceKey)), new LinkedArrayListGet(sliceSize)])
      ).toBeNull();

      // concat the slice with itself
      await cursor.writePath([
        new HashMapGet(new HashMapGetValue(comboKey)),
        new WriteData(evenListSliceCursor.slotPtr.slot),
        new LinkedArrayListInit(),
        new LinkedArrayListConcat(evenListSliceCursor.slotPtr.slot),
      ]);

      // check all values in the combo list
      const comboValues: number[] = [];
      comboValues.push(...values.slice(sliceOffset, sliceOffset + sliceSize));
      comboValues.push(...values.slice(sliceOffset, sliceOffset + sliceSize));
      for (let i = 0; i < comboValues.length; i++) {
        const n = (
          await cursor.readPath([new HashMapGet(new HashMapGetValue(comboKey)), new LinkedArrayListGet(i)])
        )!.readUint();
        expect(comboValues[i]).toBe(n);
      }

      // append to the slice
      await cursor.writePath([
        new HashMapGet(new HashMapGetValue(evenSliceKey)),
        new LinkedArrayListInit(),
        new LinkedArrayListAppend(),
        new WriteData(new Uint(3)),
      ]);

      // read the new value from the slice
      expect(
        (await cursor.readPath([new HashMapGet(new HashMapGetValue(evenSliceKey)), new LinkedArrayListGet(-1)]))!
          .readUint()
      ).toBe(3);
    }),
  ]);
}

async function testConcat(core: Core, hasher: Hasher, listASize: number, listBSize: number): Promise<void> {
  await core.setLength(0);
  const db = await Database.create(core, hasher);
  const rootCursor = await db.rootCursor();

  const evenKey = await db.hasher.digest(new TextEncoder().encode('even'));
  const oddKey = await db.hasher.digest(new TextEncoder().encode('odd'));
  const comboKey = await db.hasher.digest(new TextEncoder().encode('combo'));

  const values: number[] = [];

  await rootCursor.writePath([
    new ArrayListInit(),
    new ArrayListAppend(),
    new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
    new HashMapInit(false, false),
    new Context(async (cursor) => {
      // create even list
      await cursor.writePath([new HashMapGet(new HashMapGetValue(evenKey)), new LinkedArrayListInit()]);
      for (let i = 0; i < listASize; i++) {
        const n = i * 2;
        values.push(n);
        await cursor.writePath([
          new HashMapGet(new HashMapGetValue(evenKey)),
          new LinkedArrayListInit(),
          new LinkedArrayListAppend(),
          new WriteData(new Uint(n)),
        ]);
      }

      // create odd list
      await cursor.writePath([new HashMapGet(new HashMapGetValue(oddKey)), new LinkedArrayListInit()]);
      for (let i = 0; i < listBSize; i++) {
        const n = i * 2 + 1;
        values.push(n);
        await cursor.writePath([
          new HashMapGet(new HashMapGetValue(oddKey)),
          new LinkedArrayListInit(),
          new LinkedArrayListAppend(),
          new WriteData(new Uint(n)),
        ]);
      }
    }),
  ]);

  await rootCursor.writePath([
    new ArrayListInit(),
    new ArrayListAppend(),
    new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
    new HashMapInit(false, false),
    new Context(async (cursor) => {
      // get the even list
      const evenListCursor = await cursor.readPath([new HashMapGet(new HashMapGetValue(evenKey))]);

      // get the odd list
      const oddListCursor = await cursor.readPath([new HashMapGet(new HashMapGetValue(oddKey))]);

      // concat the lists
      const comboListCursor = await cursor.writePath([
        new HashMapGet(new HashMapGetValue(comboKey)),
        new WriteData(evenListCursor!.slotPtr.slot),
        new LinkedArrayListInit(),
        new LinkedArrayListConcat(oddListCursor!.slotPtr.slot),
      ]);

      // check all values in the new list
      for (let i = 0; i < values.length; i++) {
        const n = (
          await cursor.readPath([new HashMapGet(new HashMapGetValue(comboKey)), new LinkedArrayListGet(i)])
        )!.readUint();
        expect(values[i]).toBe(n);
      }

      // check all values in the new slice with an iterator
      {
        const iter = comboListCursor.iterator();
        await iter.init();
        let i = 0;
        while (await iter.hasNext()) {
          const numCursor = await iter.next();
          expect(values[i]).toBe(numCursor!.readUint());
          i += 1;
        }
        expect((await evenListCursor!.count()) + (await oddListCursor!.count())).toBe(i);
      }

      // there are no extra items
      expect(
        await cursor.readPath([new HashMapGet(new HashMapGetValue(comboKey)), new LinkedArrayListGet(values.length)])
      ).toBeNull();
    }),
  ]);
}

async function testInsertAndRemove(core: Core, hasher: Hasher, originalSize: number, insertIndex: number): Promise<void> {
  await core.setLength(0);
  const db = await Database.create(core, hasher);
  const rootCursor = await db.rootCursor();

  const evenKey = await db.hasher.digest(new TextEncoder().encode('even'));
  const evenInsertKey = await db.hasher.digest(new TextEncoder().encode('even-insert'));
  const insertValue = 12345;

  await rootCursor.writePath([
    new ArrayListInit(),
    new ArrayListAppend(),
    new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
    new HashMapInit(false, false),
    new Context(async (cursor) => {
      const values: number[] = [];

      // create list
      for (let i = 0; i < originalSize; i++) {
        if (i === insertIndex) {
          values.push(insertValue);
        }
        const n = i * 2;
        values.push(n);
        await cursor.writePath([
          new HashMapGet(new HashMapGetValue(evenKey)),
          new LinkedArrayListInit(),
          new LinkedArrayListAppend(),
          new WriteData(new Uint(n)),
        ]);
      }

      // insert into list
      const evenListCursor = await cursor.readPath([new HashMapGet(new HashMapGetValue(evenKey))]);
      const evenListInsertCursor = await cursor.writePath([
        new HashMapGet(new HashMapGetValue(evenInsertKey)),
        new WriteData(evenListCursor!.slotPtr.slot),
        new LinkedArrayListInit(),
      ]);
      await evenListInsertCursor.writePath([
        new LinkedArrayListInsert(insertIndex),
        new WriteData(new Uint(insertValue)),
      ]);

      // check all the values in the new list
      for (let i = 0; i < values.length; i++) {
        const val = values[i];
        const n = (
          await cursor.readPath([new HashMapGet(new HashMapGetValue(evenInsertKey)), new LinkedArrayListGet(i)])
        )!.readUint();
        expect(val).toBe(n);
      }

      // check all values in the new list with an iterator
      {
        const iter = evenListInsertCursor.iterator();
        await iter.init();
        let i = 0;
        while (await iter.hasNext()) {
          const numCursor = await iter.next();
          expect(values[i]).toBe(numCursor!.readUint());
          i += 1;
        }
        expect(values.length).toBe(i);
      }

      // there are no extra items
      expect(
        await cursor.readPath([
          new HashMapGet(new HashMapGetValue(evenInsertKey)),
          new LinkedArrayListGet(values.length),
        ])
      ).toBeNull();
    }),
  ]);

  await rootCursor.writePath([
    new ArrayListInit(),
    new ArrayListAppend(),
    new WriteData(await rootCursor.readPathSlot([new ArrayListGet(-1)])),
    new HashMapInit(false, false),
    new Context(async (cursor) => {
      const values: number[] = [];

      for (let i = 0; i < originalSize; i++) {
        const n = i * 2;
        values.push(n);
      }

      // remove inserted value from the list
      const evenListInsertCursor = await cursor.writePath([
        new HashMapGet(new HashMapGetValue(evenInsertKey)),
        new LinkedArrayListRemove(insertIndex),
      ]);

      // check all the values in the new list
      for (let i = 0; i < values.length; i++) {
        const val = values[i];
        const n = (
          await cursor.readPath([new HashMapGet(new HashMapGetValue(evenInsertKey)), new LinkedArrayListGet(i)])
        )!.readUint();
        expect(val).toBe(n);
      }

      // check all values in the new list with an iterator
      {
        const iter = evenListInsertCursor.iterator();
        await iter.init();
        let i = 0;
        while (await iter.hasNext()) {
          const numCursor = await iter.next();
          expect(values[i]).toBe(numCursor!.readUint());
          i += 1;
        }
        expect(values.length).toBe(i);
      }

      // there are no extra items
      expect(
        await cursor.readPath([
          new HashMapGet(new HashMapGetValue(evenInsertKey)),
          new LinkedArrayListGet(values.length),
        ])
      ).toBeNull();
    }),
  ]);
}