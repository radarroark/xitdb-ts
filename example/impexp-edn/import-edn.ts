import {
  Database,
  Hasher,
  CoreBufferedFile,
  WriteArrayList,
  WriteLinkedArrayList,
  WriteHashMap,
  WriteHashSet,
  WriteCursor,
  Bytes,
  Int,
  Float,
} from 'xitdb';
import { parseEDNString } from 'edn-data';
import type { EDNVal, EDNMap, EDNSet, EDNList, EDNKeyword, EDNSymbol } from 'edn-data/dist/types';

const FORMAT_TAG_KEYWORD = 'kw';
const FORMAT_TAG_SYMBOL = 'sy';
const FORMAT_TAG_BOOLEAN = 'bl';

export async function importEdn(ednPath: string, dbPath: string): Promise<void> {
  const ednContent = await Bun.file(ednPath).text();
  const edn = parseEDNString(ednContent) as EDNVal;

  const core = await CoreBufferedFile.create(dbPath);
  const db = await Database.create(core, new Hasher('SHA-1'));
  const rootCursor = db.rootCursor();
  const history = await WriteArrayList.create(rootCursor);

  await history.appendContext(null, async (cursor) => {
    await writeEdnValue(cursor, edn);
  });

  await core.flush();
}

function isEdnMap(value: unknown): value is EDNMap {
  return typeof value === 'object' && value !== null && 'map' in value && Array.isArray((value as EDNMap).map);
}

function isEdnSet(value: unknown): value is EDNSet {
  return typeof value === 'object' && value !== null && 'set' in value && Array.isArray((value as EDNSet).set);
}

function isEdnList(value: unknown): value is EDNList {
  return typeof value === 'object' && value !== null && 'list' in value && Array.isArray((value as EDNList).list);
}

function isEdnKeyword(value: unknown): value is EDNKeyword {
  return typeof value === 'object' && value !== null && 'key' in value && typeof (value as EDNKeyword).key === 'string';
}

function isEdnSymbol(value: unknown): value is EDNSymbol {
  return typeof value === 'object' && value !== null && 'sym' in value && typeof (value as EDNSymbol).sym === 'string';
}

async function writeEdnValue(cursor: WriteCursor, value: EDNVal): Promise<void> {
  // null (nil)
  if (value === null) {
    // Leave as NONE (default empty slot)
    return;
  }

  // boolean
  if (typeof value === 'boolean') {
    await cursor.write(new Bytes(value ? 'true' : 'false', FORMAT_TAG_BOOLEAN));
    return;
  }

  // number (integer or float)
  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      await cursor.write(new Int(value));
    } else {
      await cursor.write(new Float(value));
    }
    return;
  }

  // bigint
  if (typeof value === 'bigint') {
    await cursor.write(new Int(Number(value)));
    return;
  }

  // string
  if (typeof value === 'string') {
    await cursor.write(new Bytes(value));
    return;
  }

  // Date
  if (value instanceof Date) {
    await cursor.write(new Bytes(value.toISOString()));
    return;
  }

  // array (vector)
  if (Array.isArray(value)) {
    const list = await WriteArrayList.create(cursor);
    for (const element of value) {
      const elementCursor = await list.appendCursor();
      await writeEdnValue(elementCursor, element);
    }
    return;
  }

  // keyword
  if (isEdnKeyword(value)) {
    const keywordStr = `:${value.key}`;
    await cursor.write(new Bytes(keywordStr, FORMAT_TAG_KEYWORD));
    return;
  }

  // symbol
  if (isEdnSymbol(value)) {
    await cursor.write(new Bytes(value.sym, FORMAT_TAG_SYMBOL));
    return;
  }

  // list
  if (isEdnList(value)) {
    const list = await WriteLinkedArrayList.create(cursor);
    for (const element of value.list) {
      const elementCursor = await list.appendCursor();
      await writeEdnValue(elementCursor, element);
    }
    return;
  }

  // set
  if (isEdnSet(value)) {
    const set = await WriteHashSet.create(cursor);
    for (const element of value.set) {
      const elementBytes = ednValueToBytes(element);
      const elementCursor = await set.putCursor(elementBytes);
      await writeEdnValue(elementCursor, element);
    }
    return;
  }

  // map
  if (isEdnMap(value)) {
    const map = await WriteHashMap.create(cursor);
    for (const [key, val] of value.map) {
      const keyBytes = getMapKeyBytes(key);
      const valueCursor = await map.putCursor(keyBytes);
      await map.putKey(keyBytes, keyBytes);
      await writeEdnValue(valueCursor, val);
    }
    return;
  }

  throw new Error(`Unsupported EDN value type: ${typeof value}`);
}

function getMapKeyBytes(key: EDNVal): Bytes {
  if (typeof key === 'string') {
    return new Bytes(key);
  }
  if (isEdnKeyword(key)) {
    const keywordStr = `:${key.key}`;
    return new Bytes(keywordStr, FORMAT_TAG_KEYWORD);
  }
  throw new Error(`Unsupported map key type: ${typeof key}. Only string and keyword keys are supported.`);
}

function ednValueToBytes(value: EDNVal): Bytes {
  const str = ednValueToString(value);
  return new Bytes(str);
}

function ednValueToString(value: EDNVal): string {
  if (value === null) return 'nil';
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  if (typeof value === 'number') return value.toString();
  if (typeof value === 'bigint') return value.toString();
  if (typeof value === 'string') return `"${escapeString(value)}"`;
  if (value instanceof Date) return `#inst "${value.toISOString()}"`;
  if (Array.isArray(value)) return `[${value.map(ednValueToString).join(' ')}]`;
  if (isEdnKeyword(value)) return `:${value.key}`;
  if (isEdnSymbol(value)) return value.sym;
  if (isEdnList(value)) return `(${value.list.map(ednValueToString).join(' ')})`;
  if (isEdnSet(value)) return `#{${value.set.map(ednValueToString).join(' ')}}`;
  if (isEdnMap(value)) return `{${value.map.map(([k, v]) => `${ednValueToString(k)} ${ednValueToString(v)}`).join(' ')}}`;
  return String(value);
}

function escapeString(str: string): string {
  return str
    .replace(/\\/g, '\\\\')
    .replace(/"/g, '\\"')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r')
    .replace(/\t/g, '\\t');
}
