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
import { parseEdn } from './edn/parser';
import { EdnValue } from './edn/types';

const FORMAT_TAG_KEYWORD = 'kw';
const FORMAT_TAG_SYMBOL = 'sy';
const FORMAT_TAG_BOOLEAN = 'bl';

export async function importEdn(ednPath: string, dbPath: string): Promise<void> {
  const ednContent = await Bun.file(ednPath).text();
  const edn = parseEdn(ednContent);

  if (edn.type !== 'map') {
    throw new Error('Root EDN value must be a map');
  }

  const core = await CoreBufferedFile.create(dbPath);
  const db = await Database.create(core, new Hasher('SHA-1'));
  const rootCursor = await db.rootCursor();
  const history = await WriteArrayList.create(rootCursor);

  await history.appendContext(null, async (cursor) => {
    await writeEdnValue(cursor, edn);
  });

  await core.flush();
}

async function writeEdnValue(cursor: WriteCursor, value: EdnValue): Promise<void> {
  switch (value.type) {
    case 'nil':
      // Leave as NONE (default empty slot)
      break;

    case 'boolean':
      await cursor.write(new Bytes(value.value ? 'true' : 'false', FORMAT_TAG_BOOLEAN));
      break;

    case 'integer':
      await cursor.write(new Int(value.value));
      break;

    case 'float':
      await cursor.write(new Float(value.value));
      break;

    case 'string':
      await cursor.write(new Bytes(value.value));
      break;

    case 'keyword': {
      const keywordStr = value.namespace
        ? `:${value.namespace}/${value.name}`
        : `:${value.name}`;
      await cursor.write(new Bytes(keywordStr, FORMAT_TAG_KEYWORD));
      break;
    }

    case 'symbol': {
      const symbolStr = value.namespace
        ? `${value.namespace}/${value.name}`
        : value.name;
      await cursor.write(new Bytes(symbolStr, FORMAT_TAG_SYMBOL));
      break;
    }

    case 'vector': {
      const list = await WriteArrayList.create(cursor);
      for (const element of value.elements) {
        const elementCursor = await list.appendCursor();
        await writeEdnValue(elementCursor, element);
      }
      break;
    }

    case 'list': {
      const list = await WriteLinkedArrayList.create(cursor);
      for (const element of value.elements) {
        const elementCursor = await list.appendCursor();
        await writeEdnValue(elementCursor, element);
      }
      break;
    }

    case 'set': {
      const set = await WriteHashSet.create(cursor);
      for (const element of value.elements) {
        const elementBytes = ednValueToBytes(element);
        const elementCursor = await set.putCursorByBytes(elementBytes);
        await writeEdnValue(elementCursor, element);
      }
      break;
    }

    case 'map': {
      const map = await WriteHashMap.create(cursor);
      for (const [key, val] of value.entries) {
        const keyBytes = getMapKeyBytes(key);
        const valueCursor = await map.putCursorByBytes(keyBytes);
        // Write the key with appropriate format tag
        await map.putKeyByBytes(keyBytes, keyBytes);
        await writeEdnValue(valueCursor, val);
      }
      break;
    }
  }
}

function getMapKeyBytes(key: EdnValue): Bytes {
  if (key.type === 'string') {
    return new Bytes(key.value);
  }
  if (key.type === 'keyword') {
    const keywordStr = key.namespace
      ? `:${key.namespace}/${key.name}`
      : `:${key.name}`;
    return new Bytes(keywordStr, FORMAT_TAG_KEYWORD);
  }
  throw new Error(`Unsupported map key type: ${key.type}. Only string and keyword keys are supported.`);
}

function ednValueToBytes(value: EdnValue): Bytes {
  // Serialize EDN value to bytes for hashing in sets
  const str = ednValueToString(value);
  return new Bytes(str);
}

function ednValueToString(value: EdnValue): string {
  switch (value.type) {
    case 'nil':
      return 'nil';
    case 'boolean':
      return value.value ? 'true' : 'false';
    case 'integer':
      return value.value.toString();
    case 'float':
      return value.value.toString();
    case 'string':
      return `"${escapeString(value.value)}"`;
    case 'keyword':
      return value.namespace ? `:${value.namespace}/${value.name}` : `:${value.name}`;
    case 'symbol':
      return value.namespace ? `${value.namespace}/${value.name}` : value.name;
    case 'vector':
      return `[${value.elements.map(ednValueToString).join(' ')}]`;
    case 'list':
      return `(${value.elements.map(ednValueToString).join(' ')})`;
    case 'set':
      return `#{${value.elements.map(ednValueToString).join(' ')}}`;
    case 'map':
      return `{${value.entries.map(([k, v]) => `${ednValueToString(k)} ${ednValueToString(v)}`).join(' ')}}`;
  }
}

function escapeString(str: string): string {
  return str
    .replace(/\\/g, '\\\\')
    .replace(/"/g, '\\"')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r')
    .replace(/\t/g, '\\t');
}
