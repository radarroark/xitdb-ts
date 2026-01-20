import { ReadCursor, Tag, ReadArrayList } from 'xitdb';
import { EdnValue, ednNil, ednBoolean, ednInteger, ednFloat, ednString, ednKeyword, ednSymbol, ednVector, ednList, ednSet, ednMap } from './types';

const FORMAT_TAG_KEYWORD = 'kw';
const FORMAT_TAG_SYMBOL = 'sy';
const FORMAT_TAG_BOOLEAN = 'bl';

export async function cursorToEdnValue(cursor: ReadCursor): Promise<EdnValue> {
  const tag = cursor.slotPtr.slot.tag;

  switch (tag) {
    case Tag.NONE:
      return ednNil();

    case Tag.BYTES:
    case Tag.SHORT_BYTES: {
      const bytesObj = await cursor.readBytesObject(null);
      const text = new TextDecoder().decode(bytesObj.value);

      if (bytesObj.formatTag) {
        const formatTag = new TextDecoder().decode(bytesObj.formatTag);
        if (formatTag === FORMAT_TAG_BOOLEAN) {
          return ednBoolean(text === 'true');
        }
        if (formatTag === FORMAT_TAG_KEYWORD) {
          return parseKeywordOrSymbol(text, true);
        }
        if (formatTag === FORMAT_TAG_SYMBOL) {
          return parseKeywordOrSymbol(text, false);
        }
      }
      return ednString(text);
    }

    case Tag.UINT:
      return ednInteger(cursor.readUint());

    case Tag.INT:
      return ednInteger(cursor.readInt());

    case Tag.FLOAT:
      return ednFloat(cursor.readFloat());

    case Tag.ARRAY_LIST: {
      const list = new ReadArrayList(cursor);
      const count = await list.count();
      const elements: EdnValue[] = [];
      for (let i = 0n; i < count; i++) {
        const itemCursor = await list.getCursor(i);
        if (itemCursor) {
          elements.push(await cursorToEdnValue(itemCursor));
        }
      }
      return ednVector(elements);
    }

    case Tag.LINKED_ARRAY_LIST: {
      const elements: EdnValue[] = [];
      const iter = cursor.iterator();
      await iter.init();
      while (await iter.hasNext()) {
        const itemCursor = await iter.next();
        if (itemCursor) {
          elements.push(await cursorToEdnValue(itemCursor));
        }
      }
      return ednList(elements);
    }

    case Tag.HASH_MAP:
    case Tag.COUNTED_HASH_MAP: {
      const entries: [EdnValue, EdnValue][] = [];
      const iter = cursor.iterator();
      await iter.init();
      while (await iter.hasNext()) {
        const kvPairCursor = await iter.next();
        if (kvPairCursor) {
          const kvPair = await kvPairCursor.readKeyValuePair();
          const key = await cursorToEdnValue(kvPair.keyCursor);
          const value = await cursorToEdnValue(kvPair.valueCursor);
          entries.push([key, value]);
        }
      }
      return ednMap(entries);
    }

    case Tag.HASH_SET:
    case Tag.COUNTED_HASH_SET: {
      const elements: EdnValue[] = [];
      const iter = cursor.iterator();
      await iter.init();
      while (await iter.hasNext()) {
        const kvPairCursor = await iter.next();
        if (kvPairCursor) {
          const kvPair = await kvPairCursor.readKeyValuePair();
          elements.push(await cursorToEdnValue(kvPair.keyCursor));
        }
      }
      return ednSet(elements);
    }

    default:
      throw new Error(`Unsupported tag: ${tag}`);
  }
}

function parseKeywordOrSymbol(text: string, isKeyword: boolean): EdnValue {
  // Remove leading : for keywords
  const str = isKeyword && text.startsWith(':') ? text.slice(1) : text;
  const slashIdx = str.indexOf('/');

  if (slashIdx > 0 && slashIdx < str.length - 1) {
    const namespace = str.substring(0, slashIdx);
    const name = str.substring(slashIdx + 1);
    return isKeyword ? ednKeyword(name, namespace) : ednSymbol(name, namespace);
  }

  return isKeyword ? ednKeyword(str) : ednSymbol(str);
}

export function ednValueToString(value: EdnValue, indent: number = 0): string {
  const spaces = '  '.repeat(indent);
  const innerSpaces = '  '.repeat(indent + 1);

  switch (value.type) {
    case 'nil':
      return 'nil';

    case 'boolean':
      return value.value ? 'true' : 'false';

    case 'integer':
      return value.value.toString();

    case 'float': {
      const str = value.value.toString();
      // Ensure there's a decimal point for floats
      if (!str.includes('.') && !str.includes('e') && !str.includes('E')) {
        return str + '.0';
      }
      return str;
    }

    case 'string':
      return `"${escapeString(value.value)}"`;

    case 'keyword':
      return value.namespace ? `:${value.namespace}/${value.name}` : `:${value.name}`;

    case 'symbol':
      return value.namespace ? `${value.namespace}/${value.name}` : value.name;

    case 'vector': {
      if (value.elements.length === 0) {
        return '[]';
      }
      if (isSimpleCollection(value.elements)) {
        return `[${value.elements.map(e => ednValueToString(e, 0)).join(' ')}]`;
      }
      const items = value.elements.map(e => innerSpaces + ednValueToString(e, indent + 1));
      return `[\n${items.join('\n')}\n${spaces}]`;
    }

    case 'list': {
      if (value.elements.length === 0) {
        return '()';
      }
      if (isSimpleCollection(value.elements)) {
        return `(${value.elements.map(e => ednValueToString(e, 0)).join(' ')})`;
      }
      const items = value.elements.map(e => innerSpaces + ednValueToString(e, indent + 1));
      return `(\n${items.join('\n')}\n${spaces})`;
    }

    case 'set': {
      if (value.elements.length === 0) {
        return '#{}';
      }
      if (isSimpleCollection(value.elements)) {
        return `#{${value.elements.map(e => ednValueToString(e, 0)).join(' ')}}`;
      }
      const items = value.elements.map(e => innerSpaces + ednValueToString(e, indent + 1));
      return `#{\n${items.join('\n')}\n${spaces}}`;
    }

    case 'map': {
      if (value.entries.length === 0) {
        return '{}';
      }
      const pairs = value.entries.map(([k, v]) => {
        const keyStr = ednValueToString(k, indent + 1);
        const valStr = ednValueToString(v, indent + 1);
        return `${innerSpaces}${keyStr} ${valStr}`;
      });
      return `{\n${pairs.join('\n')}\n${spaces}}`;
    }
  }
}

function isSimpleCollection(elements: EdnValue[]): boolean {
  if (elements.length > 5) return false;
  return elements.every(e =>
    e.type === 'nil' ||
    e.type === 'boolean' ||
    e.type === 'integer' ||
    e.type === 'float' ||
    e.type === 'string' ||
    e.type === 'keyword' ||
    e.type === 'symbol'
  );
}

function escapeString(str: string): string {
  return str
    .replace(/\\/g, '\\\\')
    .replace(/"/g, '\\"')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r')
    .replace(/\t/g, '\\t');
}

export async function cursorToEdn(cursor: ReadCursor): Promise<string> {
  const value = await cursorToEdnValue(cursor);
  return ednValueToString(value, 0);
}
