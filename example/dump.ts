#!/usr/bin/env bun
import {
  Database,
  Hasher,
  CoreBufferedFile,
  ReadArrayList,
  ReadCursor,
  Tag,
} from '../src';

async function formatKey(cursor: ReadCursor): Promise<string> {
  const tag = cursor.slotPtr.slot.tag;

  switch (tag) {
    case Tag.NONE:
      return '(none)';
    case Tag.BYTES:
    case Tag.SHORT_BYTES: {
      const bytes = await cursor.readBytes(null);
      const text = new TextDecoder().decode(bytes);
      return `"${text}"`;
    }
    case Tag.UINT:
      return `${cursor.readUint()}n`;
    case Tag.INT:
      return `${cursor.readInt()}n`;
    case Tag.FLOAT:
      return `${cursor.readFloat()}`;
    default:
      return `<key tag: ${tag}>`;
  }
}

async function printValue(cursor: ReadCursor, indent: string): Promise<void> {
  const tag = cursor.slotPtr.slot.tag;

  switch (tag) {
    case Tag.NONE:
      console.log(`${indent}(none)`);
      break;

    case Tag.ARRAY_LIST: {
      const list = new ReadArrayList(cursor);
      const count = await list.count();
      console.log(`${indent}ArrayList[${count}]:`);
      if (indent == '') {
        const itemCursor = await list.getCursor(count - 1n);
        if (itemCursor) {
          await printValue(itemCursor, indent + '    ');
        }
      } else {
        for (let i = 0n; i < count; i++) {
          const itemCursor = await list.getCursor(i);
          if (itemCursor) {
            console.log(`${indent}  [${i}]:`);
            await printValue(itemCursor, indent + '    ');
          }
        }
      }
      break;
    }

    case Tag.HASH_MAP:
    case Tag.COUNTED_HASH_MAP: {
      const iter = cursor.iterator();
      await iter.init();
      const entries: Array<{ key: string; valueCursor: ReadCursor }> = [];

      while (await iter.hasNext()) {
        const kvPairCursor = await iter.next();
        if (kvPairCursor) {
          const kvPair = await kvPairCursor.readKeyValuePair();
          const key = await formatKey(kvPair.keyCursor);
          entries.push({ key, valueCursor: kvPair.valueCursor });
        }
      }

      const prefix = tag === Tag.COUNTED_HASH_MAP ? 'CountedHashMap' : 'HashMap';
      console.log(`${indent}${prefix}{${entries.length}}:`);

      for (const entry of entries) {
        console.log(`${indent}  ${entry.key}:`);
        await printValue(entry.valueCursor, indent + '    ');
      }
      break;
    }

    case Tag.HASH_SET:
    case Tag.COUNTED_HASH_SET: {
      const iter = cursor.iterator();
      await iter.init();
      const keys: string[] = [];

      while (await iter.hasNext()) {
        const kvPairCursor = await iter.next();
        if (kvPairCursor) {
          const kvPair = await kvPairCursor.readKeyValuePair();
          const key = await formatKey(kvPair.keyCursor);
          keys.push(key);
        }
      }

      const prefix = tag === Tag.COUNTED_HASH_SET ? 'CountedHashSet' : 'HashSet';
      console.log(`${indent}${prefix}{${keys.length}}: [${keys.join(', ')}]`);
      break;
    }

    case Tag.LINKED_ARRAY_LIST: {
      const count = await cursor.count();
      console.log(`${indent}LinkedArrayList[${count}]:`);
      const iter = cursor.iterator();
      await iter.init();
      let i = 0;
      while (await iter.hasNext()) {
        const itemCursor = await iter.next();
        if (itemCursor) {
          console.log(`${indent}  [${i}]:`);
          await printValue(itemCursor, indent + '    ');
        }
        i++;
      }
      break;
    }

    case Tag.BYTES:
    case Tag.SHORT_BYTES: {
      const bytesObj = await cursor.readBytesObject(null);
      const text = new TextDecoder().decode(bytesObj.value);
      const isPrintable = /^[\x20-\x7E\n\r\t]*$/.test(text);

      if (isPrintable && text.length <= 100) {
        if (bytesObj.formatTag) {
          const formatTag = new TextDecoder().decode(bytesObj.formatTag);
          console.log(`${indent}"${text}" (format: ${formatTag})`);
        } else {
          console.log(`${indent}"${text}"`);
        }
      } else {
        const preview = bytesObj.value.slice(0, 16);
        const hex = Array.from(preview).map(b => b.toString(16).padStart(2, '0')).join(' ');
        if (bytesObj.formatTag) {
          const formatTag = new TextDecoder().decode(bytesObj.formatTag);
          console.log(`${indent}<${bytesObj.value.length} bytes: ${hex}...> (format: ${formatTag})`);
        } else {
          console.log(`${indent}<${bytesObj.value.length} bytes: ${hex}...>`);
        }
      }
      break;
    }

    case Tag.UINT: {
      const value = cursor.readUint();
      console.log(`${indent}${value}n (uint)`);
      break;
    }

    case Tag.INT: {
      const value = cursor.readInt();
      console.log(`${indent}${value}n (int)`);
      break;
    }

    case Tag.FLOAT: {
      const value = cursor.readFloat();
      console.log(`${indent}${value} (float)`);
      break;
    }

    default:
      console.log(`${indent}<unknown tag: ${tag}>`);
  }
}

async function main() {
  const args = process.argv.slice(2);

  if (args.length < 1) {
    console.error('Usage: bun run dump.ts <database-file>');
    process.exit(1);
  }

  const filePath = args[0];

  try {
    const core = await CoreBufferedFile.create(filePath);
    const hasher = new Hasher('SHA-1');
    const db = await Database.create(core, hasher);

    console.log(`Database: ${filePath}`);
    console.log('---');

    const rootCursor = await db.rootCursor();
    await printValue(rootCursor, '');

  } catch (error) {
    console.error(`Error reading database: ${error}`);
    process.exit(1);
  }
}

main();
