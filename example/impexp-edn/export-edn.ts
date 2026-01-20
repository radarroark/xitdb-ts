import {
  Database,
  Hasher,
  CoreBufferedFile,
  ReadArrayList,
} from 'xitdb';
import { cursorToEdn } from './edn/serializer';

export async function exportEdn(dbPath: string): Promise<void> {
  const core = await CoreBufferedFile.create(dbPath);
  const db = await Database.create(core, new Hasher('SHA-1'));
  const rootCursor = await db.rootCursor();

  const history = new ReadArrayList(rootCursor);
  const count = await history.count();

  if (count === 0n) {
    console.log('{}');
    return;
  }

  const latest = await history.getCursor(count - 1n);
  if (!latest) {
    console.log('{}');
    return;
  }

  const ednString = await cursorToEdn(latest);
  console.log(ednString);
}
