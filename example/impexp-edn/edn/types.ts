export type EdnValue =
  | { type: 'nil' }
  | { type: 'boolean'; value: boolean }
  | { type: 'integer'; value: bigint }
  | { type: 'float'; value: number }
  | { type: 'string'; value: string }
  | { type: 'keyword'; namespace?: string; name: string }
  | { type: 'symbol'; namespace?: string; name: string }
  | { type: 'vector'; elements: EdnValue[] }
  | { type: 'list'; elements: EdnValue[] }
  | { type: 'set'; elements: EdnValue[] }
  | { type: 'map'; entries: [EdnValue, EdnValue][] };

export function ednNil(): EdnValue {
  return { type: 'nil' };
}

export function ednBoolean(value: boolean): EdnValue {
  return { type: 'boolean', value };
}

export function ednInteger(value: bigint): EdnValue {
  return { type: 'integer', value };
}

export function ednFloat(value: number): EdnValue {
  return { type: 'float', value };
}

export function ednString(value: string): EdnValue {
  return { type: 'string', value };
}

export function ednKeyword(name: string, namespace?: string): EdnValue {
  return namespace ? { type: 'keyword', namespace, name } : { type: 'keyword', name };
}

export function ednSymbol(name: string, namespace?: string): EdnValue {
  return namespace ? { type: 'symbol', namespace, name } : { type: 'symbol', name };
}

export function ednVector(elements: EdnValue[]): EdnValue {
  return { type: 'vector', elements };
}

export function ednList(elements: EdnValue[]): EdnValue {
  return { type: 'list', elements };
}

export function ednSet(elements: EdnValue[]): EdnValue {
  return { type: 'set', elements };
}

export function ednMap(entries: [EdnValue, EdnValue][]): EdnValue {
  return { type: 'map', entries };
}
