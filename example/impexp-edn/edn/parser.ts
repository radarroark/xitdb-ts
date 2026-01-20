import {
  EdnValue,
  ednNil,
  ednBoolean,
  ednInteger,
  ednFloat,
  ednString,
  ednKeyword,
  ednSymbol,
  ednVector,
  ednList,
  ednSet,
  ednMap,
} from './types';

type Token =
  | { type: 'lparen' }
  | { type: 'rparen' }
  | { type: 'lbracket' }
  | { type: 'rbracket' }
  | { type: 'lbrace' }
  | { type: 'rbrace' }
  | { type: 'hash_lbrace' }
  | { type: 'discard' }
  | { type: 'nil' }
  | { type: 'boolean'; value: boolean }
  | { type: 'integer'; value: bigint }
  | { type: 'float'; value: number }
  | { type: 'string'; value: string }
  | { type: 'keyword'; namespace?: string; name: string }
  | { type: 'symbol'; namespace?: string; name: string };

class Tokenizer {
  private input: string;
  private pos: number = 0;

  constructor(input: string) {
    this.input = input;
  }

  private peek(): string | null {
    if (this.pos >= this.input.length) return null;
    return this.input[this.pos];
  }

  private advance(): string {
    return this.input[this.pos++];
  }

  private skipWhitespaceAndComments(): void {
    while (this.pos < this.input.length) {
      const ch = this.peek();
      // Whitespace and comma (commas are whitespace in EDN)
      if (ch === ' ' || ch === '\t' || ch === '\n' || ch === '\r' || ch === ',') {
        this.advance();
        continue;
      }
      // Comments
      if (ch === ';') {
        while (this.pos < this.input.length && this.peek() !== '\n') {
          this.advance();
        }
        continue;
      }
      break;
    }
  }

  private isSymbolChar(ch: string): boolean {
    return /[a-zA-Z0-9.*+!\-_?$%&=<>:#/]/.test(ch);
  }

  private isDigit(ch: string): boolean {
    return ch >= '0' && ch <= '9';
  }

  private readString(): string {
    this.advance(); // skip opening quote
    let result = '';
    while (this.pos < this.input.length) {
      const ch = this.advance();
      if (ch === '"') {
        return result;
      }
      if (ch === '\\') {
        const next = this.advance();
        switch (next) {
          case 'n': result += '\n'; break;
          case 'r': result += '\r'; break;
          case 't': result += '\t'; break;
          case '\\': result += '\\'; break;
          case '"': result += '"'; break;
          default: result += next;
        }
      } else {
        result += ch;
      }
    }
    throw new Error('Unterminated string');
  }

  private readNumber(): Token {
    let numStr = '';
    let hasDecimal = false;
    let hasExponent = false;

    // Handle sign
    if (this.peek() === '-' || this.peek() === '+') {
      numStr += this.advance();
    }

    while (this.pos < this.input.length) {
      const ch = this.peek();
      if (this.isDigit(ch!)) {
        numStr += this.advance();
      } else if (ch === '.' && !hasDecimal && !hasExponent) {
        hasDecimal = true;
        numStr += this.advance();
      } else if ((ch === 'e' || ch === 'E') && !hasExponent) {
        hasExponent = true;
        hasDecimal = true; // exponent implies float
        numStr += this.advance();
        if (this.peek() === '-' || this.peek() === '+') {
          numStr += this.advance();
        }
      } else if (ch === 'N' || ch === 'M') {
        // BigInt or BigDecimal suffix - skip it
        this.advance();
        break;
      } else {
        break;
      }
    }

    if (hasDecimal || hasExponent) {
      return { type: 'float', value: parseFloat(numStr) };
    }
    return { type: 'integer', value: BigInt(numStr) };
  }

  private readSymbolOrKeyword(isKeyword: boolean): Token {
    if (isKeyword) {
      this.advance(); // skip ':'
    }

    let name = '';
    while (this.pos < this.input.length && this.isSymbolChar(this.peek()!)) {
      name += this.advance();
    }

    // Check for namespace
    const slashIdx = name.indexOf('/');
    if (slashIdx > 0 && slashIdx < name.length - 1) {
      const namespace = name.substring(0, slashIdx);
      const localName = name.substring(slashIdx + 1);
      if (isKeyword) {
        return { type: 'keyword', namespace, name: localName };
      }
      return { type: 'symbol', namespace, name: localName };
    }

    if (isKeyword) {
      return { type: 'keyword', name };
    }

    // Check for special symbols
    if (name === 'nil') return { type: 'nil' };
    if (name === 'true') return { type: 'boolean', value: true };
    if (name === 'false') return { type: 'boolean', value: false };

    return { type: 'symbol', name };
  }

  nextToken(): Token | null {
    this.skipWhitespaceAndComments();
    if (this.pos >= this.input.length) return null;

    const ch = this.peek()!;

    switch (ch) {
      case '(': this.advance(); return { type: 'lparen' };
      case ')': this.advance(); return { type: 'rparen' };
      case '[': this.advance(); return { type: 'lbracket' };
      case ']': this.advance(); return { type: 'rbracket' };
      case '{': this.advance(); return { type: 'lbrace' };
      case '}': this.advance(); return { type: 'rbrace' };
      case '"': return { type: 'string', value: this.readString() };
      case ':': return this.readSymbolOrKeyword(true);
      case '#': {
        this.advance();
        const next = this.peek();
        if (next === '{') {
          this.advance();
          return { type: 'hash_lbrace' };
        }
        if (next === '_') {
          this.advance();
          return { type: 'discard' };
        }
        throw new Error(`Unexpected character after #: ${next}`);
      }
    }

    // Numbers
    if (this.isDigit(ch) || ((ch === '-' || ch === '+') && this.isDigit(this.input[this.pos + 1]))) {
      return this.readNumber();
    }

    // Symbols
    if (this.isSymbolChar(ch)) {
      return this.readSymbolOrKeyword(false);
    }

    throw new Error(`Unexpected character: ${ch}`);
  }
}

class Parser {
  private tokenizer: Tokenizer;
  private currentToken: Token | null = null;

  constructor(input: string) {
    this.tokenizer = new Tokenizer(input);
    this.advance();
  }

  private advance(): void {
    this.currentToken = this.tokenizer.nextToken();
  }

  private expect(type: Token['type']): void {
    if (!this.currentToken || this.currentToken.type !== type) {
      throw new Error(`Expected ${type}, got ${this.currentToken?.type ?? 'EOF'}`);
    }
    this.advance();
  }

  parse(): EdnValue {
    const value = this.parseValue();
    if (this.currentToken !== null) {
      throw new Error(`Unexpected token after value: ${this.currentToken.type}`);
    }
    return value;
  }

  private parseValue(): EdnValue {
    if (!this.currentToken) {
      throw new Error('Unexpected end of input');
    }

    const token = this.currentToken;

    switch (token.type) {
      case 'nil':
        this.advance();
        return ednNil();

      case 'boolean':
        this.advance();
        return ednBoolean(token.value);

      case 'integer':
        this.advance();
        return ednInteger(token.value);

      case 'float':
        this.advance();
        return ednFloat(token.value);

      case 'string':
        this.advance();
        return ednString(token.value);

      case 'keyword':
        this.advance();
        return ednKeyword(token.name, token.namespace);

      case 'symbol':
        this.advance();
        return ednSymbol(token.name, token.namespace);

      case 'lbracket':
        return this.parseVector();

      case 'lparen':
        return this.parseList();

      case 'hash_lbrace':
        return this.parseSet();

      case 'lbrace':
        return this.parseMap();

      case 'discard':
        this.advance();
        this.parseValue(); // discard the next value
        return this.parseValue();

      default:
        throw new Error(`Unexpected token: ${token.type}`);
    }
  }

  private parseVector(): EdnValue {
    this.advance(); // skip [
    const elements: EdnValue[] = [];
    while (this.currentToken && this.currentToken.type !== 'rbracket') {
      if (this.currentToken.type === 'discard') {
        this.advance();
        this.parseValue(); // discard
        continue;
      }
      elements.push(this.parseValue());
    }
    this.expect('rbracket');
    return ednVector(elements);
  }

  private parseList(): EdnValue {
    this.advance(); // skip (
    const elements: EdnValue[] = [];
    while (this.currentToken && this.currentToken.type !== 'rparen') {
      if (this.currentToken.type === 'discard') {
        this.advance();
        this.parseValue(); // discard
        continue;
      }
      elements.push(this.parseValue());
    }
    this.expect('rparen');
    return ednList(elements);
  }

  private parseSet(): EdnValue {
    this.advance(); // skip #{
    const elements: EdnValue[] = [];
    while (this.currentToken && this.currentToken.type !== 'rbrace') {
      if (this.currentToken.type === 'discard') {
        this.advance();
        this.parseValue(); // discard
        continue;
      }
      elements.push(this.parseValue());
    }
    this.expect('rbrace');
    return ednSet(elements);
  }

  private parseMap(): EdnValue {
    this.advance(); // skip {
    const entries: [EdnValue, EdnValue][] = [];
    while (this.currentToken && this.currentToken.type !== 'rbrace') {
      if (this.currentToken.type === 'discard') {
        this.advance();
        this.parseValue(); // discard
        continue;
      }
      const key = this.parseValue();
      // After parseValue, currentToken has changed - cast to reset narrowing
      const tokenAfterKey = this.currentToken as Token | null;
      if (!tokenAfterKey || tokenAfterKey.type === 'rbrace') {
        throw new Error('Map must have even number of elements');
      }
      if (tokenAfterKey.type === 'discard') {
        this.advance();
        this.parseValue(); // discard the value
        continue;
      }
      const value = this.parseValue();
      entries.push([key, value]);
    }
    this.expect('rbrace');
    return ednMap(entries);
  }
}

export function parseEdn(input: string): EdnValue {
  const parser = new Parser(input);
  return parser.parse();
}
