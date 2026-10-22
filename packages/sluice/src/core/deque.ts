/** Growable ring buffer with O(1) push/shift at both ends. */
export class Deque<T> {
  private buf: (T | undefined)[];
  private head = 0;
  private len = 0;

  constructor(capacity = 16) {
    this.buf = new Array(capacity);
  }

  get length(): number {
    return this.len;
  }

  push(value: T): void {
    if (this.len === this.buf.length) this.grow();
    this.buf[(this.head + this.len) % this.buf.length] = value;
    this.len++;
  }

  unshift(value: T): void {
    if (this.len === this.buf.length) this.grow();
    this.head = (this.head - 1 + this.buf.length) % this.buf.length;
    this.buf[this.head] = value;
    this.len++;
  }

  shift(): T | undefined {
    if (this.len === 0) return undefined;
    const value = this.buf[this.head];
    this.buf[this.head] = undefined;
    this.head = (this.head + 1) % this.buf.length;
    this.len--;
    return value;
  }

  pop(): T | undefined {
    if (this.len === 0) return undefined;
    const idx = (this.head + this.len - 1) % this.buf.length;
    const value = this.buf[idx];
    this.buf[idx] = undefined;
    this.len--;
    return value;
  }

  peek(): T | undefined {
    return this.len === 0 ? undefined : this.buf[this.head];
  }

  /** Remove the first element matching the predicate. O(n); used only for cancellation. */
  remove(pred: (value: T) => boolean): T | undefined {
    for (let i = 0; i < this.len; i++) {
      const idx = (this.head + i) % this.buf.length;
      const value = this.buf[idx] as T;
      if (pred(value)) {
        for (let j = i; j < this.len - 1; j++) {
          this.buf[(this.head + j) % this.buf.length] = this.buf[(this.head + j + 1) % this.buf.length];
        }
        this.buf[(this.head + this.len - 1) % this.buf.length] = undefined;
        this.len--;
        return value;
      }
    }
    return undefined;
  }

  *[Symbol.iterator](): IterableIterator<T> {
    for (let i = 0; i < this.len; i++) yield this.buf[(this.head + i) % this.buf.length] as T;
  }

  private grow(): void {
    const next = new Array<T | undefined>(this.buf.length * 2);
    for (let i = 0; i < this.len; i++) next[i] = this.buf[(this.head + i) % this.buf.length];
    this.buf = next;
    this.head = 0;
  }
}
