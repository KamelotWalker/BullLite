/** Double-ended queue backed by a ring buffer. O(1) amortised push/pop at both ends. */
export class Deque<T> {
  private buf: (T | undefined)[] = new Array(16);
  private head = 0;
  private size = 0;

  get length(): number {
    return this.size;
  }

  push(value: T): void {
    if (this.size === this.buf.length) this.grow();
    this.buf[(this.head + this.size) & (this.buf.length - 1)] = value;
    this.size++;
  }

  unshift(value: T): void {
    if (this.size === this.buf.length) this.grow();
    this.head = (this.head - 1) & (this.buf.length - 1);
    this.buf[this.head] = value;
    this.size++;
  }

  shift(): T | undefined {
    if (this.size === 0) return undefined;
    const value = this.buf[this.head];
    this.buf[this.head] = undefined;
    this.head = (this.head + 1) & (this.buf.length - 1);
    this.size--;
    return value;
  }

  peek(): T | undefined {
    return this.size === 0 ? undefined : this.buf[this.head];
  }

  *[Symbol.iterator](): IterableIterator<T> {
    for (let i = 0; i < this.size; i++) yield this.buf[(this.head + i) & (this.buf.length - 1)]!;
  }

  /** Keeps only the elements for which `keep` returns true, preserving order. */
  filterInPlace(keep: (value: T) => boolean): void {
    const kept: T[] = [];
    for (const v of this) if (keep(v)) kept.push(v);
    this.clear();
    for (const v of kept) this.push(v);
  }

  clear(): void {
    this.buf = new Array(16);
    this.head = 0;
    this.size = 0;
  }

  private grow(): void {
    const next: (T | undefined)[] = new Array(this.buf.length * 2);
    for (let i = 0; i < this.size; i++) next[i] = this.buf[(this.head + i) & (this.buf.length - 1)];
    this.buf = next;
    this.head = 0;
  }
}

/** Binary min-heap ordered by `compare`. */
export class Heap<T> {
  private items: T[] = [];

  private readonly compare: (a: T, b: T) => number;

  constructor(compare: (a: T, b: T) => number) {
    this.compare = compare;
  }

  get length(): number {
    return this.items.length;
  }

  push(value: T): void {
    const items = this.items;
    items.push(value);
    let i = items.length - 1;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (this.compare(items[i]!, items[parent]!) >= 0) break;
      [items[i], items[parent]] = [items[parent]!, items[i]!];
      i = parent;
    }
  }

  peek(): T | undefined {
    return this.items[0];
  }

  pop(): T | undefined {
    const items = this.items;
    if (items.length === 0) return undefined;
    const top = items[0];
    const last = items.pop()!;
    if (items.length > 0) {
      items[0] = last;
      let i = 0;
      for (;;) {
        const l = 2 * i + 1;
        const r = l + 1;
        let min = i;
        if (l < items.length && this.compare(items[l]!, items[min]!) < 0) min = l;
        if (r < items.length && this.compare(items[r]!, items[min]!) < 0) min = r;
        if (min === i) break;
        [items[i], items[min]] = [items[min]!, items[i]!];
        i = min;
      }
    }
    return top;
  }

  /** Keeps only the elements for which `keep` returns true and restores the heap property. */
  filterInPlace(keep: (value: T) => boolean): void {
    const kept = this.items.filter(keep);
    this.items = [];
    for (const v of kept) this.push(v);
  }

  clear(): void {
    this.items = [];
  }
}
