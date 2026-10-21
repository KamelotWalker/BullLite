import type { JobJson } from 'bullmq';

export type MemoryJobState = 'wait' | 'active' | 'completed' | 'failed';

export interface StoredJob {
  json: JobJson;
  state: MemoryJobState;
  /** Present only while `state === 'active'`. */
  lock?: { token: string; until: number };
}

/**
 * All state of one `(namespace, queueName)` pair. Every mutation of a store is
 * performed by synchronous methods of the backend (no `await` between the
 * check and the mutation), which is what makes the operations atomic on the
 * single-threaded event loop.
 */
export class QueueStore {
  readonly jobs = new Map<string, StoredJob>();
  readonly meta = new Map<string, string>();
  /** FIFO of waiting job ids. `waitHead` avoids O(n) `Array#shift`. */
  private wait: string[] = [];
  private waitHead = 0;
  private idCounter = 0;
  private readonly subscribers = new Set<() => void>();

  pushWait(id: string): void {
    this.wait.push(id);
  }

  /** Removes and returns the oldest waiting id, skipping ids that were deleted. */
  shiftWait(): string | undefined {
    while (this.waitHead < this.wait.length) {
      const id = this.wait[this.waitHead++]!;
      if (this.jobs.get(id)?.state === 'wait') {
        if (this.waitHead > 1024 && this.waitHead * 2 > this.wait.length) {
          this.wait = this.wait.slice(this.waitHead);
          this.waitHead = 0;
        }
        return id;
      }
    }
    this.wait = [];
    this.waitHead = 0;
    return undefined;
  }

  nextId(): string {
    let id: string;
    do id = String(++this.idCounter);
    while (this.jobs.has(id));
    return id;
  }

  /** Registers a callback fired whenever a job may have become claimable. */
  subscribe(fn: () => void): () => void {
    this.subscribers.add(fn);
    return () => void this.subscribers.delete(fn);
  }

  notify(): void {
    for (const fn of this.subscribers) fn();
  }
}

const stores = new Map<string, QueueStore>();

/** Store shared by every backend instance created for the same namespace + queue. */
export function getStore(namespace: string, queueName: string): QueueStore {
  const key = `${namespace}\u0000${queueName}`;
  let store = stores.get(key);
  if (!store) {
    store = new QueueStore();
    stores.set(key, store);
  }
  return store;
}

/** Drops every store of `namespace` (all namespaces when omitted). Test/teardown helper. */
export function resetMemoryStores(namespace?: string): void {
  if (namespace === undefined) return stores.clear();
  const prefix = `${namespace}\u0000`;
  for (const key of stores.keys()) if (key.startsWith(prefix)) stores.delete(key);
}
