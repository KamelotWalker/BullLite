import { randomUUID } from 'node:crypto';
import { afterEach } from 'vitest';
import { withBackend, type BackendFactory, type IQueueBackend } from 'bullmq';
import { createMemoryBackend, type MemoryConnectionOptions } from '../src/index.ts';

export const { Queue, Worker, QueueEvents, QueueEventsProducer, FlowProducer } = withBackend(createMemoryBackend);

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

type Closable = { close(): Promise<void> };
const open: Closable[] = [];

/** Registers an instance to be closed after the current test (in reverse creation order). */
export const track = <T extends Closable>(c: T): T => (open.push(c), c);

afterEach(async () => {
  for (const c of open.splice(0).reverse()) await c.close();
});

/** A connection with a namespace no other test uses. */
export const freshNs = (): MemoryConnectionOptions => ({ namespace: `t-${randomUUID()}` });

/** Resolves when `cond()` becomes true; checks every 5 ms, fails after `timeoutMs`. */
export async function until(cond: () => boolean | Promise<boolean>, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await cond())) {
    if (Date.now() > deadline) throw new Error(`condition not met within ${timeoutMs} ms`);
    await sleep(5);
  }
}

export interface Call {
  instance: 'queue' | 'worker' | 'events';
  name: string;
}

/** Wraps a backend factory so every member call is appended to `log` (in call order). */
export function recordingFactory(
  inner: BackendFactory<IQueueBackend, MemoryConnectionOptions>,
  log: Call[],
): BackendFactory<IQueueBackend, MemoryConnectionOptions> {
  return (name, opts, flags) => {
    const real = inner(name, opts, flags);
    const instance = flags?.withBlockingConnection ? 'worker' : flags?.blocking ? 'events' : 'queue';
    return new Proxy(real, {
      get(target, prop, receiver) {
        const value = Reflect.get(target, prop, receiver);
        if (typeof prop !== 'string' || typeof value !== 'function') return value;
        return (...args: unknown[]) => {
          log.push({ instance, name: prop });
          return value.apply(target, args);
        };
      },
    });
  };
}
