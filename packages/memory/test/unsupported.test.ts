import { describe, expect, it } from 'vitest';
import type { IQueueBackend } from 'bullmq';
import { createMemoryBackend, UnsupportedError } from '../src/index.ts';
import { FlowProducer, freshNs, Queue, track, Worker } from './helpers.ts';

describe('unsupported features fail loudly (never silent success)', () => {
  it('job options that need flows, deduplication or schedulers are rejected and nothing is stored', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    await expect(queue.add('c', {}, { parent: { id: '1', queue: 'bull:p' } })).rejects.toThrow(/flows/);
    await expect(queue.add('d', {}, { deduplication: { id: 'x' } })).rejects.toThrow(/deduplication/);
    await expect(
      queue.addBulk([{ name: 'ok', data: {} }, { name: 'dedup', data: {}, opts: { deduplication: { id: 'y' } } }]),
    ).rejects.toThrow(/deduplication/);
    expect(await queue.getJobCounts()).toMatchObject({ waiting: 0, delayed: 0, prioritized: 0 }); // addBulk is all-or-nothing
  });

  it('FlowProducer, job schedulers, rate limiting, metrics and deduplication getters reject', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const flow = track(new FlowProducer({ connection }));
    await expect(flow.add({ name: 'parent', queueName: 'q', children: [{ name: 'c', queueName: 'q' }] })).rejects.toThrow(
      UnsupportedError,
    );
    await expect(queue.upsertJobScheduler('s', { every: 1000 })).rejects.toThrow(UnsupportedError);
    await expect(queue.getJobSchedulers()).rejects.toThrow(/getJobSchedulersRange/);
    await expect(queue.setGlobalRateLimit(1, 1000)).rejects.toThrow(/rate limiting/);
    await expect(queue.rateLimit(1000)).rejects.toThrow(/setRateLimit/);
    await expect(queue.getMetrics('completed')).rejects.toThrow(/getMetrics/);
    await expect(queue.getDeduplicationJobId('x')).rejects.toThrow(/getDeduplicationJobId/);
    expect(await queue.getGlobalRateLimit()).toBeNull(); // nothing was stored by the rejected call
  });

  it('Worker options that need unsupported features throw at construction', () => {
    const connection = freshNs();
    expect(() => new Worker('q', async () => undefined, { connection, limiter: { max: 1, duration: 1000 } })).toThrow(
      /limiter/,
    );
    expect(() => new Worker('q', async () => undefined, { connection, metrics: { maxDataPoints: 10 } })).toThrow(
      /metrics/,
    );
  });

  it('every unsupported backend member rejects with UnsupportedError naming the member', async () => {
    const backend = createMemoryBackend('q', { connection: freshNs() });
    const names = [
      'addFlow', 'moveToWaitingChildren', 'removeUnprocessedChildren', 'removeChildDependency',
      'getDependencyCounts', 'getDependencies', 'getProcessedChildrenValues', 'getIgnoredChildrenFailures',
      'paginate', 'addJobScheduler', 'updateJobSchedulerNextMillis', 'removeJobScheduler', 'getJobScheduler',
      'isJobScheduler', 'getJobSchedulerData', 'getJobSchedulersRange', 'getJobSchedulersCount',
      'removeDeduplicationKey', 'deleteDeduplicationKey', 'getDeduplicationJobId', 'setRateLimit', 'getMetrics',
    ] as const;
    for (const name of names) {
      const call = (backend[name] as (...args: unknown[]) => Promise<unknown>).bind(backend);
      const err = await call().catch((e: unknown) => e);
      expect(err, name).toBeInstanceOf(UnsupportedError);
      expect((err as UnsupportedError).method).toBe(name);
    }
    expect(() => (backend as IQueueBackend).forQueue('other')).toThrow(UnsupportedError);
    await backend.close();
  });
});
