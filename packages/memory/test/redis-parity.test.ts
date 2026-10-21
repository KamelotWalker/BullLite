// Regression tests for Redis (bullmq@6.4.0 Lua) parity gaps found in the cross-family review.
// Each test cites the reference script it mirrors.
import { describe, expect, it, vi } from 'vitest';
import { createMemoryBackend } from '../src/index.ts';
import { freshNs, Queue, track, Worker } from './helpers.ts';

function backend(connection = freshNs()) {
  const opts = { connection, lockDuration: 100, stalledInterval: 1 };
  return track(createMemoryBackend('q', opts));
}

describe('Redis v6.4.0 parity', () => {
  it('does not resurrect an expired lock (extendLock-2.lua)', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const b = backend(connection);
    const job = await queue.add('j', {});
    await b.moveToActive('old');
    const clock = vi.spyOn(Date, 'now').mockReturnValue(Date.now() + 1000);
    try { expect(await b.extendLock(job.id!, 'old', 100)).toBe(0); }
    finally { clock.mockRestore(); }
  });

  it('rejects stale-token moveToWait after recovery and re-claim (moveJobFromActiveToWait-9.lua removeLock)', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const b = backend(connection);
    const job = await queue.add('j', {});
    await b.moveToActive('old');
    await b.moveStalledJobsToWait();
    const clock = vi.spyOn(Date, 'now').mockReturnValue(Date.now() + 1000);
    try {
      await b.moveStalledJobsToWait();
      await b.moveToActive('new');
      await expect(b.moveJobFromActiveToWait(job.id!, 'old')).rejects.toThrow(/Lock mismatch/);
    } finally { clock.mockRestore(); }
  });

  it('returns a recovered stalled job to the head, ahead of existing waiting jobs (moveJobToWait RPUSH)', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const b = backend(connection);
    const job = await queue.add('stalled', {});
    await b.moveToActive('old');
    await queue.add('waiting', {});
    await b.moveStalledJobsToWait();
    const clock = vi.spyOn(Date, 'now').mockReturnValue(Date.now() + 1000);
    try {
      await b.moveStalledJobsToWait();
      expect((await b.moveToActive('new'))[1]).toBe(job.id);
    } finally { clock.mockRestore(); }
  });

  it('emits waiting before stalled on recovery (moveStalledJobsToWait-9.lua)', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const b = backend(connection);
    await queue.add('j', {});
    await b.moveToActive('old');
    const before = await b.readEvents('0-0', 1);
    const cursor = before![0]![1].at(-1)![0];
    await b.moveStalledJobsToWait();
    const clock = vi.spyOn(Date, 'now').mockReturnValue(Date.now() + 1000);
    try {
      await b.moveStalledJobsToWait();
      const result = await b.readEvents(cursor, 1);
      expect(result![0]![1].map((entry) => entry[1][1])).toEqual(['waiting', 'stalled']);
    } finally { clock.mockRestore(); }
  });

  it('orders multiple priority-front jobs lexicographically (pushBackJobWithPriority.lua)', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const worker = track(new Worker('q', null, { connection, autorun: false }));
    const a = await queue.add('a', {}, { jobId: 'a', priority: 1 });
    const b = await queue.add('b', {}, { jobId: 'b', priority: 1 });
    await a.changePriority({ priority: 1, lifo: true });
    await b.changePriority({ priority: 1, lifo: true });
    expect((await worker.getNextJob('t'))!.id).toBe('a');
  });

  it('keeps recently retried delayed jobs within the clean grace (cleanJobsInSet-3.lua processedOn)', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const b = backend(connection);
    const job = await queue.add('j', {}, { timestamp: Date.now() - 60_000 });
    await b.moveToActive('t');
    await b.moveToDelayed(job.id!, Date.now(), 60_000, 't');
    expect(await queue.clean(10_000, 0, 'delayed')).toEqual([]);
    expect(await queue.getJob(job.id!)).toBeDefined();
  });

  it("treats '$' as the stream tail at read time (XREAD)", async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const b = backend(connection);
    await b.waitUntilReady();
    await queue.add('past', {});
    expect(await b.readEvents('$', 10)).toBeNull();
  });
});
