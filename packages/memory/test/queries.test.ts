import { once } from 'node:events';
import { describe, expect, it } from 'vitest';
import { freshNs, Queue, track, until, Worker } from './helpers.ts';

describe('job queries', () => {
  it('getJob returns undefined for unknown ids; getJobState returns unknown', async () => {
    const queue = track(new Queue('q', { connection: freshNs() }));
    expect(await queue.getJob('nope')).toBeUndefined();
    expect(await queue.getJobState('nope')).toBe('unknown');
  });

  it('getJobCounts / count / state getters across every state', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const worker = track(new Worker('q', null, { connection, autorun: false }));
    await queue.add('w1', {});
    await queue.add('w2', {});
    await queue.add('p', {}, { priority: 5 });
    await queue.add('d', {}, { delay: 60_000 });
    const a = (await worker.getNextJob('t1'))!; // w1
    const c = (await worker.getNextJob('t2'))!; // w2
    await c.moveToCompleted('r', 't2', false);
    const f = (await worker.getNextJob('t3'))!; // p
    await f.moveToFailed(new Error('bad'), 't3');

    expect(await queue.getJobCounts()).toEqual({
      active: 1, completed: 1, delayed: 1, failed: 1, prioritized: 0, waiting: 0, 'waiting-children': 0,
    });
    expect(await queue.count()).toBe(1); // waiting + delayed + prioritized + waiting-children
    expect(await queue.getActiveCount()).toBe(1);
    expect(await queue.getCompletedCount()).toBe(1);
    expect(await queue.getFailedCount()).toBe(1);
    expect(await queue.getDelayedCount()).toBe(1);

    expect((await queue.getActive()).map((j) => j.id)).toEqual([a.id]);
    expect((await queue.getCompleted()).map((j) => j.name)).toEqual(['w2']);
    expect((await queue.getFailed()).map((j) => j.name)).toEqual(['p']);
    expect((await queue.getDelayed()).map((j) => j.name)).toEqual(['d']);
    expect(await a.isActive()).toBe(true);
    expect(await c.isCompleted()).toBe(true);
    expect(await f.isFailed()).toBe(true);
    expect(await (await queue.getDelayed())[0]!.isDelayed()).toBe(true);
  });

  it('getJobs ordering mirrors Redis: waiting newest-first by default, FIFO with asc', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    for (const name of ['a', 'b', 'c', 'd']) await queue.add(name, {});
    const names = async (...args: Parameters<typeof queue.getJobs>) => (await queue.getJobs(...args)).map((j) => j.name);
    expect(await names(['waiting'])).toEqual(['d', 'c', 'b', 'a']);
    expect(await names(['waiting'], 0, -1, true)).toEqual(['a', 'b', 'c', 'd']);
    expect(await names(['waiting'], 0, 1, true)).toEqual(['a', 'b']);
    expect(await names(['waiting'], 0, 1, false)).toEqual(['d', 'c']);
    expect((await queue.getWaiting()).map((j) => j.name)).toEqual(['a', 'b', 'c', 'd']);
    expect(await queue.getWaitingCount()).toBe(4);
  });

  it('getJobs over finished sets is ordered by finishedOn (newest first unless asc)', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const seen: string[] = [];
    track(new Worker('q', async (job) => { seen.push(job.name); }, { connection }));
    for (const name of ['a', 'b', 'c']) await queue.add(name, {});
    await until(() => seen.length === 3);
    await until(async () => (await queue.getCompletedCount()) === 3);
    expect((await queue.getJobs('completed')).map((j) => j.name)).toEqual(['c', 'b', 'a']);
    expect((await queue.getJobs('completed', 0, -1, true)).map((j) => j.name)).toEqual(['a', 'b', 'c']);
  });

  it('progress, data updates and logs are stored and readable', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const worker = track(
      new Worker('q', async (job) => {
        await job.updateProgress({ pct: 50 });
        await job.log('step 1');
        await job.log('step 2');
        await job.updateData({ ...job.data, touched: true });
        return 'done';
      }, { connection }),
    );
    const progress = once(worker, 'progress');
    const added = await queue.add('j', { v: 1 });
    expect((await progress)[1]).toEqual({ pct: 50 });
    await once(worker, 'completed');
    const job = (await queue.getJob(added.id!))!;
    expect(job.progress).toEqual({ pct: 50 });
    expect(job.data).toEqual({ v: 1, touched: true });
    expect(await queue.getJobLogs(added.id!)).toEqual({ logs: ['step 1', 'step 2'], count: 2 });
    expect(await queue.getJobLogs(added.id!, 0, 0, false)).toEqual({ logs: ['step 2'], count: 2 });
    await job.clearLogs();
    expect(await queue.getJobLogs(added.id!)).toEqual({ logs: [], count: 0 });
  });

  it('getJob returns a snapshot: mutating it does not change the stored job', async () => {
    const queue = track(new Queue('q', { connection: freshNs() }));
    const added = await queue.add('j', { nested: { x: 1 } }, { attempts: 2 });
    const a = (await queue.getJob(added.id!))!;
    a.data.nested.x = 99;
    a.opts.attempts = 7;
    const b = (await queue.getJob(added.id!))!;
    expect(b.data).toEqual({ nested: { x: 1 } });
    expect(b.opts.attempts).toBe(2);
  });

  it('getWorkers lists open workers (named and unnamed) like the Redis client list', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    track(new Worker('q', async () => undefined, { connection }));
    const named = track(new Worker('q', async () => undefined, { connection, name: 'w-two' }));
    expect(await queue.getWorkersCount()).toBe(2);
    await named.close();
    expect((await queue.getWorkers()).map((w) => w.rawname)).toEqual(['bull:q']);
  });
});
