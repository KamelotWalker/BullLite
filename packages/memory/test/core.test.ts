import { once } from 'node:events';
import { describe, expect, it } from 'vitest';
import { withBackend, type Job } from 'bullmq';
import { createMemoryBackend } from '../src/index.ts';
import { freshNs, Queue, recordingFactory, sleep, track, until, Worker, type Call } from './helpers.ts';

describe('real BullMQ Queue/Worker on the memory backend', () => {
  it('add -> process -> completed, return value visible on the job', async () => {
    const connection = freshNs();
    const queue = track(new Queue<{ n: number }, { doubled: number }>('q', { connection }));
    const worker = track(
      new Worker<{ n: number }, { doubled: number }>('q', async (job) => ({ doubled: job.data.n * 2 }), { connection }),
    );
    const completed = once(worker, 'completed') as Promise<[Job, { doubled: number }]>;
    const added = await queue.add('double', { n: 21 });

    const [job, result] = await completed;
    expect(job.id).toBe(added.id);
    expect(result).toEqual({ doubled: 42 });

    const stored = await queue.getJob(added.id!);
    expect(stored?.returnvalue).toEqual({ doubled: 42 });
    expect(stored?.finishedOn).toBeTypeOf('number');
    expect(stored?.processedOn).toBeTypeOf('number');
    expect(stored?.attemptsMade).toBe(1);
    expect(stored?.data).toEqual({ n: 21 });
    expect(await stored?.getState()).toBe('completed');
  });

  it('processor throws -> failed with reason and stacktrace', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const worker = track(new Worker('q', async () => { throw new Error('boom'); }, { connection }));
    const failed = once(worker, 'failed') as Promise<[Job, Error]>;
    const added = await queue.add('bad', {});

    const [job, err] = await failed;
    expect(job.id).toBe(added.id);
    expect(err.message).toBe('boom');

    const stored = await queue.getJob(added.id!);
    expect(stored?.failedReason).toBe('boom');
    expect(stored?.attemptsMade).toBe(1);
    expect(stored?.stacktrace?.[0]).toContain('boom');
    expect(await stored?.getState()).toBe('failed');
  });

  it('jobs are claimed in FIFO order', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    for (let i = 0; i < 10; i++) await queue.add('j', { i });
    const seen: number[] = [];
    track(new Worker('q', async (job) => { seen.push(job.data.i); }, { connection }));
    await until(() => seen.length === 10);
    expect(seen).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  it('concurrency: 3 never exceeds 3 in-flight and does reach 3', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    let inFlight = 0;
    let max = 0;
    let done = 0;
    track(
      new Worker('q', async () => {
        max = Math.max(max, ++inFlight);
        await sleep(30);
        inFlight--;
        done++;
      }, { connection, concurrency: 3 }),
    );
    await queue.addBulk(Array.from({ length: 12 }, (_, i) => ({ name: 'j', data: { i } })));
    await until(() => done === 12);
    expect(max).toBe(3);
  });

  it('duplicate custom jobId: second add returns the existing id and is not processed again', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const first = await queue.add('a', { v: 1 }, { jobId: 'custom-1' });
    const second = await queue.add('a', { v: 2 }, { jobId: 'custom-1' });
    expect(first.id).toBe('custom-1');
    expect(second.id).toBe('custom-1');
    expect((await queue.getJob('custom-1'))?.data).toEqual({ v: 1 });
    expect(await queue.getJobCounts('waiting')).toEqual({ waiting: 1 });

    const seen: unknown[] = [];
    const worker = track(new Worker('q', async (job) => { seen.push(job.data); }, { connection }));
    await once(worker, 'completed');
    // Same id after completion is still a duplicate while the job is retained.
    await queue.add('a', { v: 3 }, { jobId: 'custom-1' });
    await sleep(50);
    expect(seen).toEqual([{ v: 1 }]);
  });

  it('auto-generated ids are increasing integers per queue', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const ids = [];
    for (let i = 0; i < 3; i++) ids.push((await queue.add('j', {})).id);
    expect(ids).toEqual(['1', '2', '3']);
  });

  it('namespaces are isolated', async () => {
    const a = freshNs();
    const b = freshNs();
    const qa = track(new Queue('shared-name', { connection: a }));
    const qb = track(new Queue('shared-name', { connection: b }));
    const ja = await qa.add('x', { ns: 'a' });
    const jb = await qb.add('x', { ns: 'b' });

    const seen: unknown[] = [];
    const wa = track(new Worker('shared-name', async (job) => { seen.push(job.data); }, { connection: a }));
    await once(wa, 'completed');
    await sleep(50);
    expect(seen).toEqual([{ ns: 'a' }]);
    expect(await (await qb.getJob(jb.id!))?.getState()).toBe('waiting');
    expect(await (await qa.getJob(ja.id!))?.getState()).toBe('completed');
  });

  it('prefixes are isolated like Redis key prefixes', async () => {
    const connection = freshNs();
    const q1 = track(new Queue('q', { connection, prefix: 'one' }));
    const q2 = track(new Queue('q', { connection, prefix: 'two' }));
    await q1.add('x', {});
    expect(await q1.count()).toBe(1);
    expect(await q2.count()).toBe(0);
  });

  it('atomic claim: three workers on one queue process each job exactly once', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const runs: string[] = [];
    for (let w = 0; w < 3; w++) {
      track(new Worker('q', async (job) => { runs.push(job.id!); await sleep(5); }, { connection, concurrency: 2 }));
    }
    await queue.addBulk(Array.from({ length: 30 }, (_, i) => ({ name: 'j', data: { i } })));
    await until(() => runs.length >= 30);
    await sleep(50);
    expect(runs).toHaveLength(30);
    expect(new Set(runs).size).toBe(30);
  });

  it('idle worker blocks on waitForJob instead of busy-polling, and wakes on add', async () => {
    const connection = freshNs();
    const log: Call[] = [];
    const { Worker: W, Queue: Q } = withBackend(recordingFactory(createMemoryBackend, log));
    const worker = track(new W('q', async () => 'ok', { connection }));
    await sleep(500);
    const claims = log.filter((c) => c.instance === 'worker' && c.name === 'moveToActive').length;
    const waits = log.filter((c) => c.instance === 'worker' && c.name === 'waitForJob').length;
    expect(claims).toBeLessThanOrEqual(2);
    expect(waits).toBe(1);

    const queue = track(new Q('q', { connection }));
    const started = Date.now();
    const done = once(worker, 'completed');
    await queue.add('j', {});
    await done;
    expect(Date.now() - started).toBeLessThan(1_000); // woken by the add, not by the 5 s drainDelay timeout
  });

  it('worker.close() waits for the active job; no new jobs are taken afterwards', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const started: string[] = [];
    const worker = new Worker('q', async (job) => { started.push(job.id!); await sleep(100); return 'done'; }, { connection });
    await queue.add('a', {});
    await until(() => started.length === 1);
    await queue.add('b', {});
    await worker.close();
    expect(started).toEqual(['1']);
    expect(await queue.getJobState('1')).toBe('completed');
    expect(await queue.getJobState('2')).toBe('waiting');
  });

  it('lock renewal: a job longer than lockDuration is not treated as stalled', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    let runs = 0;
    const worker = track(
      new Worker('q', async () => { runs++; await sleep(900); }, {
        connection, lockDuration: 300, stalledInterval: 150, maxStalledCount: 0,
      }),
    );
    const stalled: string[] = [];
    worker.on('stalled', (id) => stalled.push(id));
    const completed = once(worker, 'completed');
    const added = await queue.add('slow', {});
    await completed;
    expect(runs).toBe(1);
    expect(stalled).toEqual([]);
    expect(await (await queue.getJob(added.id!))?.getState()).toBe('completed');
  });

  it('finishing with a wrong lock token fails with BullMQ lock errors', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const worker = track(new Worker('q', null, { connection, autorun: false }));
    await queue.add('j', {});
    const job = (await worker.getNextJob('token-a'))!;
    await expect(job.moveToCompleted('x', 'token-b', false)).rejects.toThrow(/Lock mismatch for job 1/);
    await expect(job.moveToCompleted('x', 'token-a', false)).resolves.toBeUndefined();
    await expect(job.moveToCompleted('x', 'token-a', false)).rejects.toThrow(/Missing lock for job 1/);
  });
});
