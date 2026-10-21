import { once } from 'node:events';
import { describe, expect, it } from 'vitest';
import { UnrecoverableError, type Job } from 'bullmq';
import { freshNs, Queue, QueueEvents, track, until, Worker } from './helpers.ts';

describe('retries (attempts + backoff)', () => {
  it('retries immediately without backoff and completes on the 3rd attempt; job id never changes', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const attempts: { id: string; made: number; started: number }[] = [];
    const worker = track(
      new Worker('q', async (job) => {
        attempts.push({ id: job.id!, made: job.attemptsMade, started: job.attemptsStarted });
        if (attempts.length < 3) throw new Error(`fail ${attempts.length}`);
        return 'ok';
      }, { connection }),
    );
    const failedEvents: number[] = [];
    worker.on('failed', (job) => failedEvents.push(job!.attemptsMade));
    const completed = once(worker, 'completed') as Promise<[Job]>;
    const added = await queue.add('j', {}, { attempts: 3 });
    const [job] = await completed;

    expect(attempts).toEqual([
      { id: added.id, made: 0, started: 1 },
      { id: added.id, made: 1, started: 2 },
      { id: added.id, made: 2, started: 3 },
    ]);
    expect(failedEvents).toEqual([1, 2]);
    expect(job.attemptsMade).toBe(3);
    const stored = (await queue.getJob(added.id!))!;
    expect(await stored.getState()).toBe('completed');
    expect(stored.attemptsMade).toBe(3);
    expect(stored.attemptsStarted).toBe(3);
    expect(stored.failedReason).toBe('fail 2'); // last failure is kept, like Redis
  });

  it('exhausts attempts -> failed, with retries-exhausted on the event stream', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const events = track(new QueueEvents('q', { connection }));
    await events.waitUntilReady();
    const exhausted = once(events, 'retries-exhausted') as Promise<[{ jobId: string; attemptsMade: string }]>;
    let runs = 0;
    track(new Worker('q', async () => { runs++; throw new Error('always'); }, { connection }));
    const added = await queue.add('j', {}, { attempts: 2 });

    const [args] = await exhausted;
    expect(args).toMatchObject({ jobId: added.id, attemptsMade: '2' });
    expect(runs).toBe(2);
    const stored = (await queue.getJob(added.id!))!;
    expect(await stored.getState()).toBe('failed');
    expect(stored.attemptsMade).toBe(2);
  });

  it('fixed backoff: the retry waits at least `delay` ms in the delayed state', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const startedAt: number[] = [];
    const worker = track(
      new Worker('q', async () => {
        startedAt.push(Date.now());
        if (startedAt.length === 1) throw new Error('once');
      }, { connection }),
    );
    const failed = once(worker, 'failed');
    const completed = once(worker, 'completed');
    const added = await queue.add('j', {}, { attempts: 2, backoff: { type: 'fixed', delay: 200 } });

    await failed;
    expect(await queue.getJobState(added.id!)).toBe('delayed');
    expect(await queue.getJobCounts('delayed')).toEqual({ delayed: 1 });
    await completed;
    expect(startedAt[1]! - startedAt[0]!).toBeGreaterThanOrEqual(200);
    // Generous upper bound: the worker sleeps until the due time instead of polling at drainDelay (5 s).
    expect(startedAt[1]! - startedAt[0]!).toBeLessThan(1_500);
  });

  it('exponential backoff doubles the delay per attempt', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const startedAt: number[] = [];
    track(
      new Worker('q', async () => {
        startedAt.push(Date.now());
        if (startedAt.length < 3) throw new Error('again');
      }, { connection }),
    );
    await queue.add('j', {}, { attempts: 3, backoff: { type: 'exponential', delay: 100 } });
    await until(() => startedAt.length === 3);
    const gap1 = startedAt[1]! - startedAt[0]!;
    const gap2 = startedAt[2]! - startedAt[1]!;
    expect(gap1).toBeGreaterThanOrEqual(100); // 100 * 2^0
    expect(gap2).toBeGreaterThanOrEqual(200); // 100 * 2^1
    expect(gap2).toBeLessThan(2_000);
  });

  it('UnrecoverableError skips the remaining attempts', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    let runs = 0;
    const worker = track(new Worker('q', async () => { runs++; throw new UnrecoverableError('fatal'); }, { connection }));
    const failed = once(worker, 'failed') as Promise<[Job, Error]>;
    const added = await queue.add('j', {}, { attempts: 5 });
    const [job, err] = await failed;
    expect(err.message).toBe('fatal');
    expect(job.id).toBe(added.id);
    expect(runs).toBe(1);
    expect(await queue.getJobState(added.id!)).toBe('failed');
  });

  it('job.retry() re-queues a failed job and it can then complete', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    let shouldFail = true;
    const worker = track(new Worker('q', async () => { if (shouldFail) throw new Error('x'); return 'second time'; }, { connection }));
    const failed = once(worker, 'failed');
    const added = await queue.add('j', {});
    await failed;
    shouldFail = false;
    const completed = once(worker, 'completed');
    const job = (await queue.getJob(added.id!))!;
    await job.retry('failed');
    await completed;
    const stored = (await queue.getJob(added.id!))!;
    expect(await stored.getState()).toBe('completed');
    expect(stored.returnvalue).toBe('second time');
    await expect(stored.retry('failed')).rejects.toThrow(/is not in the failed state/);
  });

  it('queue.retryJobs() moves every failed job back to wait', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const worker = track(new Worker('q', async () => { throw new Error('x'); }, { connection, autorun: false }));
    await queue.addBulk([{ name: 'a', data: {} }, { name: 'b', data: {} }]);
    for (let i = 0; i < 2; i++) {
      const job = (await worker.getNextJob('t'))!;
      await job.moveToFailed(new Error('nope'), 't');
    }
    expect(await queue.getJobCounts('failed', 'waiting')).toEqual({ failed: 2, waiting: 0 });
    await queue.retryJobs({ state: 'failed' });
    expect(await queue.getJobCounts('failed', 'waiting')).toEqual({ failed: 0, waiting: 2 });
    const job = (await queue.getJob('1'))!;
    expect(job.failedReason).toBeFalsy();
  });
});
