// Fake timers with the memory backend (docs/fake-timers.md): BullMQ's own timers
// (delays, backoff, stalled checks, lock renewal) follow the fake clock, while backend
// replies travel over a MessageChannel that fake-timer libraries do not patch.
// `vi.advanceTimersByTimeAsync` yields to the real event loop between timers, which is
// what lets those replies arrive; the synchronous `vi.advanceTimersByTime` does not.
import { once } from 'node:events';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Job } from 'bullmq';
import { freshNs, Queue, QueueEvents, track, Worker } from './helpers.ts';

const MINUTE = 60_000;

beforeEach(() => {
  vi.useFakeTimers();
});
// Registered after helpers.ts' afterEach, so it runs first: instances close on real timers.
afterEach(() => {
  vi.useRealTimers();
});

describe('fake timers (vi.useFakeTimers)', () => {
  it('queue.add() and getters resolve without advancing the clock', async () => {
    const queue = track(new Queue('q', { connection: freshNs() }));
    const job = await queue.add('j', { n: 1 });
    expect(await queue.getJobState(job.id!)).toBe('waiting');
    expect(job.timestamp).toBe(Date.now()); // the fake clock
  });

  it('a delayed job runs once the fake clock reaches its delay, not before', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const worker = track(new Worker('q', async () => 'done', { connection }));
    await worker.waitUntilReady();
    const added = await queue.add('reminder', {}, { delay: 60 * MINUTE });

    await vi.advanceTimersByTimeAsync(59 * MINUTE);
    expect(await queue.getJobState(added.id!)).toBe('delayed');

    const completed = once(worker, 'completed') as Promise<[Job, string]>;
    await vi.advanceTimersByTimeAsync(MINUTE);
    const [job, result] = await completed;
    expect(result).toBe('done');
    expect(job.processedOn! - added.timestamp).toBe(60 * MINUTE);
  });

  it('exponential backoff retries fire on the fake clock', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const startedAt: number[] = [];
    const worker = track(
      new Worker('q', async () => {
        startedAt.push(Date.now());
        if (startedAt.length < 3) throw new Error('flaky dependency');
        return 'ok';
      }, { connection }),
    );
    await worker.waitUntilReady();
    let failed = once(worker, 'failed');
    const completed = once(worker, 'completed');
    const added = await queue.add('j', {}, { attempts: 3, backoff: { type: 'exponential', delay: 10_000 } });
    await failed; // 1st attempt, no clock movement needed
    expect(await queue.getJobState(added.id!)).toBe('delayed');

    failed = once(worker, 'failed');
    await vi.advanceTimersByTimeAsync(9_999);
    expect(startedAt).toHaveLength(1); // the 10 s backoff is not over yet
    await vi.advanceTimersByTimeAsync(1);
    await failed; // 2nd attempt

    await vi.advanceTimersByTimeAsync(20_000); // 10 s * 2^1
    await completed; // 3rd attempt
    expect(startedAt.map((t) => t - startedAt[0]!)).toEqual([0, 10_000, 30_000]);
  });

  it('a stalled job is recovered by the stalled checker on the fake clock', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const events = track(new QueueEvents('q', { connection }));
    await events.waitUntilReady();
    // A "crashed" worker: claims the job, never processes it, never renews its 30 s lock.
    const crashed = track(new Worker('q', null, { connection, autorun: false }));
    const added = await queue.add('j', { v: 1 });
    expect((await crashed.getNextJob('crashed-token'))!.id).toBe(added.id);

    const rescuer = track(new Worker('q', async (job) => `rescued ${job.data.v}`, { connection }));
    await rescuer.waitUntilReady();
    const stalled = once(events, 'stalled') as Promise<[{ jobId: string }]>;
    const completed = once(rescuer, 'completed') as Promise<[Job, string]>;

    // Defaults: lockDuration 30 s, stalledInterval 30 s; a job is moved back to wait on
    // the first check that finds it unlocked after a previous check marked it (two-phase
    // mark/sweep, as on Redis).
    await vi.advanceTimersByTimeAsync(90_000);
    expect((await stalled)[0].jobId).toBe(added.id);
    const [job, result] = await completed;
    expect(result).toBe('rescued 1');
    expect(job.stalledCounter).toBe(1);
  });

  it('a long-running job is not stalled: lock renewal runs on the same fake clock', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    let release!: () => void;
    const worker = track(
      new Worker('q', () => new Promise<string>((r) => { release = () => r('finished'); }), {
        connection,
        lockDuration: 1_000,
        stalledInterval: 1_000,
      }),
    );
    const stalled = vi.fn();
    worker.on('stalled', stalled);
    await worker.waitUntilReady();
    const active = once(worker, 'active');
    await queue.add('long', {});
    await active;

    await vi.advanceTimersByTimeAsync(10_000); // ten lock durations
    expect(stalled).not.toHaveBeenCalled();
    expect(await queue.getJobCounts('active')).toEqual({ active: 1 });

    const completed = once(worker, 'completed') as Promise<[Job, string]>;
    release();
    expect((await completed)[1]).toBe('finished');
  });

  it('limitation: runAllTimersAsync() never drains while a Worker is open', async () => {
    const connection = freshNs();
    const worker = track(new Worker('q', async () => 'ok', { connection }));
    await worker.waitUntilReady();
    // The stalled checker and the lock manager re-arm their timers forever (also on Redis).
    await expect(vi.runAllTimersAsync()).rejects.toThrow(/infinite loop/);
  });
});
