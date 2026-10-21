import { once } from 'node:events';
import { describe, expect, it } from 'vitest';
import { withBackend, type Job } from 'bullmq';
import { createMemoryBackend } from '../src/index.ts';
import { freshNs, Queue, recordingFactory, sleep, track, until, Worker, type Call } from './helpers.ts';

describe('delayed jobs', () => {
  it('a delayed job does not start before its delay and starts soon after', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const worker = track(new Worker('q', async () => 'ok', { connection }));
    const completed = once(worker, 'completed') as Promise<[Job]>;
    const added = await queue.add('d', {}, { delay: 300 });
    expect(await queue.getJobState(added.id!)).toBe('delayed');
    expect((await queue.getJob(added.id!))?.delay).toBe(300);

    const [job] = await completed;
    expect(job.processedOn! - added.timestamp).toBeGreaterThanOrEqual(300);
    expect(job.processedOn! - added.timestamp).toBeLessThan(1_500);
  });

  it('delayed jobs run in due order regardless of insertion order', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const seen: string[] = [];
    track(new Worker('q', async (job) => { seen.push(job.name); }, { connection }));
    await queue.add('third', {}, { delay: 300 });
    await queue.add('first', {}, { delay: 100 });
    await queue.add('now', {});
    await queue.add('second', {}, { delay: 200 });
    await until(() => seen.length === 4);
    expect(seen).toEqual(['now', 'first', 'second', 'third']);
  });

  it('an idle worker sleeps until the next due job instead of polling', async () => {
    const connection = freshNs();
    const log: Call[] = [];
    const { Worker: W } = withBackend(recordingFactory(createMemoryBackend, log));
    const queue = track(new Queue('q', { connection }));
    await queue.add('d', {}, { delay: 400 });
    const worker = track(new W('q', async () => 'ok', { connection }));
    await once(worker, 'completed');
    const claims = log.filter((c) => c.name === 'moveToActive').length;
    const waits = log.filter((c) => c.name === 'waitForJob').length;
    // Redis/Postgres semantics: claim -> empty + delayUntil -> one block until due -> claim. No tight loop.
    expect(claims).toBeLessThanOrEqual(3);
    expect(waits).toBeLessThanOrEqual(2);
  });

  it('a delayed job added while the worker is blocked shortens the wait', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const worker = track(new Worker('q', async () => 'ok', { connection, drainDelay: 30 }));
    await sleep(100); // worker is now blocked in waitForJob(30 s)
    const completed = once(worker, 'completed');
    const added = Date.now();
    await queue.add('d', {}, { delay: 150 });
    await completed;
    expect(Date.now() - added).toBeGreaterThanOrEqual(150);
    expect(Date.now() - added).toBeLessThan(2_000);
  });

  it('promote() makes a delayed job run immediately', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const worker = track(new Worker('q', async () => 'ok', { connection }));
    const added = await queue.add('d', {}, { delay: 60_000 });
    const completed = once(worker, 'completed');
    await sleep(50);
    await added.promote();
    await completed;
    expect(await queue.getJobState(added.id!)).toBe('completed');
    await expect(added.promote()).rejects.toThrow(/is not in the delayed state/);
  });

  it('changeDelay() reschedules; promoteJobs() promotes all delayed jobs', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const a = await queue.add('a', {}, { delay: 60_000 });
    await queue.add('b', {}, { delay: 60_000 });
    await a.changeDelay(10);
    const worker = track(new Worker('q', async () => 'ok', { connection }));
    await once(worker, 'completed');
    expect(await queue.getJobState(a.id!)).toBe('completed');
    expect(await queue.getJobCounts('delayed')).toEqual({ delayed: 1 });
    await queue.promoteJobs();
    await until(async () => (await queue.getJobCounts('completed')).completed === 2);
  });

  it('job.moveToDelayed() from a manual worker re-delays without counting an attempt', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const worker = track(new Worker('q', null, { connection, autorun: false }));
    await queue.add('j', {});
    const job = (await worker.getNextJob('tok'))!;
    await job.moveToDelayed(Date.now() + 60_000, 'tok');
    const stored = (await queue.getJob(job.id!))!;
    expect(await stored.getState()).toBe('delayed');
    expect(stored.attemptsMade).toBe(0);
  });
});
