import { once } from 'node:events';
import { describe, expect, it } from 'vitest';
import type { Job } from 'bullmq';
import { freshNs, Queue, QueueEvents, track, Worker } from './helpers.ts';

/** A job claimed by a "crashed" worker: claimed manually, never processed, lock never renewed. */
async function claimAndAbandon(connection: { namespace?: string }, lockDuration: number) {
  const queue = track(new Queue('q', { connection }));
  const crashed = track(new Worker('q', null, { connection, autorun: false, lockDuration }));
  const added = await queue.add('j', { v: 1 });
  const job = (await crashed.getNextJob('crashed-token'))!;
  expect(job.id).toBe(added.id);
  return { queue, job };
}

describe('stalled job recovery', () => {
  it('an abandoned job is moved back to wait, reprocessed, and the stale worker can no longer finish it', async () => {
    const connection = freshNs();
    const { queue, job } = await claimAndAbandon(connection, 100);
    const events = track(new QueueEvents('q', { connection }));
    await events.waitUntilReady();
    const stalledEvent = once(events, 'stalled') as Promise<[{ jobId: string }]>;

    const rescuer = track(new Worker('q', async (j) => `rescued ${j.data.v}`, { connection, stalledInterval: 100 }));
    const stalled = once(rescuer, 'stalled') as Promise<[string]>;
    const completed = once(rescuer, 'completed') as Promise<[Job, string]>;
    expect((await stalled)[0]).toBe(job.id);
    expect((await stalledEvent)[0].jobId).toBe(job.id);
    const [done, result] = await completed;
    expect(done.id).toBe(job.id);
    expect(result).toBe('rescued 1');
    const stored = (await queue.getJob(job.id!))!;
    expect(stored.stalledCounter).toBe(1);
    expect(stored.attemptsStarted).toBe(2);

    await expect(job.moveToCompleted('late', 'crashed-token', false)).rejects.toThrow(/Missing lock for job/);
  });

  it('beyond maxStalledCount the job fails with "job stalled more than allowable limit"', async () => {
    const connection = freshNs();
    const { queue, job } = await claimAndAbandon(connection, 100);
    let processed = 0;
    const rescuer = track(
      new Worker('q', async () => { processed++; }, { connection, stalledInterval: 100, maxStalledCount: 0 }),
    );
    const failed = once(rescuer, 'failed') as Promise<[Job, Error]>;
    const [failedJob, err] = await failed;
    expect(failedJob.id).toBe(job.id);
    expect(err.message).toBe('job stalled more than allowable limit');
    expect(processed).toBe(0);
    expect(await queue.getJobState(job.id!)).toBe('failed');
  });
});
