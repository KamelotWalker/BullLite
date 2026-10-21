import { execFile } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';
import { withBackend, type Job } from 'bullmq';
import { createMemoryBackend, UnsupportedError } from '../src/index.ts';
import { recordingFactory, type Call } from '../scratch/record.ts';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const { Queue, Worker } = withBackend(createMemoryBackend);

type Closable = { close(): Promise<void> };
const open: Closable[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((c) => c.close()));
});
const track = <T extends Closable>(c: T): T => (open.push(c), c);
const freshNs = () => ({ namespace: `t-${randomUUID()}` });

describe('real BullMQ Queue/Worker on the memory backend', () => {
  it('(a) add -> process -> completed, return value visible on the job', async () => {
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
    expect(await stored?.getState()).toBe('completed');
  });

  it('(b) processor throws -> failed with the error', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const worker = track(
      new Worker('q', async () => { throw new Error('boom'); }, { connection }),
    );
    const failed = once(worker, 'failed') as Promise<[Job, Error]>;
    const added = await queue.add('bad', {});

    const [job, err] = await failed;
    expect(job.id).toBe(added.id);
    expect(err.message).toBe('boom');

    const stored = await queue.getJob(added.id!);
    expect(stored?.failedReason).toBe('boom');
    expect(stored?.attemptsMade).toBe(1);
    expect(await stored?.getState()).toBe('failed');
  });

  it('(c) concurrency: 2 never exceeds 2 in-flight and does reach 2', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    let inFlight = 0;
    let max = 0;
    let done = 0;
    const all = Promise.withResolvers<void>();
    const worker = track(
      new Worker('q', async () => {
        max = Math.max(max, ++inFlight);
        await sleep(40);
        inFlight--;
        if (++done === 8) all.resolve();
      }, { connection, concurrency: 2 }),
    );
    void worker;
    for (let i = 0; i < 8; i++) await queue.add('j', { i });
    await all.promise;
    expect(max).toBe(2);
  });

  it('(d) duplicate custom jobId: second add is ignored, first job kept (BullMQ documented behaviour)', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const seen: unknown[] = [];
    const first = await queue.add('a', { v: 1 }, { jobId: 'custom-1' });
    const second = await queue.add('a', { v: 2 }, { jobId: 'custom-1' });
    expect(first.id).toBe('custom-1');
    expect(second.id).toBe('custom-1');

    const worker = track(new Worker('q', async (job) => { seen.push(job.data); }, { connection }));
    await once(worker, 'drained');
    await sleep(50);
    expect(seen).toEqual([{ v: 1 }]);
  });

  it('(e) namespaces are isolated', async () => {
    const a = freshNs();
    const b = freshNs();
    const qa = track(new Queue('shared-name', { connection: a }));
    const qb = track(new Queue('shared-name', { connection: b }));
    const ja = await qa.add('x', { ns: 'a' });
    const jb = await qb.add('x', { ns: 'b' });

    const seen: unknown[] = [];
    const wa = track(new Worker('shared-name', async (job) => { seen.push(job.data); }, { connection: a }));
    await once(wa, 'completed');
    await sleep(100);
    expect(seen).toEqual([{ ns: 'a' }]);
    expect(await (await qb.getJob(jb.id!))?.getState()).toBe('waiting');
    expect(await (await qa.getJob(ja.id!))?.getState()).toBe('completed');

    const wb = track(new Worker('shared-name', async (job) => { seen.push(job.data); }, { connection: b }));
    await once(wb, 'completed');
    expect(seen).toEqual([{ ns: 'a' }, { ns: 'b' }]);
  });

  it('atomic claim: two workers on one queue process each job exactly once', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const runs: string[] = [];
    const mk = (tag: string) =>
      track(new Worker('q', async (job) => { runs.push(`${job.id}`); void tag; await sleep(30); }, { connection }));
    mk('w1');
    mk('w2');
    await sleep(50);
    for (let i = 0; i < 20; i++) await queue.add('j', { i });
    while (runs.length < 20) await sleep(20);
    await sleep(100);
    expect(runs).toHaveLength(20);
    expect(new Set(runs).size).toBe(20);
  });

  it('idle worker blocks on waitForJob instead of busy-polling', async () => {
    const connection = freshNs();
    const log: Call[] = [];
    const { Worker: W } = withBackend(recordingFactory(createMemoryBackend, log));
    const worker = track(new W('q', async () => undefined, { connection }));
    await sleep(1000);
    const claims = log.filter((c) => c.name === 'moveToActive').length;
    const waits = log.filter((c) => c.name === 'waitForJob').length;
    expect(claims).toBeLessThanOrEqual(2);
    expect(waits).toBe(1);
    void worker;
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

  it('unsupported operations fail loudly and name the method', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    await expect(queue.pause()).rejects.toThrow(UnsupportedError);
    await expect(queue.pause()).rejects.toThrow(/pause/);
    await expect(queue.add('d', {}, { delay: 1000 })).rejects.toThrow(/delay/);
    await expect(queue.add('p', {}, { priority: 1 })).rejects.toThrow(/priority/);
    await expect(queue.add('r', {}, { attempts: 3 })).rejects.toThrow(/attempts/);
    await expect(queue.getJobCounts()).rejects.toThrow(/getCounts/);
  });
});

describe('(f) close() leaves no active handles', () => {
  const script = fileURLToPath(new URL('./fixtures/lifecycle.ts', import.meta.url));
  const run = (arg: string, timeout: number) =>
    new Promise<{ code: number | null; killed: boolean; stdout: string }>((resolve) => {
      execFile(process.execPath, [script, arg], { timeout }, (err, stdout) => {
        resolve({ code: err ? ((err as { code?: number }).code ?? null) : 0, killed: !!err && (err as { killed?: boolean }).killed === true, stdout });
      });
    });

  it('child process exits by itself after worker.close() + queue.close()', async () => {
    const r = await run('clean', 15_000);
    expect(r.stdout).toContain('closed');
    expect(r.killed).toBe(false);
    expect(r.code).toBe(0);
  });

  it('control: the same script with the worker left open does NOT exit by itself', async () => {
    const r = await run('leak', 3_000);
    expect(r.stdout).toContain('closed');
    expect(r.killed).toBe(true);
  });
});
