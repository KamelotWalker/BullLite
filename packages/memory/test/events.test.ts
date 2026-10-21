import { once } from 'node:events';
import { describe, expect, it } from 'vitest';
import { freshNs, Queue, QueueEvents, QueueEventsProducer, sleep, track, until, Worker } from './helpers.ts';

describe('QueueEvents (event stream)', () => {
  it('delivers the lifecycle events of a job in order', async () => {
    const connection = freshNs();
    const events = track(new QueueEvents('q', { connection }));
    await events.waitUntilReady();
    const seen: string[] = [];
    for (const name of ['added', 'waiting', 'active', 'progress', 'completed', 'drained'] as const) {
      events.on(name, () => seen.push(name));
    }
    const queue = track(new Queue('q', { connection }));
    track(new Worker('q', async (job) => { await job.updateProgress(10); return { ok: 1 }; }, { connection }));
    const completed = once(events, 'completed') as Promise<[{ jobId: string; returnvalue: unknown; prev?: string }]>;
    const job = await queue.add('j', {});
    const [args] = await completed;
    expect(args).toEqual({ jobId: job.id, returnvalue: { ok: 1 }, prev: 'active' });
    await until(() => seen.includes('drained'));
    expect(seen).toEqual(['added', 'waiting', 'active', 'progress', 'completed', 'drained']);
  });

  it('job.waitUntilFinished resolves with the return value and rejects with the failure', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const events = track(new QueueEvents('q', { connection }));
    track(new Worker('q', async (job) => {
      if (job.data.fail) throw new Error('nope');
      return job.data.n * 2;
    }, { connection }));
    const ok = await queue.add('ok', { n: 4 });
    const bad = await queue.add('bad', { fail: true });
    await expect(ok.waitUntilFinished(events)).resolves.toBe(8);
    await expect(bad.waitUntilFinished(events)).rejects.toThrow('nope');
    // Already finished before waiting: answered from the stored state.
    await expect(ok.waitUntilFinished(events)).resolves.toBe(8);
  });

  it('per-job events (`completed:<id>`), delayed and failed payloads', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const events = track(new QueueEvents('q', { connection }));
    await events.waitUntilReady();
    const delayed = once(events, 'delayed') as Promise<[{ jobId: string; delay: number }]>;
    const failed = once(events, 'failed') as Promise<[{ jobId: string; failedReason: string }]>;
    track(new Worker('q', async () => { throw new Error('kaput'); }, { connection }));
    const job = await queue.add('j', {}, { delay: 50 });
    const [d] = await delayed;
    expect(d).toEqual({ jobId: job.id, delay: job.timestamp + 50 });
    const [f] = await failed;
    expect(f).toMatchObject({ jobId: job.id, failedReason: 'kaput' });
  });

  it("'$' starts after the QueueEvents was created; lastEventId resumes from a cursor", async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    await queue.add('before', {});

    const live = track(new QueueEvents('q', { connection }));
    const ids: string[] = [];
    let cursor = '';
    live.on('added', ({ jobId }, id) => { ids.push(jobId); cursor ||= id; });
    await live.waitUntilReady();
    await queue.add('after-1', {});
    await queue.add('after-2', {});
    await until(() => ids.length === 2);
    expect(ids).toEqual(['2', '3']); // job 1 was added before the QueueEvents existed

    // A new reader resuming from the first cursor sees everything after it.
    const resumed = track(new QueueEvents('q', { connection, lastEventId: cursor }));
    const resumedIds: string[] = [];
    resumed.on('added', ({ jobId }) => resumedIds.push(jobId));
    await until(() => resumedIds.length === 1);
    expect(resumedIds).toEqual(['3']);

    // Resuming from 0 replays the retained history.
    const replay = track(new QueueEvents('q', { connection, lastEventId: '0' }));
    const replayed: string[] = [];
    replay.on('added', ({ jobId }) => replayed.push(jobId));
    await until(() => replayed.length === 3);
    expect(replayed).toEqual(['1', '2', '3']);
  });

  it('event ids are strictly increasing', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const events = track(new QueueEvents('q', { connection }));
    await events.waitUntilReady();
    const ids: string[] = [];
    events.on('added', (_args, id) => ids.push(id));
    await queue.addBulk(Array.from({ length: 50 }, () => ({ name: 'j', data: {} })));
    await until(() => ids.length === 50);
    const parsed = ids.map((id) => id.split('-').map(Number) as [number, number]);
    for (let i = 1; i < parsed.length; i++) {
      const [pm, ps] = parsed[i - 1]!;
      const [m, s] = parsed[i]!;
      expect(m > pm || (m === pm && s > ps)).toBe(true);
    }
  });

  it('the stream is trimmed to streams.events.maxLen', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection, streams: { events: { maxLen: 10 } } }));
    await until(async () => (await queue.getMeta()).maxLenEvents === 10);
    for (let i = 0; i < 20; i++) await queue.add('j', {}); // 40 events (added + waiting)
    const replay = track(new QueueEvents('q', { connection, lastEventId: '0' }));
    let count = 0;
    replay.on('waiting', () => count++);
    replay.on('added', () => count++);
    await until(() => count === 10);
    await sleep(50);
    expect(count).toBe(10);
  });

  it('QueueEventsProducer custom events reach QueueEvents', async () => {
    const connection = freshNs();
    const events = track(new QueueEvents('q', { connection }));
    await events.waitUntilReady();
    const producer = track(new QueueEventsProducer('q', { connection }));
    const got = once(events, 'my-event' as never) as Promise<[Record<string, string>]>;
    await producer.publishEvent({ eventName: 'my-event', foo: 'bar' });
    const [args] = await got;
    expect(args).toMatchObject({ foo: 'bar' });
  });

  it('close() interrupts a blocking read immediately', async () => {
    const connection = freshNs();
    const events = new QueueEvents('q', { connection, blockingTimeout: 60_000 });
    await events.waitUntilReady();
    await sleep(20);
    const started = Date.now();
    await events.close();
    expect(Date.now() - started).toBeLessThan(500);
  });
});
