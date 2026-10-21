import { once } from 'node:events';
import { describe, expect, it } from 'vitest';
import { freshNs, Queue, QueueEvents, sleep, track, until, Worker } from './helpers.ts';

describe('pause / resume', () => {
  it('a paused queue keeps jobs waiting; resume wakes the blocked worker', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const seen: string[] = [];
    const worker = track(new Worker('q', async (job) => { seen.push(job.name); }, { connection, drainDelay: 30 }));
    await queue.pause();
    expect(await queue.isPaused()).toBe(true);
    await queue.add('a', {});
    await sleep(150);
    expect(seen).toEqual([]);
    // BullMQ 6 pauses with a meta flag only: the job stays (and is counted) in `wait`.
    expect(await queue.getJobCounts('waiting')).toEqual({ waiting: 1 });
    expect(await queue.count()).toBe(1);
    expect(await queue.getJobState('1')).toBe('waiting');

    const done = once(worker, 'completed');
    const resumedAt = Date.now();
    await queue.resume();
    await done;
    expect(Date.now() - resumedAt).toBeLessThan(1_000); // woken by resume, not by the 30 s drainDelay
    expect(await queue.isPaused()).toBe(false);
    expect(seen).toEqual(['a']);
  });

  it('paused/resumed are published on the event stream', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const events = track(new QueueEvents('q', { connection }));
    await events.waitUntilReady();
    const seen: string[] = [];
    events.on('paused', () => seen.push('paused'));
    events.on('resumed', () => seen.push('resumed'));
    await queue.pause();
    await queue.resume();
    await until(() => seen.length === 2);
    expect(seen).toEqual(['paused', 'resumed']);
  });

  it('worker.pause() / resume() (local) stop and restart consumption', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const seen: string[] = [];
    const worker = track(new Worker('q', async (job) => { seen.push(job.name); }, { connection }));
    await worker.pause();
    await queue.add('a', {});
    await sleep(100);
    expect(seen).toEqual([]);
    worker.resume();
    await until(() => seen.length === 1);
  });

  it('global concurrency (setGlobalConcurrency) caps active jobs across workers', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    await queue.setGlobalConcurrency(1);
    expect(await queue.getGlobalConcurrency()).toBe(1);
    let inFlight = 0;
    let max = 0;
    let done = 0;
    const processor = async () => { max = Math.max(max, ++inFlight); await sleep(20); inFlight--; done++; };
    track(new Worker('q', processor, { connection, concurrency: 3 }));
    track(new Worker('q', processor, { connection, concurrency: 3 }));
    await queue.addBulk(Array.from({ length: 6 }, () => ({ name: 'j', data: {} })));
    await until(() => done === 6);
    expect(max).toBe(1);
    await queue.removeGlobalConcurrency();
    expect(await queue.getGlobalConcurrency()).toBeNull();
  });
});

describe('remove / clean / drain / obliterate', () => {
  it('remove() deletes a waiting job but refuses a locked (active) one', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const worker = track(new Worker('q', null, { connection, autorun: false }));
    const a = await queue.add('a', {});
    const b = await queue.add('b', {});
    await a.remove(); // claimed? no: still waiting
    expect(await queue.getJob(a.id!)).toBeUndefined();
    const active = (await worker.getNextJob('tok'))!;
    expect(active.id).toBe(b.id);
    await expect(active.remove()).rejects.toThrow(/locked by another worker/);
    expect(await queue.remove(b.id!)).toBe(0);
    await active.moveToCompleted('ok', 'tok', false);
    expect(await queue.remove(b.id!)).toBe(1);
    expect(await queue.getJobCounts()).toMatchObject({ completed: 0, waiting: 0, active: 0 });
  });

  it('clean() removes finished jobs older than the grace period, honouring the limit', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const worker = track(new Worker('q', async () => 'ok', { connection }));
    await queue.addBulk(Array.from({ length: 5 }, () => ({ name: 'j', data: {} })));
    await until(async () => (await queue.getCompletedCount()) === 5);
    void worker;
    expect(await queue.clean(60_000, 10, 'completed')).toEqual([]); // all younger than the grace
    await sleep(20);
    const cleaned = await queue.clean(0, 2, 'completed');
    expect(cleaned).toHaveLength(2);
    expect(await queue.getCompletedCount()).toBe(3);
    expect(await queue.clean(0, 0, 'completed')).toHaveLength(3);
  });

  it('clean() of waiting / delayed jobs', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    await queue.add('w', {});
    await queue.add('d', {}, { delay: 60_000 });
    await sleep(5);
    expect(await queue.clean(0, 0, 'wait')).toEqual(['1']);
    expect(await queue.clean(0, 0, 'delayed')).toEqual(['2']);
    expect(await queue.count()).toBe(0);
  });

  it('drain() removes waiting (and with `true`, delayed) jobs but keeps finished ones', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const worker = track(new Worker('q', null, { connection, autorun: false }));
    await queue.add('done', {});
    const job = (await worker.getNextJob('t'))!;
    await job.moveToCompleted('x', 't', false);
    await queue.add('w', {});
    await queue.add('p', {}, { priority: 3 });
    await queue.add('d', {}, { delay: 60_000 });
    await queue.drain();
    expect(await queue.getJobCounts('waiting', 'prioritized', 'delayed', 'completed')).toEqual({
      waiting: 0, prioritized: 0, delayed: 1, completed: 1,
    });
    await queue.drain(true);
    expect(await queue.getDelayedCount()).toBe(0);
  });

  it('obliterate() refuses active jobs unless forced and leaves an empty, usable queue', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const worker = track(new Worker('q', null, { connection, autorun: false }));
    await queue.add('a', {});
    await queue.add('b', {});
    await worker.getNextJob('t');
    await expect(queue.obliterate()).rejects.toThrow('Cannot obliterate queue with active jobs');
    await queue.obliterate({ force: true });
    expect(await queue.getJobCounts()).toMatchObject({ active: 0, waiting: 0 });
    expect(await queue.isPaused()).toBe(false);
    expect((await queue.add('c', {})).id).toBe('1'); // id counter restarts, like Redis
  });

  it('removeOnComplete / removeOnFail with true, count and age', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const worker = track(new Worker('q', async (job) => { if (job.data.fail) throw new Error('x'); }, { connection }));
    let finished = 0;
    worker.on('completed', () => finished++);
    worker.on('failed', () => finished++);
    await queue.add('gone', {}, { removeOnComplete: true });
    for (let i = 0; i < 4; i++) await queue.add(`keep-${i}`, {}, { removeOnComplete: 2 });
    await queue.add('f1', { fail: true }, { removeOnFail: { age: 3600 } });
    await queue.add('f2', { fail: true }, { removeOnFail: true });
    await until(() => finished === 7);
    expect(await queue.getJob('1')).toBeUndefined();
    expect((await queue.getCompleted()).map((j) => j.name)).toEqual(['keep-3', 'keep-2']);
    expect((await queue.getFailed()).map((j) => j.name)).toEqual(['f1']);
  });

  it('worker-level removeOnComplete applies when the job has none', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    const worker = track(new Worker('q', async () => 'ok', { connection, removeOnComplete: { count: 1 } }));
    let done = 0;
    worker.on('completed', () => done++);
    await queue.addBulk([{ name: 'a', data: {} }, { name: 'b', data: {} }, { name: 'c', data: {} }]);
    await until(() => done === 3);
    expect((await queue.getCompleted()).map((j) => j.name)).toEqual(['c']);
  });
});

describe('priority and lifo', () => {
  it('lower priority number runs first; plain jobs before prioritized ones; FIFO within a priority', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    await queue.add('p5-a', {}, { priority: 5 });
    await queue.add('p1', {}, { priority: 1 });
    await queue.add('plain', {});
    await queue.add('p5-b', {}, { priority: 5 });
    await queue.add('p3', {}, { priority: 3 });
    expect(await queue.getJobState('2')).toBe('prioritized');
    expect(await queue.getJobCounts('prioritized', 'waiting')).toEqual({ prioritized: 4, waiting: 1 });
    expect(await queue.getCountsPerPriority([0, 1, 5, 9])).toEqual({ '0': 1, '1': 1, '5': 2, '9': 0 });
    expect((await queue.getPrioritized()).map((j) => j.name)).toEqual(['p1', 'p3', 'p5-a', 'p5-b']);
    const seen: string[] = [];
    track(new Worker('q', async (job) => { seen.push(job.name); }, { connection }));
    await until(() => seen.length === 5);
    expect(seen).toEqual(['plain', 'p1', 'p3', 'p5-a', 'p5-b']);
  });

  it('lifo jobs jump to the head of the waiting list', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    await queue.add('a', {});
    await queue.add('b', {});
    await queue.add('urgent', {}, { lifo: true });
    const seen: string[] = [];
    track(new Worker('q', async (job) => { seen.push(job.name); }, { connection }));
    await until(() => seen.length === 3);
    expect(seen).toEqual(['urgent', 'a', 'b']);
  });

  it('changePriority() re-orders a waiting job', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    await queue.add('x', {}, { priority: 10 });
    const y = await queue.add('y', {}, { priority: 20 });
    await y.changePriority({ priority: 1 });
    const seen: string[] = [];
    track(new Worker('q', async (job) => { seen.push(job.name); }, { connection }));
    await until(() => seen.length === 2);
    expect(seen).toEqual(['y', 'x']);
  });
});
