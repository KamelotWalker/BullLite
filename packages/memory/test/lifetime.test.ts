import { describe, expect, it } from 'vitest';
import { memoryNamespaces, resetMemoryStore } from '../src/index.ts';
import { freshNs, Queue, QueueEvents, track, Worker } from './helpers.ts';

describe('namespace store lifetime', () => {
  it('a namespace lives while any instance on it is open and is released when the last one closes', async () => {
    const connection = freshNs();
    const queue = new Queue('q', { connection });
    const worker = new Worker('q', null, { connection, autorun: false });
    const events = new QueueEvents('q', { connection, autorun: false });
    await queue.add('j', {});
    expect(memoryNamespaces()).toContain(connection.namespace);

    await queue.close();
    // Still held by the worker and the QueueEvents: the job is still there.
    const other = new Queue('q', { connection });
    expect(await other.count()).toBe(1);
    await other.close();
    await worker.close();
    expect(memoryNamespaces()).toContain(connection.namespace);
    await events.close();
    expect(memoryNamespaces()).not.toContain(connection.namespace);

    const reopened = new Queue('q', { connection });
    expect(await reopened.count()).toBe(0); // fresh store
    await reopened.close();
    expect(memoryNamespaces()).not.toContain(connection.namespace);
  });

  it('`retain: true` keeps the data after every instance closed, until resetMemoryStore()', async () => {
    const connection = { ...freshNs(), retain: true };
    const queue = new Queue('q', { connection });
    await queue.add('j', { kept: true });
    await queue.close();
    expect(memoryNamespaces()).toContain(connection.namespace);

    const later = new Queue('q', { connection: { namespace: connection.namespace } });
    expect((await later.getJob('1'))?.data).toEqual({ kept: true });
    await later.close();

    resetMemoryStore(connection.namespace);
    expect(memoryNamespaces()).not.toContain(connection.namespace);
  });

  it('resetMemoryStore() empties a namespace in place while instances stay usable', async () => {
    const connection = freshNs();
    const queue = track(new Queue('q', { connection }));
    await queue.add('a', {});
    await queue.add('b', {});
    resetMemoryStore(connection.namespace);
    expect(await queue.count()).toBe(0);
    expect((await queue.add('c', {})).id).toBe('1');
    expect(await queue.count()).toBe(1);
  });

  it('resetMemoryStore() without arguments resets every namespace', async () => {
    const a = freshNs();
    const b = freshNs();
    const qa = track(new Queue('q', { connection: a }));
    const qb = track(new Queue('q', { connection: b }));
    await qa.add('x', {});
    await qb.add('x', {});
    resetMemoryStore();
    expect(await qa.count()).toBe(0);
    expect(await qb.count()).toBe(0);
  });

  it('operations on a closed Queue reject like a closed Redis connection', async () => {
    const queue = new Queue('q', { connection: freshNs() });
    await queue.close();
    await expect(queue.add('j', {})).rejects.toThrow('Connection is closed.');
  });

  it('the default namespace is shared by instances that do not set one', async () => {
    const queue = track(new Queue('default-ns-q', { connection: {} }));
    const other = track(new Queue('default-ns-q', { connection: { namespace: 'default' } }));
    await queue.add('j', {});
    expect(await other.count()).toBe(1);
  });
});
