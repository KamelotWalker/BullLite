// Child process used by the handle-leak test. Exits by itself iff close() leaves no active handles.
import { withBackend } from 'bullmq';
import { createMemoryBackend } from '../../src/index.ts';

const leak = process.argv[2] === 'leak';
const { Queue, Worker } = withBackend(createMemoryBackend);
const connection = { namespace: 'lifecycle' };

const queue = new Queue('q', { connection });
const worker = new Worker('q', async (job) => job.data, { connection });
const completed = new Promise<void>((resolve) => worker.once('completed', () => resolve()));
await queue.add('j', { ok: true });
await completed;
if (!leak) await worker.close(); // 'leak' variant: worker left open -> process must keep running
await queue.close();
console.log('closed');
