// Child process used by the handle-leak tests. It imports the *built* package
// (dist/esm) and exits by itself iff close() leaves no active handles/timers.
//   clean: Queue + Worker + QueueEvents, all closed       -> must exit 0
//   leak:  the Worker is left open (control experiment)  -> must keep running
import { withBackend } from 'bullmq';
import { createMemoryBackend } from '../../dist/esm/index.js';

const leak = process.argv[2] === 'leak';
const { Queue, Worker, QueueEvents } = withBackend(createMemoryBackend);
const connection = { namespace: 'lifecycle' };

const queue = new Queue('q', { connection });
const events = new QueueEvents('q', { connection });
const worker = new Worker('q', async (job) => job.data, { connection });
const job = await queue.add('j', { ok: true }, { delay: 20, attempts: 2, backoff: 10 });
await job.waitUntilFinished(events);
await queue.add('later', {}, { delay: 60_000 }); // a pending delayed job must not keep the process alive
if (!leak) await worker.close();
await events.close();
await queue.close();
console.log('closed');
