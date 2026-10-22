# bullmq-memory-backend

Experimental alpha (`0.1.0-alpha.1`) in-memory backend for BullMQ v6 `IQueueBackend`
(`bullmq@^6.3.9`): run real `Queue`, `Worker` and `QueueEvents` in tests and local
development without Redis.

**Unofficial community project; not affiliated with BullMQ or Taskforce.sh.**
No persistence, single process, no production guarantees.

## Install

```bash
npm install -D bullmq bullmq-memory-backend
```

Supported: `bullmq@^6.3.9` (CI tests 6.3.9 and the newest 6.x release, weekly).
Node.js 20, 22 and 24 are tested. Both ESM and CommonJS are shipped.

## Quick start

```ts
import { withBackend } from 'bullmq';
import { createMemoryBackend } from 'bullmq-memory-backend';

const { Queue, Worker, QueueEvents } = withBackend(createMemoryBackend);
const connection = { namespace: 'demo' };
const queue = new Queue('emails', { connection });
const events = new QueueEvents('emails', { connection });
const worker = new Worker('emails', async (job) => ({ delivered: job.data.to }), { connection, concurrency: 2 });
const job = await queue.add('send', { to: 'hello@example.com' }, { attempts: 3, backoff: { type: 'exponential', delay: 100 } });
console.log(await job.waitUntilFinished(events)); // { delivered: 'hello@example.com' }
await worker.close();
await events.close();
await queue.close();
```

Or keep your code unchanged and call `setDefaultBackendFactory(createMemoryBackend)`
in your test setup. Support matrix, store lifetime (`retain`, `resetMemoryStore`),
fake timers and limitations: https://github.com/KamelotWalker/BullLite#readme
