# @bulllite/memory

Experimental in-memory backend for BullMQ 6 (`bullmq@6.4.0`): run real `Queue`,
`Worker` and `QueueEvents` in tests and local development without a Redis server.

**Unofficial community project; not affiliated with BullMQ.** No persistence,
single process, no production guarantees.

```ts
import { withBackend } from 'bullmq';
import { createMemoryBackend } from '@bulllite/memory';

const { Queue, Worker } = withBackend(createMemoryBackend);
const connection = { namespace: 'demo' };
const queue = new Queue('emails', { connection });
const worker = new Worker('emails', async (job) => job.data, { connection });
await queue.add('send', { to: 'hello@example.com' });
```

Or keep your code unchanged and call `setDefaultBackendFactory(createMemoryBackend)`
in your test setup. Support matrix, store lifetime (`retain`, `resetMemoryStore`)
and limitations: https://github.com/KamelotWalker/BullLite#readme
