// Small, reproducible timing check for the README (not a load test).
//   node examples/scripts/bench.mjs      (after `pnpm build`)
// 1. 20 jobs: time from the first Queue.add to the 20th `completed` (one worker, concurrency 1).
// 2. QueueEvents: per-job latency from Queue.add to the QueueEvents `completed` event (20 jobs, median/max).
import { withBackend } from 'bullmq';
import { createMemoryBackend } from 'bullmq-memory-backend';

const { Queue, Worker, QueueEvents } = withBackend(createMemoryBackend);
const connection = { namespace: 'bench' };
const queue = new Queue('bench', { connection });
const events = new QueueEvents('bench', { connection });
const worker = new Worker('bench', async (job) => job.data.i, { connection });
await Promise.all([queue.waitUntilReady(), events.waitUntilReady(), worker.waitUntilReady()]);

const runs = [];
for (let round = 0; round < 5; round++) {
  let done = 0;
  const finished = new Promise((resolve) => {
    const onCompleted = () => {
      if (++done === 20) {
        worker.off('completed', onCompleted);
        resolve();
      }
    };
    worker.on('completed', onCompleted);
  });
  const started = performance.now();
  for (let i = 0; i < 20; i++) await queue.add('j', { i });
  await finished;
  runs.push(performance.now() - started);
}

const latencies = [];
for (let i = 0; i < 20; i++) {
  // Jobs run one at a time, so the next `completed` on the stream is this job's.
  const delivered = new Promise((resolve) => events.once('completed', resolve));
  const started = performance.now();
  await queue.add('e', { i });
  await delivered;
  latencies.push(performance.now() - started);
}
latencies.sort((a, b) => a - b);

await worker.close();
await events.close();
await queue.close();

const fmt = (ms) => `${ms.toFixed(1)} ms`;
console.log(`node ${process.version}`);
console.log(`20 jobs add -> all completed: ${runs.map(fmt).join(', ')}`);
console.log(`QueueEvents add -> completed event: median ${fmt(latencies[10])}, max ${fmt(latencies[19])}`);
