// Pass 2: ordered call log of the real vertical path against the memory backend.
// Scenario: idle worker -> add (completes) -> add (throws) -> getJob/getState -> close.
// ALLOW=a,b,c restricts callable members; anything else throws NotImplemented (strict re-run).
import { withBackend } from 'bullmq';
import { createMemoryBackend } from '../src/index.ts';
import { recordingFactory, type Call } from './record.ts';

const log: Call[] = [];
const allow = process.env.ALLOW ? new Set(process.env.ALLOW.split(',')) : undefined;
const { Queue, Worker } = withBackend(recordingFactory(createMemoryBackend, log, { allow }));
const connection = { namespace: 'measure' };
const mark = (s: string) => log.push({ seq: -1, who: '--', kind: 'call', name: `# ${s}` });

const queue = new Queue('q', { connection });
const worker = new Worker('q', async (job) => {
  if (job.name === 'bad') throw new Error('boom');
  return { echoed: job.data };
}, { connection });
worker.on('error', (e) => console.log('worker error', e.message));
const finished = (ev: 'completed' | 'failed') => new Promise<void>((r) => worker.once(ev as 'completed', () => r()));

await worker.waitUntilReady();
await new Promise((r) => setTimeout(r, 200)); // worker idles inside waitForJob
mark('add good job');
const c1 = finished('completed');
const good = await queue.add('good', { a: 1 });
await c1;
mark('add failing job');
const f1 = finished('failed');
const bad = await queue.add('bad', {});
await f1;
mark('read back');
const j = await queue.getJob(good.id!);
console.log('good', await j!.getState(), j!.returnvalue);
const k = await queue.getJob(bad.id!);
console.log('bad', await k!.getState(), k!.failedReason);
mark('close');
await worker.close();
await queue.close();
console.log(log.map((c) => `${c.seq}\t${c.who}\t${c.kind}\t${c.name}`).join('\n'));
