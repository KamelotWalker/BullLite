// Pass 1 (pure recorder): which members does Queue construction + Queue.add touch, in order?
import { withBackend } from 'bullmq';
import { recordingFactory, type Call } from './record.ts';
const log: Call[] = [];
const defaults: Record<string, unknown> = {
  qualifiedName: 'ns:q', keys: {}, toKey: (t: string) => `ns:q:${t}`,
  on() {}, waitUntilReady: () => Promise.resolve(),
};
const { Queue } = withBackend(recordingFactory(null, log, { defaults }));
const q = new Queue('q', { connection: {} });
try { await q.add('j', { a: 1 }); } catch (e) { console.log('ERR', (e as Error).message); }
console.log(log.map((c) => `${c.kind}:${c.name}`).join('\n'));
process.exit(0);
