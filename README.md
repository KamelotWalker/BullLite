# BullLite

**Run real BullMQ 6 `Queue` / `Worker` / `QueueEvents` without a Redis server.**

`@bulllite/memory` is an in-memory backend for BullMQ's `IQueueBackend` /
`withBackend()` abstraction. Your code and your tests keep using BullMQ's own
classes; only the datastore underneath is swapped for a fast, deterministic,
in-process one. Built for test suites and local development.

> [!WARNING]
> **Experimental (v0.1.0-alpha).** No production guarantees. Not a Redis
> replacement, not durable, not multi-process.
>
> **Unofficial community project; not affiliated with BullMQ** or Taskforce.sh.

## Install

```bash
npm install --save-dev bullmq@6.4.0 @bulllite/memory
```

(`@bulllite/memory` is not published to npm yet; until then build it from this
repository: `pnpm install && pnpm build`.) ESM and CommonJS are both shipped;
Node.js 20, 22 and 24 are tested.

## Quick start

```ts
import { withBackend } from 'bullmq';
import { createMemoryBackend } from '@bulllite/memory';

const { Queue, Worker, QueueEvents } = withBackend(createMemoryBackend);
const connection = { namespace: 'demo' }; // instances with the same namespace share jobs

const queue = new Queue('emails', { connection });
const events = new QueueEvents('emails', { connection });
const worker = new Worker('emails', async (job) => ({ delivered: job.data.to }), { connection, concurrency: 2 });

const job = await queue.add('send', { to: 'hello@example.com' }, { attempts: 3, backoff: { type: 'exponential', delay: 100 } });
console.log(await job.waitUntilFinished(events)); // { delivered: 'hello@example.com' }

await worker.close();
await events.close();
await queue.close(); // nothing left open: the process exits by itself
```

## Using it in an existing test suite

If your code constructs BullMQ classes itself (with Redis connection options),
switch the process-wide default backend in your test setup file. The Redis
connection options are then ignored; nothing connects to Redis.

```ts
import { Queue, Worker, setDefaultBackendFactory } from 'bullmq';
import { createMemoryBackend, resetMemoryStore } from '@bulllite/memory';

setDefaultBackendFactory(createMemoryBackend); // e.g. in vitest/jest setupFiles

// Unchanged application code:
const connection = { host: 'localhost', port: 6379 }; // never contacted
const queue = new Queue('reports', { connection });
const worker = new Worker('reports', async (job) => `report ${job.data.id}`, { connection });

const completed = new Promise((resolve) => worker.once('completed', (_job, result) => resolve(result)));
await queue.add('build', { id: 42 }, { delay: 50 });
console.log(await completed); // report 42

await worker.close();
await queue.close();
resetMemoryStore(); // e.g. in afterEach: drop all in-memory data
setDefaultBackendFactory(); // back to Redis
```

### Isolation and store lifetime

- `connection.namespace` (default `'default'`) is the isolation domain. Inside a
  namespace, queues are keyed by `prefix` + queue name exactly like Redis keys.
- A namespace lives while at least one Queue/Worker/QueueEvents/QueueEventsProducer
  on it is open, and is **released when the last one closes** — so tests that close
  what they open get a fresh store automatically.
- `connection: { namespace, retain: true }` keeps the data after everything closed
  (Redis-like persistence within the process) until `resetMemoryStore(namespace)`.
- `resetMemoryStore(namespace?)` empties one (or every) namespace in place. Still-open
  instances keep working against the empty queues.
- `memoryNamespaces()` lists namespaces currently held in memory (leak checks).

```ts
import { withBackend } from 'bullmq';
import { createMemoryBackend, memoryNamespaces, resetMemoryStore } from '@bulllite/memory';

const { Queue } = withBackend(createMemoryBackend);

const first = new Queue('q', { connection: { namespace: 'kept', retain: true } });
await first.add('job', { n: 1 });
await first.close();

const second = new Queue('q', { connection: { namespace: 'kept' } });
console.log(await second.count()); // 1: retained after the first instance closed
await second.close();

resetMemoryStore('kept');
console.log(memoryNamespaces().includes('kept')); // false
```

## What is supported

Status terms (from the design doc): **Supported** = implemented and covered by
behavioural tests through the real BullMQ classes; **Experimental** = works but
with known semantic gaps; **Unsupported** = rejects with `UnsupportedError`
naming the member or option (never a silent success).

| Feature | Status | Notes |
|---|---|---|
| `Queue.add` / `addBulk`, Worker processing, `concurrency` | Supported | atomic claim; FIFO; several workers per queue |
| Custom `jobId` duplicates | Supported | existing id returned, not re-added; `duplicated` event |
| Completed / failed, `removeOnComplete` / `removeOnFail` (`true`, count, age) | Supported | job- or worker-level |
| Retries: `attempts`, fixed / exponential / custom backoff, `UnrecoverableError` | Supported | backoff is computed by BullMQ itself |
| Delayed jobs, `promote`, `changeDelay`, `promoteJobs` | Supported | idle workers sleep until the next due job |
| Priority, `lifo`, `changePriority` | Supported | Redis ordering rules |
| Getters: `getJob`, `getJobs`, `getJobCounts`, `getWaiting`/…, `count`, `getJobState`, logs, progress, `updateData` | Supported | ordering mirrors Redis lists/sorted sets |
| `QueueEvents`, `job.waitUntilFinished`, `QueueEventsProducer` | Supported | XREAD-shaped stream, monotonic ids, `lastEventId` resume, `maxLen` trimming |
| Pause / resume (queue and worker), `setGlobalConcurrency` | Supported | |
| `remove`, `clean`, `drain`, `obliterate`, `retryJobs`, `job.retry` | Supported | |
| Lock renewal, stalled-job recovery, `maxStalledCount` | Supported | Redis two-phase mark/sweep semantics |
| `getWorkers` / `getWorkersCount` | Supported | open workers of the namespace |
| Job cancellation (`worker.cancelJob`) | Supported | BullMQ feature; covered by BullMQ's own suite |
| `moveToWait` (manual rate-limit path), `getRateLimitTtl` | Experimental | no limiter exists, so the TTL is always "not limited" |
| Sandboxed (file) processors, OpenTelemetry | Not tested | |
| FlowProducer / parent-child jobs | Unsupported | |
| Job schedulers / repeatable jobs | Unsupported | |
| Deduplication / debounce | Unsupported | |
| Rate limiting (`limiter`, `setGlobalRateLimit`, `rateLimit`) | Unsupported | |
| Metrics (`metrics` option, `getMetrics`) | Unsupported | |

Per-`IQueueBackend`-member status: [docs/compatibility.md](docs/compatibility.md).

### Conformance against BullMQ's own test suite

`pnpm conformance` runs BullMQ v6.4.0's shared test files (unmodified) against
this backend, the same way BullMQ runs them against its Postgres backend. Latest
local run: **510 passed, 272 failed, 3 skipped** (785 tests, 23 files). 256 failures
are unsupported features above (flows, schedulers, deduplication, rate limiting,
metrics) and 16 are tests that talk to Redis directly; none is attributed to a
Supported feature. Details and the per-file table: [docs/conformance.md](docs/conformance.md).

## Limitations (by design)

- **No persistence.** Everything lives in the memory of one Node.js process.
- **Single process.** Workers in other processes (or sandboxed child processes
  talking to their own backend) cannot see the jobs.
- Not for production traffic: no memory limits beyond BullMQ's retention options;
  completed/failed jobs are kept until removed, like on Redis.
- Every backend reply is delivered on a later macrotask (like a network reply),
  using a `MessageChannel` so that fake timers (`vi.useFakeTimers()`, Jest, sinon)
  do not stall `queue.add()`. BullMQ's own timers (delays, backoff, stalled checks)
  still follow your fake clock.

## How does this compare to PGlite + BullMQ's Postgres backend?

A zero-infrastructure alternative already exists: BullMQ 6 ships a PostgreSQL
backend, and it runs on in-memory [PGlite](https://pglite.dev) through
`@electric-sql/pglite-socket` (it needs the socket server's `maxConnections`
multiplexing and `SET search_path TO bullmq, public` after the migrations). That
gives you BullMQ's maintained SQL implementation, including flows, schedulers,
deduplication and rate limiting, which BullLite does not support.

BullLite's case is narrower: no WASM Postgres or socket server to bootstrap, all
state transitions run synchronously in-process, and event delivery is immediate,
which keeps tests fast and deterministic. Measured with
`examples/scripts/bench.mjs` (Node 24.22, Windows 11, one worker): 20 jobs from
first `add` to 20th `completed` in 1.6–7.8 ms (15 rounds), and `add` → `QueueEvents`
`completed` event in ≤ 0.3 ms per job. We have not benchmarked the PGlite setup ourselves.

## Version policy

- `bullmq` peer dependency: **exactly `6.4.0`**, the only version tested. The range
  widens only after the suite and the conformance run pass on another release.
- Node.js: 20, 22, 24 (CI matrix).
- `0.x`: only the support matrix above is promised; behaviour may change between
  minor versions and is noted in [CHANGELOG.md](CHANGELOG.md).

## Development

```bash
corepack pnpm install
pnpm typecheck
pnpm test            # build + behaviour suite + README examples (compiled and executed)
pnpm conformance     # BullMQ's own suite (clones taskforcesh/bullmq@v6.4.0, or set BULLMQ_SRC)
```

Design document (Turkish): [PROJECT.md](PROJECT.md).

## License

MIT
