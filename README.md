# BullLite: run BullMQ without Redis

**`bullmq-memory-backend` is an in-memory backend for BullMQ 6. It lets you test code
that uses BullMQ `Queue`, `Worker` and `QueueEvents` without a Redis server, Docker or a
Redis mock.**

[![npm](https://img.shields.io/npm/v/bullmq-memory-backend/alpha?label=npm)](https://www.npmjs.com/package/bullmq-memory-backend)
[![CI](https://github.com/KamelotWalker/BullLite/actions/workflows/ci.yml/badge.svg)](https://github.com/KamelotWalker/BullLite/actions/workflows/ci.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue)](https://github.com/KamelotWalker/BullLite/blob/main/LICENSE)

**Short answer:** to test BullMQ code without Redis, install `bullmq-memory-backend` and
call `setDefaultBackendFactory(createMemoryBackend)` in your Vitest or Jest setup file.
Your application code keeps creating ordinary BullMQ classes with its usual Redis
connection options. Those options are then ignored and nothing connects to Redis. Jobs,
retries, delays, priorities, events and stalled-job recovery run in the test process,
through BullMQ's own classes.

It plugs into BullMQ's official `IQueueBackend` / `withBackend()` extension point (BullMQ
6.3.9+), the same mechanism BullMQ uses for its own Redis and PostgreSQL backends. It
does not emulate the Redis protocol or Lua scripts; it implements the backend interface
directly.

> [!WARNING]
> **Experimental alpha (0.1.x).** For tests and local development only: not durable,
> single process, no production guarantees.
>
> **Unofficial community project; not affiliated with BullMQ or Taskforce.sh.**

## When to use it

Use it when:

- unit or integration tests exercise code that adds jobs or runs BullMQ workers, and you
  do not want Redis, Docker or Testcontainers in CI;
- you want fast, deterministic queue tests: no network, every namespace starts empty and
  is released when its last instance closes;
- you test delays, retry backoff or stalled jobs with fake timers (`vi.useFakeTimers()`);
- you run a local dev script or demo that needs a working queue but no infrastructure.

Do not use it when:

- you need persistence, several processes, or production traffic: use Redis;
- your code relies on **FlowProducer / parent-child jobs, job schedulers / repeatable
  jobs, deduplication / debounce, rate limiting or metrics**. These throw
  `UnsupportedError`. For these, use real Redis, or BullMQ's Postgres backend on in-memory
  PGlite (see [Alternatives](#alternatives-compared)).

## Install

```bash
npm install -D bullmq bullmq-memory-backend
```

Requirements: `bullmq@^6.3.9` (6.3.9 is the first release with `withBackend()` and the
generic `BackendFactory` this package implements) and Node.js 20, 22 or 24. ESM and
CommonJS are both shipped. CI tests the oldest and newest BullMQ 6.x releases in that
range. A weekly CI run also tests new BullMQ releases.

## Quick start

```ts
import { withBackend } from 'bullmq';
import { createMemoryBackend } from 'bullmq-memory-backend';

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

## Test runner setup

### Vitest

```js
// vitest.config.js
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: { setupFiles: ['./test/bullmq-memory.setup.js'] },
});
```

```js
// test/bullmq-memory.setup.js
import { afterEach } from 'vitest';
import { setDefaultBackendFactory } from 'bullmq';
import { createMemoryBackend, resetMemoryStore } from 'bullmq-memory-backend';

// Every Queue/Worker/QueueEvents created in this test process uses the in-memory
// backend; Redis connection options are ignored.
setDefaultBackendFactory(createMemoryBackend);

afterEach(() => resetMemoryStore());
```

### Jest

```js
// jest.config.js
module.exports = {
  setupFilesAfterEnv: ['<rootDir>/bullmq-memory.setup.js'],
};
```

```js
// bullmq-memory.setup.js
const { setDefaultBackendFactory } = require('bullmq');
const { createMemoryBackend, resetMemoryStore } = require('bullmq-memory-backend');

setDefaultBackendFactory(createMemoryBackend);

afterEach(() => resetMemoryStore());
```

These two recipes were tested on Vitest 5.0.5 and Jest 30.5.2 (CommonJS), with
unchanged application modules that create `Queue` and `Worker` with Redis connection
options.

### Unchanged application code

The default backend is process-wide. Code that builds BullMQ classes with Redis options
runs unchanged:

```ts
import { Queue, Worker, setDefaultBackendFactory } from 'bullmq';
import { createMemoryBackend, resetMemoryStore } from 'bullmq-memory-backend';

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
  on it is open, and is **released when the last one closes**. Tests that close what
  they open get a fresh store automatically.
- `connection: { namespace, retain: true }` keeps the data after everything closed
  (Redis-like persistence within the process) until `resetMemoryStore(namespace)`.
- `resetMemoryStore(namespace?)` empties one (or every) namespace in place. Still-open
  instances keep working against the empty queues.
- `memoryNamespaces()` lists namespaces currently held in memory (leak checks).
- The store lives in the module instance, so each test worker process or thread has its
  own. With per-file module isolation (the Vitest and Jest default), each test file does
  too.

```ts
import { withBackend } from 'bullmq';
import { createMemoryBackend, memoryNamespaces, resetMemoryStore } from 'bullmq-memory-backend';

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

Per-`IQueueBackend`-member status (63 of 90 members Supported, 4 Experimental, 23
Unsupported): [docs/compatibility.md](https://github.com/KamelotWalker/BullLite/blob/main/docs/compatibility.md).

### Conformance against BullMQ's own test suite

`pnpm conformance` runs BullMQ's shared test files against this backend. The files are
unmodified and taken from the git tag of the installed `bullmq` version, the same way
BullMQ runs them against its Postgres backend. It is also a CI gate: every test recorded
as passing in
[`conformance/baseline.json`](https://github.com/KamelotWalker/BullLite/blob/main/conformance/baseline.json)
must keep passing. Latest local run on 6.4.0: **510 passed, 272 failed, 3 skipped**
(785 tests, 23 files). 256 failures are unsupported features above (flows, schedulers,
deduplication, rate limiting, metrics) and 16 are tests that talk to Redis directly;
none is attributed to a Supported feature. Details, gate rules and how to refresh the
baseline: [docs/conformance.md](https://github.com/KamelotWalker/BullLite/blob/main/docs/conformance.md).

## Fake timers

Delayed jobs, retry backoff and stalled-job checks run on your fake clock
(`vi.useFakeTimers()`), while backend replies do not depend on it. Advance with the
async APIs (`vi.advanceTimersByTimeAsync`), wait for worker/queue events, and do not
call `runAllTimers*` while a `Worker` is open. Guide, examples and limitations:
[docs/fake-timers.md](https://github.com/KamelotWalker/BullLite/blob/main/docs/fake-timers.md).

## Limitations (by design)

- **No persistence.** Everything lives in the memory of one Node.js process.
- **Single process.** Workers in other processes cannot see the jobs.
- Not for production traffic: no memory limits beyond BullMQ's retention options;
  completed/failed jobs are kept until removed, like on Redis.
- Every backend reply is delivered on a later macrotask (like a network reply).
  It uses a `MessageChannel`, so fake timers do not stall `queue.add()`. BullMQ's
  own timers (delays, backoff, stalled checks) still follow your fake clock; see
  [docs/fake-timers.md](https://github.com/KamelotWalker/BullLite/blob/main/docs/fake-timers.md).

## Alternatives compared

| Approach | Extra infrastructure | BullMQ feature coverage | Notes |
|---|---|---|---|
| Real Redis (local, Docker, Testcontainers) | Redis server or Docker | Full | The reference; slower start-up, shared state needs cleanup |
| `redis-memory-server` | Real `redis-server` binary (downloaded or built on first use) | Full | Still a separate Redis process per test run |
| BullMQ Postgres backend on [PGlite](https://pglite.dev) | WASM Postgres + `@electric-sql/pglite-socket` server | Includes flows, schedulers, deduplication, rate limiting | Maintained by BullMQ; setup notes below |
| Redis client mocks (e.g. `ioredis-mock`) | None | Not tested by this project | Emulates the Redis client, not BullMQ's backend |
| **`bullmq-memory-backend`** | **None** | Core queue/worker/events subset ([table](#what-is-supported)) | In-process, synchronous state transitions, immediate events |

### PGlite + BullMQ's Postgres backend in detail

A zero-infrastructure alternative already exists: BullMQ 6 ships a PostgreSQL
backend, and it runs on in-memory PGlite through `@electric-sql/pglite-socket`. It
needs the socket server's `maxConnections` multiplexing and
`SET search_path TO bullmq, public` after the migrations. That gives you BullMQ's
maintained SQL implementation, including flows, schedulers, deduplication and rate
limiting, which BullLite does not support.

BullLite's case is narrower: no WASM Postgres or socket server to bootstrap, all
state transitions run synchronously in-process, and event delivery is immediate,
which keeps tests fast and deterministic. Measured with
`examples/scripts/bench.mjs` (Node 24.22, Windows 11, one worker): 20 jobs from
first `add` to 20th `completed` in 1.6–7.8 ms (15 rounds), and `add` → `QueueEvents`
`completed` event in ≤ 0.3 ms per job. We have not benchmarked the PGlite setup ourselves.

## FAQ

### How do I test BullMQ without Redis?

Install `bullmq-memory-backend` as a dev dependency and call
`setDefaultBackendFactory(createMemoryBackend)` in your test setup file
([recipes](#test-runner-setup)). Your BullMQ `Queue`, `Worker` and `QueueEvents`
instances then run in memory.

### Is it a Redis mock?

No. It does not speak the Redis protocol and does not run BullMQ's Lua scripts. BullMQ
6 talks to its datastore through the `IQueueBackend` interface; this package implements
that interface in memory, so BullMQ's own classes, retry logic and events run unchanged.

### Does it work with Jest? With Vitest?

Yes, both; see [Test runner setup](#test-runner-setup). Jest uses the CommonJS entry
point. Fake timers are tested with Vitest; Jest's modern fake timers are expected to
behave the same but are not part of the test suite yet
([details](https://github.com/KamelotWalker/BullLite/blob/main/docs/fake-timers.md)).

### Does it work with NestJS or other frameworks that create queues for me?

Not tested yet. The default backend is process-wide, so libraries that construct BullMQ
classes from the same `bullmq` installation should pick it up once
`setDefaultBackendFactory(createMemoryBackend)` has run before they create their
queues. Please open an issue with your result.

### What happens if my code uses an unsupported feature?

The call rejects (or throws) an `UnsupportedError` whose message and `method` property
name the member or option, for example a `FlowProducer` call or a `limiter` worker
option. Nothing silently succeeds.

### Can I use it in production?

No. It has no persistence and no cross-process sharing. Use Redis, or BullMQ's Postgres
backend.

### Which BullMQ versions are supported?

`bullmq@^6.3.9`. Earlier releases, including all of BullMQ 5, do not have `withBackend()`,
so they cannot use it. BullMQ 7 will need a new major range of this package.

## API

| Export | Description |
|---|---|
| `createMemoryBackend` | `BackendFactory` to pass to `withBackend(createMemoryBackend)` or `setDefaultBackendFactory(createMemoryBackend)`. |
| `MemoryConnectionOptions` | Type of `connection` for this backend: `{ namespace?: string; retain?: boolean }`. Redis options are accepted and ignored (namespace `'default'`). |
| `resetMemoryStore(namespace?: string): void` | Empties one namespace, or all when called without an argument. Open instances keep working. |
| `memoryNamespaces(): string[]` | Names of namespaces currently held in memory. |
| `UnsupportedError` | Error class for unsupported BullMQ members/options; `method` names the member. |

## Version policy

- `bullmq` peer dependency: **`^6.3.9`**. CI runs the full suite and the conformance
  gate on 6.3.9, on the newest 6.x release (re-resolved weekly) and on the pinned
  development version. A BullMQ release that breaks the gate is excluded or fixed in a
  patch release; BullMQ 7 needs a new major range.
- Node.js: 20, 22, 24 (CI matrix).
- `0.x`: only the support matrix above is promised; behaviour may change between
  minor versions and is noted in
  [CHANGELOG.md](https://github.com/KamelotWalker/BullLite/blob/main/CHANGELOG.md).

## Development

```bash
corepack pnpm install
pnpm typecheck
pnpm test            # build + behaviour suite + README examples (compiled and executed)
pnpm conformance     # BullMQ's own suite as a gate against conformance/baseline.json
pnpm bullmq:use oldest   # or: latest | 6.x.y — reinstall the workspace with another bullmq
```

Design document (Turkish): [PROJECT.md](https://github.com/KamelotWalker/BullLite/blob/main/PROJECT.md).

## License

MIT
