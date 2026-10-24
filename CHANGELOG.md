# Changelog

## bullmq-memory-backend 0.1.0-alpha.2

Documentation and package metadata only; no runtime change.

- The npm package README is now the full repository README (copied at build time), with
  Vitest and Jest setup recipes (tested on Vitest 5.0.5 and Jest 30.5.2), when-to-use
  guidance, a comparison with other ways to test BullMQ without Redis, an FAQ and an API
  table. Links point to GitHub so they work on npmjs.com.
- `llms.txt` at the repository root for AI assistants and crawlers.
- Package description and keywords describe the use case ("run BullMQ without Redis",
  Vitest, Jest).

## bullmq-memory-backend 0.1.0-alpha.1

- **Peer dependency widened from exactly `bullmq@6.4.0` to `^6.3.9`.** 6.3.9 is the first
  release with `withBackend()` and the generic `BackendFactory`; every release from 6.3.9 to
  6.4.0 typechecks and passes the behaviour suite. Evidence: `docs/compatibility.md`.
- CI: tests run on the pinned, the oldest (6.3.9) and the newest 6.x BullMQ on Node 20/22/24,
  plus a weekly scheduled run that picks up new BullMQ releases (`pnpm bullmq:use`).
- BullMQ's own test suite is now a CI gate: the tests recorded in `conformance/baseline.json`
  (500 names on 6.4.0, plus 1 timing-dependent upstream test listed as ignored with its reason)
  must keep passing; new upstream tests and tests absent from other BullMQ versions do not fail
  it. The harness runs the suite of the installed BullMQ version. Refresh procedure:
  `docs/conformance.md`.
- Fake timers guide (`docs/fake-timers.md`) and tests: delayed jobs, exponential backoff,
  stalled-job recovery and lock renewal on `vi.useFakeTimers()`; documented limitations
  (async clock APIs only, no `runAllTimers*` with an open Worker).

## bullmq-memory-backend 0.1.0-alpha.0

First experimental release. Tested with `bullmq@6.4.0` on Node.js 20, 22, 24.

- In-memory `IQueueBackend` for BullMQ 6: Queue/Worker/QueueEvents/QueueEventsProducer.
- Supported: add/addBulk, concurrency, duplicate job ids, retries with fixed/exponential/custom
  backoff, delayed jobs, priority/lifo, job queries, QueueEvents + `waitUntilFinished`,
  pause/resume, remove/clean/drain/obliterate, retryJobs/job.retry, retention options,
  lock renewal and stalled recovery, global concurrency, getWorkers.
- Unsupported (fail with `UnsupportedError`): flows, job schedulers, deduplication,
  rate limiting, metrics.
- Store lifetime: namespaces are reference-counted and released when the last instance
  closes; `connection.retain`, `resetMemoryStore()`, `memoryNamespaces()`.
- Dual ESM/CommonJS build.
- Redis parity fixes from review: clean() ages jobs by finishedOn/processedOn/timestamp per state;
  expired locks cannot be renewed; moveToWait enforces the lock token; stalled jobs return to the
  head of wait with a `waiting` event; priority-front ties order by job id; `'$'` is the stream
  tail when the read is issued.
