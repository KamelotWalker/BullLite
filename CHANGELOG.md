# Changelog

## @bulllite/memory 0.1.0-alpha.0 (unreleased)

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
