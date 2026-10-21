# BullMQ's own test suite on `@bulllite/memory`

`pnpm conformance` (script: `conformance/run.mjs`) copies `src/` and `tests/` of a
`taskforcesh/bullmq` checkout at tag `v6.4.0` (cloned, or `BULLMQ_SRC=<path>`) into
`conformance/.work/` without modifying them, takes the generated Lua/SQL script
modules from the published `bullmq@6.4.0` tarball, and runs the shared test files
with BullMQ's own non-Redis pattern (`vitest.postgres.setup.ts`): an inert test
connection plus `setDefaultBackendFactory(createMemoryBackend)` with one retained
namespace per test-file process.

## Latest local run (Node 24.22, Windows 11, 2026-10-21)

**510 passed, 272 failed, 3 skipped** (785 tests in 23 files, ~75 s). Re-run after the review fixes (redis-parity tests): unchanged.

Failure breakdown (classified from the failure messages):

| Cause | Tests |
|---|---:|
| Unsupported feature (`UnsupportedError`: flows, job schedulers, deduplication, rate limiting, metrics) | 252 |
| Test talks to Redis directly (raw client commands, stubs on ioredis internals, real Redis constructors) | 16 |
| Flow children added from inside a processor (the add rejects with `UnsupportedError` in the processor; the test then times out waiting) | 4 |

No failure is attributed to a feature the README lists as Supported. Files with
zero failures: `delay`, `job_cancellation`, `job_cancellation_advanced`,
`lock_manager`, `obliterate`. Bugs this harness found and that are fixed:
BullMQ 6 pause semantics (paused jobs stay in `wait`), `retryJob` promoting due
delayed jobs first, `changePriority` with `lifo` inside a priority group, and
microtask-ordering assumptions (replies now arrive on a later macrotask and new
instances "connect" one macrotask after construction).

## Per file

| File | Passed | Failed | Skipped |
|---|---:|---:|---:|
| bulk.test.ts | 3 | 1 | 0 |
| clean.test.ts | 22 | 11 | 0 |
| concurrency.test.ts | 7 | 3 | 0 |
| constructor-options.test.ts | 80 | 1 | 0 |
| deduplication.test.ts | 0 | 36 | 0 |
| delay.test.ts | 11 | 0 | 0 |
| events.test.ts | 16 | 1 | 0 |
| flow.test.ts | 2 | 75 | 0 |
| getters.test.ts | 34 | 9 | 0 |
| job_cancellation_advanced.test.ts | 21 | 0 | 0 |
| job_cancellation.test.ts | 23 | 0 | 0 |
| job_scheduler_stress.test.ts | 0 | 6 | 1 |
| job_scheduler.test.ts | 7 | 66 | 0 |
| job.test.ts | 76 | 1 | 0 |
| lock_manager.test.ts | 19 | 0 | 0 |
| metrics.test.ts | 0 | 4 | 0 |
| obliterate.test.ts | 2 | 0 | 0 |
| pause.test.ts | 12 | 2 | 0 |
| queue.test.ts | 20 | 10 | 0 |
| rate_limiter.test.ts | 0 | 26 | 0 |
| stalled_jobs.test.ts | 10 | 4 | 0 |
| telemetry_interface.test.ts | 22 | 9 | 0 |
| worker.test.ts | 123 | 7 | 2 |
| **Total** | **510** | **272** | **3** |

## Not run

| File | Reason |
|---|---|
| adapter-conformance.test.ts | IRedisClient adapter suites (need Redis/Bun) |
| async_fifo_queue.test.ts | does not exercise a backend (pure unit test) |
| bun-adapter-suite.test.ts | IRedisClient adapter suites (need Redis/Bun) |
| bun-redis-nested-duplicate.test.ts | IRedisClient adapter suites (need Redis/Bun) |
| bun-redis.test.ts | IRedisClient adapter suites (need Redis/Bun) |
| child-pool.test.ts | does not exercise a backend (pure unit test) |
| child_stdio_null.test.ts | does not exercise a backend (pure unit test) |
| clean.redis.test.ts | raw Redis commands / Redis key layout |
| cluster.test.ts | Redis client/connection specific |
| connection-closed-error.test.ts | Redis client/connection specific |
| connection.test.ts | Redis client/connection specific |
| create-backend.test.ts | Redis client/connection specific |
| delay.redis.test.ts | raw Redis commands / Redis key layout |
| events.redis.test.ts | raw Redis commands / Redis key layout |
| flow.redis.test.ts | raw Redis commands / Redis key layout |
| getters.redis.test.ts | raw Redis commands / Redis key layout |
| ioredis-client.test.ts | Redis client/connection specific |
| job.redis.test.ts | raw Redis commands / Redis key layout |
| job_scheduler.redis.test.ts | raw Redis commands / Redis key layout |
| node-redis.test.ts | Redis client/connection specific |
| obliterate.redis.test.ts | raw Redis commands / Redis key layout |
| postgres-listen-client-recovery.test.ts | PostgreSQL backend internals |
| postgres-queue-backend.test.ts | PostgreSQL backend internals |
| queue.redis.test.ts | raw Redis commands / Redis key layout |
| relay-resilience.test.ts | IRelayBackend (Relay class) is out of scope |
| relay.ioredis.test.ts | Redis client/connection specific |
| relay.test.ts | IRelayBackend (Relay class) is out of scope |
| relay_stress.test.ts | IRelayBackend (Relay class) is out of scope |
| sandboxed_process.test.ts | excluded by BullMQ’s default config too (ioredis-only run) |
| script_loader.test.ts | Redis Lua script loader |
| scripts.test.ts | Redis Lua script loader |
| valkey-glide-client.test.ts | Redis client/connection specific |
| worker.ioredis.test.ts | Redis client/connection specific |
| worker.redis.test.ts | raw Redis commands / Redis key layout |
