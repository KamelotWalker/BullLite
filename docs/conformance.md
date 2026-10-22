# BullMQ's own test suite on `bullmq-memory-backend`

`pnpm conformance` (script: `conformance/run.mjs`) copies `src/` and `tests/` of a
`taskforcesh/bullmq` checkout at the git tag of the **installed** `bullmq` version
(`v<version>`, cloned into `conformance/.work/bullmq-src-<version>`, or
`BULLMQ_SRC=<path>`) into `conformance/.work/` without modifying them, takes the
generated Lua/SQL script modules from the published tarball of the same version, and
runs the shared test files with BullMQ's own non-Redis pattern
(`vitest.postgres.setup.ts`): an inert test connection plus
`setDefaultBackendFactory(createMemoryBackend)` with one retained namespace per
test-file process. Switch the version with `pnpm bullmq:use oldest|latest|6.x.y`.

## The gate

[`conformance/baseline.json`](../conformance/baseline.json) lists, per test file, the
names of the tests that passed when it was generated (BullMQ version and date are in the
file). `pnpm conformance` fails (exit code 1) when:

- a baseline test runs and fails, and fails again when its file is re-run once
  (timing-sensitive upstream tests get one retry; a test that passes on the re-run is
  reported as flaky);
- a test file that has baseline tests errors before running them (import/collection error);
- the installed BullMQ version **equals** the baseline's version and a baseline test did
  not run at all (guards against tests disappearing silently, e.g. a harness exclusion
  that matches too much).

It does not fail for tests outside the baseline (new upstream tests, unsupported
features), nor for baseline tests that do not exist in another BullMQ version (renamed,
removed, or not yet added upstream); both are listed in the report
(`conformance/.work/summary.md`, also the CI job summary). Same-named tests inside a file
count as one name that passes only if every occurrence passes.

`ignored` in the same file lists tests whose outcome depends on timing rather than on the
backend, each with a reason; they are never gated and survive a refresh. Currently one:
`worker.test.ts` "should retry job after a delay time, keeping the current step" asserts
`Date.now() - start > 200` after a 200 ms `moveToDelayed`; an in-process backend can
finish in exactly 200 ms (reproduced in 4 of 24 local runs under load; it failed twice in
the first CI run). Add entries only with such a reproduction, never to hide a bug.

Baseline on 6.4.0: **500 gated names** + 1 ignored, covering the 510 passing tests.
Three full runs in parallel (CPU contention) passed every gated test.

CI runs the gate on Node 22 for the pinned development version, the oldest version of the
peer range and the newest 6.x release; the weekly scheduled run re-resolves "newest".

### Refreshing the baseline

When a change makes more tests pass, or after moving the pinned `bullmq` devDependency to
a new release:

```bash
pnpm install                            # the pinned version (or: pnpm bullmq:use x.y.z)
pnpm conformance --update-baseline      # full run only; rewrites conformance/baseline.json
git diff conformance/baseline.json      # removals are regressions: explain them or fix them
```

Generate the baseline from the pinned version so that the strict "same version" rule
applies to everyday CI runs. A test that genuinely cannot pass any more (an upstream test
rewritten to need Redis, say) is removed by the refresh; say why in the commit message.

Verified 2026-10-22: with `promote()` broken on purpose (`job.test.ts` + `delay.test.ts`
run), the gate reported 5 regressions and exited 1; on the unmodified backend it passes on
6.4.0 (510 passed) and on 6.3.9 (504 passed; 6 baseline tests do not exist in 6.3.9's suite).

## Latest local run on 6.4.0 (Node 24.22, Windows 11, 2026-10-22)

**510 passed, 272 failed, 3 skipped** (785 tests in 23 files, ~75 s); unchanged since 2026-10-21.

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
