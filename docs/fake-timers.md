# Fake timers

Delayed jobs, retry backoff, lock renewal and stalled-job checks are all driven by
BullMQ's own timers (`setTimeout`/`setInterval` inside `Worker`, `Job` and the backend's
blocking wait) and by `Date.now()`. With the memory backend these follow your fake clock,
so a test can run an hour-long delay or a 30-second stalled check in milliseconds.

Backend replies themselves are delivered over a `MessageChannel`, which fake-timer
libraries do not patch: `queue.add()`, getters and `worker.waitUntilReady()` resolve
without advancing the clock.

Everything below is exercised by
[`packages/memory/test/fake-timers.test.ts`](../packages/memory/test/fake-timers.test.ts)
(vitest), on every `bullmq` version of the supported range in CI.

## The rules

1. **Use the async clock APIs**: `vi.advanceTimersByTimeAsync(ms)` and
   `vi.runOnlyPendingTimersAsync()` (Jest: `jest.advanceTimersByTimeAsync`,
   `jest.runOnlyPendingTimersAsync`). They yield to the real event loop between timers,
   which is what lets the backend replies triggered by one timer arrive before the next
   one fires. The synchronous `vi.advanceTimersByTime(ms)` fires every timer in one go:
   the worker never sees the replies, lock renewals arrive too late and jobs lose their
   lock (`Missing lock for job …`).
2. **Do not use `runAllTimers` / `runAllTimersAsync` while a `Worker` is open.** The
   stalled checker and the lock manager re-arm their timers forever, so the fake-timer
   library aborts with "Aborting after running 10000 timers, assuming an infinite loop!".
   This is BullMQ behaviour and happens with Redis as well. Advance by an explicit amount
   instead.
3. **Wait for events, not for time.** After advancing, `await once(worker, 'completed')`
   (or `'failed'`, `'active'`, a `QueueEvents` event, `job.waitUntilFinished(queueEvents)`)
   instead of polling helpers that sleep with `setTimeout` — those sleeps are fake too.
4. **Restore real timers before closing** instances, e.g. call `vi.useRealTimers()` in an
   `afterEach` that runs before the one that closes queues and workers.

## Delayed job

```ts
import { once } from 'node:events';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { withBackend } from 'bullmq';
import { createMemoryBackend } from 'bullmq-memory-backend';

const { Queue, Worker } = withBackend(createMemoryBackend);

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

it('sends the reminder after one hour', async () => {
  const connection = { namespace: 'reminders' };
  const queue = new Queue('reminders', { connection });
  const worker = new Worker('reminders', async () => 'sent', { connection });
  await worker.waitUntilReady();

  const job = await queue.add('remind', { userId: 1 }, { delay: 60 * 60_000 });
  await vi.advanceTimersByTimeAsync(59 * 60_000);
  expect(await job.getState()).toBe('delayed');

  const completed = once(worker, 'completed');
  await vi.advanceTimersByTimeAsync(60_000);
  await completed;

  vi.useRealTimers();
  await worker.close();
  await queue.close();
});
```

## Retries with backoff

```ts
let failed = once(worker, 'failed');
await queue.add('charge', {}, { attempts: 3, backoff: { type: 'exponential', delay: 10_000 } });
await failed;                                // attempt 1 fails at t = 0
failed = once(worker, 'failed');
await vi.advanceTimersByTimeAsync(10_000);   // attempt 2 at t = 10 s
await failed;
await vi.advanceTimersByTimeAsync(20_000);   // attempt 3 at t = 30 s (10 s * 2^1)
```

`Date.now()` inside the processor is the fake clock, so attempt timestamps are exact.

## Stalled jobs

With the defaults (`lockDuration` 30 s, `stalledInterval` 30 s), a job whose worker died
is moved back to `wait` by another worker's stalled checker within 90 s of fake time
(two-phase mark/sweep, as on Redis), then reprocessed:

```ts
const stalled = once(queueEvents, 'stalled');
await vi.advanceTimersByTimeAsync(90_000);
await stalled;
```

A job that is still being processed is *not* reported as stalled, however far you
advance: lock renewal runs on the same fake clock (covered by the test file).

## Jest

The test suite of this repository runs on vitest; Jest itself is not run in CI. Both use
`@sinonjs/fake-timers`, and Jest's default set of faked APIs additionally includes
`process.nextTick` and `queueMicrotask`. A vitest run with exactly that set (Date,
timers, `setImmediate`, `performance`, `queueMicrotask`, `nextTick`) passes the
delayed-job scenario above, so the same rules should apply to `jest.useFakeTimers()`;
please open an issue if they do not. CommonJS (Jest's default module system) is
supported through the package's `require` entry point.

## Limitations

- `runAllTimers` / `runAllTimersAsync` cannot be used with an open `Worker` (rule 2).
- Synchronous clock advancement does not work (rule 1).
- Legacy Jest fake timers (`jest.useFakeTimers({ legacyFakeTimers: true })`) do not fake
  `Date`, so delayed jobs and backoff would never become due: not supported.
