# BullMQ 6.4.0 backend contract — compatibility inventory

English, source-verified against `bullmq@6.4.0` (git tag `v6.4.0`, commit `253bc4b1acd9c6030c01817b38ae65e067666ed4`, https://github.com/taskforcesh/bullmq/tree/v6.4.0).

§0 is the **current** status of `bullmq-memory-backend` (v0.1.0-alpha.1). §1–§8 are the Phase 0 contract inventory; their "Prio" columns are the original plan and are kept for reference.

**Supported BullMQ versions: `^6.3.9`** (peer dependency since 0.1.0-alpha.1; 0.1.0-alpha.0 required exactly 6.4.0). How the lower bound was established (2026-10-22, from the published npm tarballs):

| BullMQ | Result |
|---|---|
| 6.0.0, 6.1.0, 6.2.0, 6.3.0 – 6.3.8 | Not supported: no `withBackend()` (`dist/esm/utils/with-backend.js` absent). In 6.3.8 `BackendFactory` / `QueueBaseOptions` are also not generic over the connection options: `tsc --noEmit` of this package against 6.3.8 reports 55 errors (e.g. `TS2315: Type 'QueueBaseOptions' is not generic`). |
| 6.3.9 – 6.4.0 (12 releases) | `dist/esm/interfaces/queue-backend.d.ts` and `dist/esm/utils/create-backend.d.ts` are byte-identical across all 12. For each release: typecheck clean, full behaviour suite and README examples green. BullMQ's shared suite passes the conformance gate on 6.3.9 (504 passed) and 6.4.0 (510 passed). |

CI keeps checking both ends of the range: the oldest version (6.3.9) and the newest 6.x release, re-resolved by a weekly scheduled run. §1–§8 below are verified against 6.4.0 only; line numbers may differ in other releases.

Conventions: `qb:N` = `src/interfaces/queue-backend.ts:N`; `rel:N` = `src/interfaces/relay-backend.ts:N`; all other paths are relative to `src/` of the tag. `pg:N` = `src/postgres/postgres-queue-backend.ts:N`. Caller line numbers come from a scripted scan of `src/classes/*.ts` and `src/utils/*.ts` (excluding the Redis/Postgres backends themselves); a member showing no caller is exposed publicly by a high-level class only through a path I did not trace (marked "—").

Honesty rule (PROJECT.md): a member that is not implemented MUST throw a clear `unsupported` error. The Postgres backend itself does this via `notImplemented()` (`pg:160`).

---

## 0. `bullmq-memory-backend` v0.1.0-alpha.1 — per-member status

**Supported** = real behaviour, exercised through the real BullMQ classes by `packages/memory/test` and/or BullMQ's own shared suite (`docs/conformance.md`). **Experimental** = implemented, but with a documented semantic gap. **Unsupported** = rejects with `UnsupportedError` (`.method` names the member); `forQueue` throws synchronously. Totals: **63 Supported, 4 Experimental, 23 Unsupported = 90**.

Option-level rejections (also `UnsupportedError`, nothing is stored): `addJob`/`addJobs` with `parent`/`parentKey` (flows), `deduplication`/`debounce`, a scheduler `repeatJobKey`; `setQueueMeta` with `max`/`duration` (`Queue.setGlobalRateLimit`); Worker options `limiter` and `metrics` (thrown by the factory, i.e. by `new Worker(...)`).

| # | Member | Status | Notes |
|---|---|---|---|
| 1 | `waitUntilReady` | Supported | resolves one macrotask after construction (simulated handshake; ops issued earlier are queued, like ioredis' offline queue) |
| 2 | `close` | Supported | interrupts `waitForJob`/`readEvents`, releases the namespace reference |
| 3 | `closing` | Supported | |
| 4 | `disconnect` | Supported | = `close(true)` |
| 5 | `setName` | Supported | name is reported by `getClientList` |
| 6 | `minimumBlockTimeout` | Supported | 0.001 s |
| 7 | `maximumBlockTimeout` | Supported | 3600 s (the wait re-arms to the next delayed job itself) |
| 8–10 | `on` / `once` / `removeListener` | Supported | EventEmitter; emits `close` |
| 11 | `forQueue` | Unsupported | FlowProducer only |
| 12 | `qualifiedName` | Supported | `<prefix>:<queue>` like Redis |
| 13 | `keys` | Supported | BullMQ `QueueKeys` for the prefix |
| 14 | `toKey` | Supported | |
| 15 | `parseNodeKey` | Experimental | pure string helper; only flows use it, and flows are unsupported |
| 16 | `clientName` | Supported | |
| 17 | `addJob` | Supported | wait / prioritized / delayed routing, `lifo`, duplicate id → existing id + `duplicated` event |
| 18 | `addJobs` | Supported | all-or-nothing validation |
| 19 | `addFlow` | Unsupported | |
| 20 | `addJobScheduler` | Unsupported | |
| 21 | `moveToActive` | Supported | promotes due delayed jobs; honours pause and global concurrency; empty result carries the next delayed timestamp |
| 22 | `moveToCompleted` | Supported | lock/state checks with BullMQ error codes −1/−2/−3/−6, retention, fused fetch-next, `drained` |
| 23 | `moveToFailed` | Supported | same + `retries-exhausted` |
| 24 | `moveToDelayed` | Supported | backoff retries and `job.moveToDelayed` |
| 25 | `moveToWaitingChildren` | Unsupported | |
| 26 | `moveJobFromActiveToWait` | Experimental | Redis `removeLock` check (−2 missing/expired, −6 other owner), then re-queues at the head; always reports "no limiter window" (0) because rate limiting is unsupported |
| 27 | `retryJob` | Supported | promotes due delayed jobs first, like `retryJob-11.lua` |
| 28 | `retryFinishedJob` | Supported | `job.retry()` incl. `resetAttemptsMade/Started` |
| 29 | `promote` | Supported | |
| 30 | `moveStalledJobsToWait` | Supported | two-phase mark/sweep, throttled by `stalledInterval`, deferred failure past `maxStalledCount`; recovered job goes to the head of `wait` (even if prioritized) with `waiting` then `stalled` events, like `moveJobToWait(…, "RPUSH")` |
| 31 | `retryFinishedJobs` | Supported | `Queue.retryJobs` |
| 32 | `promoteJobs` | Supported | |
| 33 | `pause` | Supported | BullMQ 6 semantics: meta flag only, jobs stay in `wait` |
| 34 | `drain` | Supported | |
| 35 | `cleanJobsByState` | Supported | age = first set of `finishedOn`/`processedOn`/`timestamp` (wait, active), `processedOn`/`timestamp` (delayed), `timestamp` (prioritized), `finishedOn` (completed/failed), as in `cleanJobsInSet-3.lua`; locked active jobs skipped; `cleaned` event |
| 36 | `obliterate` | Supported | requires pause; refuses active jobs unless `force`; resets the id counter |
| 37 | `removeOrphanedJobs` | Supported | returns 0: a job and its state are one record, orphans cannot exist |
| 38 | `extendLock` | Supported | an expired lock cannot be renewed (Redis key TTL) |
| 39 | `extendLocks` | Supported | same; also clears the stalled mark, like Redis |
| 40 | `updateData` | Supported | |
| 41 | `updateProgress` | Supported | `progress` event |
| 42 | `addLog` | Supported | `keepLogs` trimming |
| 43 | `clearLogs` | Supported | |
| 44 | `changeDelay` | Supported | |
| 45 | `changePriority` | Supported | `lifo` moves to the head of the priority group; several head jobs tie-break by job id like Redis equal-score sorted-set members |
| 46 | `remove` | Supported | 0 while locked; `removeChildren` has no effect (no flows) |
| 47 | `removeUnprocessedChildren` | Unsupported | |
| 48 | `removeChildDependency` | Unsupported | |
| 49 | `removeDeduplicationKey` | Unsupported | |
| 50 | `deleteDeduplicationKey` | Unsupported | |
| 51–57 | job-scheduler members (`updateJobSchedulerNextMillis`, `removeJobScheduler`, `getJobScheduler`, `isJobScheduler`, `getJobSchedulerData`, `getJobSchedulersRange`, `getJobSchedulersCount`) | Unsupported | 7 members |
| 58 | `getState` | Supported | incl. `prioritized` |
| 59 | `isFinished` | Supported | `waitUntilFinished` |
| 60 | `isMaxed` | Supported | |
| 61 | `isJobInState` | Supported | `paused` is always false (BullMQ 6 keeps paused jobs in `wait`) |
| 62 | `getJobData` | Supported | returns a copy |
| 63 | `getDeduplicationJobId` | Unsupported | |
| 64 | `getJobLogs` | Supported | |
| 65 | `getRateLimitTtl` | Experimental | no limiter can exist: 0 with `maxJobs`, else −2 (Redis PTTL of a missing key) |
| 66 | `getCounts` | Supported | |
| 67 | `getCountsPerPriority` | Supported | |
| 68 | `getRanges` | Supported | Redis list/zset ordering and `asc` semantics |
| 69 | `getDependencyCounts` | Unsupported | |
| 70 | `getDependencies` | Unsupported | |
| 71 | `getProcessedChildrenValues` | Unsupported | |
| 72 | `getIgnoredChildrenFailures` | Unsupported | |
| 73 | `getMetrics` | Unsupported | |
| 74 | `getClientList` | Supported | open Workers (`:w:<name>`) and QueueEvents of the namespace |
| 75 | `paginate` | Unsupported | |
| 76 | `setQueueMeta` | Supported | except `max`/`duration` (rate limit) |
| 77–81 | `getQueueMetaField`, `getQueueMetaFields`, `getQueueMeta`, `removeQueueMetaFields`, `hasQueueMetaField` | Supported | 5 members |
| 82 | `setRateLimit` | Unsupported | |
| 83 | `removeRateLimitKey` | Experimental | returns 0: the key can never exist |
| 84 | `removeDeprecatedPriorityKey` | Supported | returns 0: the legacy key never exists |
| 85 | `trimEvents` | Supported | |
| 86 | `publishEvent` | Supported | |
| 87 | `readEvents` | Supported | XREAD shape; `'$'` = stream tail when the read is issued; resume by id; blocking with timeout; trimmed to `streams.events.maxLen` |
| 88 | `waitForJob` | Supported | check-and-block without lost wake-ups; re-armed to the next delayed job |
| 89 | `disconnectBlocking` | Supported | |
| 90 | `reconnectBlocking` | Supported | |

Store lifetime (PROJECT.md §16 Q4, decided): each backend instance holds one reference on its namespace; the namespace (all its queues) is dropped when the last owning instance closes, unless an instance opened it with `connection.retain: true`; `resetMemoryStore(namespace?)` clears data in place and drops the pin. Rationale: tests that close what they open get isolation for free; code that relies on data surviving with no open instance must opt in explicitly. Operations on a closed instance reject with `Connection is closed.` like ioredis.

---

Status legend for the Phase 0 "Prio" column below:
`v0.1` = v0.1-required (Queue.add → Worker → completed/failed, concurrency, duplicate jobId, close). `v0.2` = v0.2. `later`. `oos` = out-of-scope.

---

## 1. Where the contract lives

| Item | Location | Notes |
|---|---|---|
| `IQueueBackend` | `src/interfaces/queue-backend.ts:61-782` (interface `export interface IQueueBackend` at :61) | 90 members (count method below). Exported from the package root via `src/index.ts:3` (`export * from './interfaces'`). |
| `BackendFactory<B, ConnectionOptionsType>` | `qb:794-806` | `(name, opts: QueueBaseOptions<ConnectionOptionsType>, options?: { blocking?: boolean; withBlockingConnection?: boolean }) => B`. Called once per Queue/Worker/QueueEvents/QueueEventsProducer/FlowProducer instance (`queue-base.ts:~134`). `blocking` = main connection is itself blocking (QueueEvents); `withBlockingConnection` = Worker (dedicated blocking connection, `worker.ts:433`). |
| `withBackend(factory)` | `src/utils/with-backend.ts:77` (returns `BackendClasses<B,C>`, interface at :27) | Returns bound `Queue`, `Worker`, `QueueEvents`, `QueueEventsProducer`, `FlowProducer` constructors that pass `factory` as the last constructor argument; process-wide default untouched. Exported from root (`src/index.ts:10`). |
| Default factory registry | `src/utils/create-backend.ts:114` (`defaultBackendFactory`), `setDefaultBackendFactory` :127, `getDefaultBackendFactory` :139 | Alternative to `withBackend`; this is how the repo's own test suite swaps in Postgres (`vitest.postgres.setup.ts`). |
| Connection options typing | `QueueBaseOptions<ConnectionOptionsType = ConnectionOptions>` at `src/interfaces/queue-options.ts:17`, field `connection: ConnectionOptionsType` :21. `ConnectionOptions` (Redis union) at `src/interfaces/redis-options.ts:48`. | A custom backend picks its own `C` (e.g. Postgres: `PostgresConnectionOptions`). With a factory, `connection` is **required** (`with-backend.ts` doc comment). For BullLite `C` can be e.g. `{ namespace?: string }`; the factory must ignore/replace Redis semantics. |
| Reference factory | `src/postgres/create-postgres-backend.ts:25-55` (`BackendFactory<PostgresQueueBackend, PostgresConnectionOptions>`) | Maps `blocking`/`withBlockingConnection` to a named LISTEN client. |
| Constructor overload | `Queue` etc. accept `(name, opts, backendFactory?)`; `queue.ts:147,166,185`; docs `docs/gitbook/guide/connections.md:240-285`. | |

Packaging: npm tarball has `"files": ["dist"]` (package.json), **no `exports` map**, `main: ./dist/cjs/index.js`, `module/types: ./dist/esm/...`. `IQueueBackend`, `BackendFactory`, `withBackend`, `setDefaultBackendFactory`, `createPostgresBackend` are all importable from `'bullmq'` (`dist/esm/index.d.ts` lines 7-9). `peerDependencies` (all optional): `bullmq-otel`, `ioredis`, `pg`, `redis`. Runtime deps: `cron-parser`, `msgpackr`, `node-abort-controller`, `semver`, `tslib`. `engines.node >=14.17.0` (declared; the repo's `.nvmrc` not checked).

Member counting method: a script lists every two-space-indented member declaration between `qb:61` and the closing `}` at `qb:782`. Result **90** (`on`/`once`/`removeListener` counted once each; `closing`, `minimumBlockTimeout`, `maximumBlockTimeout`, `qualifiedName`, `keys` are `readonly` properties, the rest methods). Chief's rough figure was ~93; the exact scripted count is 90.

---

## 2. `IQueueBackend` member table (90 members)

Who calls it: `Q`=Queue (`queue.ts`), `QG`=QueueGetters (base of Queue, `queue-getters.ts`), `W`=Worker (`worker.ts`), `J`=Job (`job.ts`), `QE`=QueueEvents, `QEP`=QueueEventsProducer, `FP`=FlowProducer, `JS`=JobScheduler (`job-scheduler.ts`), `QB`=QueueBase (`queue-base.ts`), `LM`=LockManager via `Worker.extendJobLocks`.

### 2.1 Connection lifecycle & identity (qb:74-199)

| # | Member (decl line) | Short signature | Called by | Atomicity | Prio |
|---|---|---|---|---|---|
| 1 | `waitUntilReady` (qb:74) | `() => Promise<void>` | QB:118, W:418, FP:209/244/305, relay | none; must resolve after both main and (for Worker) blocking resources ready | v0.1 |
| 2 | `close` (qb:83) | `(force?: boolean) => Promise<void>` | QB:182, W:1406, QE:433, QEP:72, FP:693 | `closing` set synchronously; in-flight ops settle (non-force); a pending `waitForJob` MUST be released; release namespace ref | v0.1 |
| 3 | `closing` (qb:91) | `readonly Promise<void> \| undefined` | W:1178/1210 | Must be set as soon as close begins; worker skips completing the job if set | v0.1 |
| 4 | `disconnect` (qb:96) | `() => Promise<void>` | QB:193, FP:703, QE:432 | force teardown | v0.1 |
| 5 | `setName` (qb:102) | `(name) => Promise<void>` | QE:359 | none; no-op allowed ("no concept") | v0.2 |
| 6 | `minimumBlockTimeout` (qb:108) | `readonly number` (seconds) | W:839 (`getBlockTimeout` `worker.ts:913-935`) | — | v0.1 |
| 7 | `maximumBlockTimeout` (qb:121) | `readonly number?` optional | W:843 (falls back to 10 s default) | — | v0.2 (optional; Postgres returns 3600) |
| 8 | `on` (qb:127) | `('ready'\|'error'\|'close', fn) => this` | QB:106/107, FP:154/159 | none (EventEmitter) | v0.1 |
| 9 | `once` (qb:131) | same | `.once` only on the high-level classes' own emitters, not on backend by scan | none | v0.1 (EventEmitter) |
| 10 | `removeListener` (qb:135) | `(event, fn) => this` | not called directly on backend by scan; FP:656 binds its own | none | v0.1 (EventEmitter) |
| 11 | `forQueue` (qb:153) | `(queueName, prefix?) => IQueueBackend` sibling sharing connection | FP:644 | non-owning sibling; closing it must not close the shared store | later (FlowProducer) |
| 12 | `qualifiedName` (qb:171) | `readonly string` | QB:102, FP:650 | — | v0.1 |
| 13 | `keys` (qb:178) | `readonly KeysMap` (may be `{}`) | QB:103, FP:647 | — | v0.1 |
| 14 | `toKey` (qb:184) | `(type: string) => string` | QB:104, FP:648; J uses `${parentKey}:dependencies` string concat (`job.ts:~278`) | — | v0.1 |
| 15 | `parseNodeKey` (qb:192) | `(key) => {prefix,queueName,id}` | FP:614 | — | later (flows) |
| 16 | `clientName` (qb:199) | `(suffix?) => string` | QB:173, QE:359 | — | v0.2 (return any stable string) |

### 2.2 Adding jobs (qb:211-266)

| # | Member | Signature | Called by | Atomicity | Prio |
|---|---|---|---|---|---|
| 17 | `addJob` (qb:211) | `(job: JobJson, jobId: string, parentKeyOpts?) => Promise<string>` | J:275 (`Job.create` ← `Queue.add`) | **One transition**: id assignment (counter if `jobId` empty) + duplicate-id check (existing id ⇒ **return existing id, no insert**, emit `duplicated`) + route to wait / delayed / prioritized / waiting-children + dedup-key handling + parent-existence check + marker/wake-up of blocked workers + `added`/`waiting` events. Pg: `add_job` plpgsql (`0002_functions.sql:47-185`, `ON CONFLICT (queue,id) DO NOTHING` then `handle_duplicated_job`). Parent missing ⇒ error code −5 (`finishedErrors`). | v0.1 |
| 18 | `addJobs` (qb:227) | `(entries[]) => Promise<string[]>` ids in order | J:322 (`Queue.addBulk`) | all-or-nothing batch | v0.2 |
| 19 | `addFlow` (qb:244) | `(entries[]) => [Error\|null, id\|code][]` | FP:28/276/362 | whole tree in one transaction (Redis `MULTI`); cross-queue | later |
| 20 | `addJobScheduler` (qb:266) | `(id, nextMillis, tplData, tplOpts, opts, delayedOpts, producerId?) => [jobId, delay]` | JS:244 | upsert scheduler record + create its next delayed job atomically | later |

### 2.3 Job state transitions (qb:284-369)

| # | Member | Signature | Called by | Atomicity | Prio |
|---|---|---|---|---|---|
| 21 | `moveToActive` (qb:284) | `(token, name?) => Promise<[jobData\|null, id, rateLimitDelay, delayUntil]>` | W:855 (`worker.ts:850-860`) | **Claim one job**: promote due delayed jobs → if paused return empty → enforce global `concurrency` meta + limiter → pick next waiting by (priority, FIFO/LIFO) → set active, lock token, `lockUntil = now+lockDuration`, `processedOn`, `attemptsStarted++` → `active` event. Two concurrent workers MUST NOT get the same job. Empty result must carry rate-limit ttl / next-delayed timestamp (`buildNextJobResult` `pg:781-806`). Pg: `move_to_active` using `FOR UPDATE SKIP LOCKED` (`0002_functions.sql:189-326`). | v0.1 |
| 22 | `moveToCompleted` (qb:292) | `(job, returnValue, removeOnComplete, token, fetchNext) => {result: void\|tuple, finishedOn}` | J:675 → W:`handleCompleted` 1170-1200 | **One transition**: verify job exists / active / lock token matches (errors −1 missing, −3 not active, −6 lock mismatch, −4 pending children, −9 failed children; `pg: 0002_functions.sql:328-440`) → state completed, `returnvalue`, `finishedOn`, clear lock → apply removeOnComplete (bool/count/age) → release parent dependency → `completed` event → if `fetchNext`: claim next job in the same step and return the same tuple as `moveToActive` (else `result: undefined`). | v0.1 |
| 23 | `moveToFailed` (qb:306) | `(job, failedReason, removeOnFail, token, fetchNext, fieldsToUpdate?) => {result, finishedOn}` | J:801 | same shape as completed: lock/active check → failed, `failedReason`, `stacktrace`, retention, parent handling, `failed` event, optional fetchNext | v0.1 |
| 24 | `moveToDelayed` (qb:318) | `(jobId, timestamp, delay, token?, opts?{skipAttempt,fieldsToUpdate,fetchNext}) => void\|tuple` | J:778/1193 | active→delayed with lock check, optional fused next-fetch; wake workers for new delay horizon | v0.2 (retry-with-backoff, `Job.moveToDelayed`) |
| 25 | `moveToWaitingChildren` (qb:330) | `(jobId, token, opts?) => boolean` | J:1212 | parent→waiting-children iff pending deps | later |
| 26 | `moveJobFromActiveToWait` (qb:339) | `(jobId, token?) => number (pttl)` | J:699 | active→wait for manual rate limit | later |
| 27 | `retryJob` (qb:344) | `(jobId, lifo, token?, opts?) => void` | J:789 (retry when `attempts>1`) | lock check; active→wait (front/back); wake workers | v0.2 |
| 28 | `retryFinishedJob` (qb:354) | `(job, 'failed'\|'completed', opts?)` | J:1249 (`job.retry()`) | finished→wait, reset counters | v0.2 |
| 29 | `promote` (qb:363) | `(jobId)` | J:1231 | delayed→wait | v0.2 |
| 30 | `moveStalledJobsToWait` (qb:369) | `() => string[]` | W:1634 (timer: `startStalledCheckTimer` `worker.ts:1441`, first call at `run()` start `worker.ts:638`, then every `stalledInterval` = 30 s default `worker.ts:308`) | Two-phase mark/sweep: active jobs whose lock expired and that were marked on a *previous* pass go back to wait (or fail beyond `maxStalledCount`), `stalled` event; whole scan throttled to once per `stalledInterval` (pg `0002_functions.sql:2274-2358`) | v0.1 (call happens on every Worker; minimal implementation may return `[]`, full semantics v0.2) |

### 2.4 Bulk admin transitions (qb:379-422)

| # | Member | Signature | Called by | Atomicity | Prio |
|---|---|---|---|---|---|
| 31 | `retryFinishedJobs` (qb:379) | `(state?, count?, ts?) => cursor` | Q:899 | batch per call | later |
| 32 | `promoteJobs` (qb:389) | `(count?) => cursor` | Q:929 | batch per call | later |
| 33 | `pause` (qb:394) | `(pause: boolean)` | Q:515/559 | set/clear pause flag; `moveToActive` must observe it; `paused`/`resumed` events | v0.2 |
| 34 | `drain` (qb:399) | `(delayed: boolean)` | Q:774 | remove waiting (+delayed) in one step | v0.2 |
| 35 | `cleanJobsByState` (qb:405) | `(state, ts, limit?) => ids` | Q:817 | batch delete by age | v0.2 |
| 36 | `obliterate` (qb:415) | `({force,count}) => cursor` | Q:865 | destroy queue; refuses when active jobs unless `force` | v0.2 (also the test-teardown primitive: `tests/utils/cleanup-queue.ts`) |
| 37 | `removeOrphanedJobs` (qb:422) | `(count?, limit?) => number` | Q:986 | Redis keyspace repair; Pg returns 0 (`pg:1169-1173`) | oos (return 0) |

### 2.5 Locks (qb:431-440)

| # | Member | Signature | Called by | Atomicity | Prio |
|---|---|---|---|---|---|
| 38 | `extendLock` (qb:431) | `(jobId, token, duration) => number` | J:640 (`job.extendLock`) | compare token + set new expiry in one step; returns 1/0 | v0.2 |
| 39 | `extendLocks` (qb:437) | `(jobIds[], tokens[], duration) => string[] failed ids` | W:474 ← `LockManager` timer (`lock-manager.ts:41-125`, every `lockRenewTime` = `lockDuration/2` = 15 s default, `worker.ts:344`) | batch; per-id token check | v0.1 (a job >15 s needs it; must not throw) |

### 2.6 Job mutations (qb:450-515)

| # | Member | Signature | Called by | Atomicity | Prio |
|---|---|---|---|---|---|
| 40 | `updateData` (qb:450) | `(job, data)` | J:545 | single write | v0.2 |
| 41 | `updateProgress` (qb:458) | `(jobId, progress)` | J:555, Q:733 | write + `progress` event | v0.2 |
| 42 | `addLog` (qb:464) | `(jobId, row, keepLogs?) => count` | J:489 (via `scripts.addLog`) | append + trim | v0.2 |
| 43 | `clearLogs` (qb:469) | `(jobId, keepLogs?)` | J:594 | single write | later |
| 44 | `changeDelay` (qb:474) | `(jobId, delay)` | J:992 | delayed job only | later |
| 45 | `changePriority` (qb:479) | `(jobId, priority?, lifo?)` | J:1006 | re-queue with new order | later |
| 46 | `remove` (qb:489) | `(jobId, removeChildren) => 1\|0` | J:610, Q:705 | refuse when locked (0); emit `removed`; flow cascade | v0.2 |
| 47 | `removeUnprocessedChildren` (qb:494) | `(jobId)` | J:630 | flows | later |
| 48 | `removeChildDependency` (qb:500) | `(jobId, parentKey) => bool` | J:575 | flows | later |
| 49 | `removeDeduplicationKey` (qb:506) | `(dedupId, jobId) => 1\|0` | J:1324 | compare-and-delete | later |
| 50 | `deleteDeduplicationKey` (qb:515) | `(dedupId) => number` | Q:651/671 | delete | later |

### 2.7 Job schedulers (qb:521-559)

| # | Member | Called by | Prio |
|---|---|---|---|
| 51 | `updateJobSchedulerNextMillis` (qb:521) `(id,nextMillis,tplData,delayedOpts,producerId?) => string\|null` | JS:286 (called from the worker when a scheduler job is picked up, `worker.ts:~1000`) | later |
| 52 | `removeJobScheduler` (qb:529) | JS:361/372 | later |
| 53 | `getJobScheduler` (qb:531) `=> [raw, next\|null]` | JS:463 | later |
| 54 | `isJobScheduler` (qb:536) | JS:457; W (via JS, `worker.ts:~985` for legacy-shaped repeat keys) | later |
| 55 | `getJobSchedulerData` (qb:541) | JS:368 | later |
| 56 | `getJobSchedulersRange` (qb:547) | JS:491 | later |
| 57 | `getJobSchedulersCount` (qb:556) | JS:503 | later |

Atomicity for 51: read+update scheduler next-time and insert the next delayed job as one step (Redis Lua; pg `add_job_scheduler`/`update_job_scheduler_next_millis` SQL functions).

### 2.8 Queries (qb:565-667)

| # | Member | Called by | Prio |
|---|---|---|---|
| 58 | `getState` (qb:565) `=> JobState\|'unknown'` | J:974 (`job.getState`), QG:232 (`getJobState`) | v0.1 |
| 59 | `isFinished` (qb:570) `=> number \| [number, string]` | J:1167 (`job.waitUntilFinished`; needs QueueEvents too) | v0.2 |
| 60 | `isMaxed` (qb:578) | Q:576 (`isMaxed()`) | v0.2 |
| 61 | `isJobInState` (qb:583) | J:1265 (`job.isCompleted/isFailed/isActive/isWaiting/isDelayed`) | v0.2 |
| 62 | `getJobData` (qb:588) `=> JobJson\|undefined` | J:464 (`Job.fromId` ← `queue.getJob`) | v0.1 |
| 63 | `getDeduplicationJobId` (qb:593) | QG:121/130 | later |
| 64 | `getJobLogs` (qb:598) | QG:544 | v0.2 |
| 65 | `getRateLimitTtl` (qb:608) | QG:111 | later |
| 66 | `getCounts` (qb:610) | QG:188 (`getJobCounts`) | v0.2 |
| 67 | `getCountsPerPriority` (qb:612) | QG:316 | later |
| 68 | `getRanges` (qb:614) | QG:469 (`getJobs`, `getWaiting` …) | v0.2 |
| 69 | `getDependencyCounts` (qb:621) | J:1097 | later |
| 70 | `getDependencies` (qb:626) | J:1066 | later |
| 71 | `getProcessedChildrenValues` (qb:643) | J:1016 | later |
| 72 | `getIgnoredChildrenFailures` (qb:648) | J:1031/1042 | later |
| 73 | `getMetrics` (qb:650) | QG:644 | later |
| 74 | `getClientList` (qb:662) | QG:553 (`getWorkers`/`getQueueEvents`); "may return empty array" | later (return `[]`) |
| 75 | `paginate` (qb:667) | QG:438 (`getDependencies*` pages); Pg throws `notImplemented` for non-dependency keys (`pg:1914`) | later |

### 2.9 Queue metadata & maintenance (qb:684-732)

| # | Member | Called by | Prio |
|---|---|---|---|
| 76 | `setQueueMeta` (qb:684) | **Queue constructor** (`queue.ts:193-195`, `.setQueueMeta(this.metaValues)` unless `skipMetasUpdate`; rejection is swallowed by BullMQ, but Prototip's call log shows it is issued on every Queue), Q:291 (`setGlobalConcurrency`), Q:300 (`setGlobalRateLimit`) | v0.1 (store the meta hash; the constructor write is on the hot path of every Queue) |
| 77 | `getQueueMetaField` (qb:689) | QG:138, Q:263 (`getVersion`) | v0.2 |
| 78 | `getQueueMetaFields` (qb:694) | QG:153 | v0.2 |
| 79 | `getQueueMeta` (qb:699) | QG:241 (`getMeta`) | v0.2 |
| 80 | `removeQueueMetaFields` (qb:704) | Q:307/314 | v0.2 |
| 81 | `hasQueueMetaField` (qb:709) | Q:569 (`isPaused`) | v0.2 |
| 82 | `setRateLimit` (qb:714) | Q:546, W:833 (`worker.rateLimit`) | later |
| 83 | `removeRateLimitKey` (qb:720) | Q:680 | later |
| 84 | `removeDeprecatedPriorityKey` (qb:726) | Q:959 ; Pg throws `notImplemented` (`pg:2027-2029`) | oos |
| 85 | `trimEvents` (qb:732) | Q:950 ; Pg throws `notImplemented` (`pg:2031-2033`; trimming happens inside `publish_event`) | later |

### 2.10 Event stream (qb:742-752)

| # | Member | Signature | Called by | Atomicity | Prio |
|---|---|---|---|---|---|
| 86 | `publishEvent` (qb:742) | `(fields, maxEvents) => id` | QEP:64 (custom events) | append + wake readers | later |
| 87 | `readEvents` (qb:752) | `(lastId, blockTimeoutMs) => StreamReadRaw` (Redis `XREAD` shape `[[streamKey, [[id, [k,v,k,v…]]]]]`, falsy on timeout) | QE:380 (`consumeEvents` loop `queue-events.ts:368-415`) | Ordered, monotonic ids; `'$'` = "only events after now"; cursor-resume via `lastEventId`; an event appended by any transition must be visible to readers in transition order | v0.2 |

### 2.11 Worker blocking primitive (qb:768-781)

| # | Member | Signature | Called by | Atomicity | Prio |
|---|---|---|---|---|---|
| 88 | `waitForJob` (qb:768) | `(blockTimeoutSeconds) => {member, score}\|null` | W:876 (`worker.ts:862-905`) | **Check-and-block**: must not lose a wake-up that occurs between the last `moveToActive` returning empty and the wait starting (Redis: `BZPOPMIN` on marker zset; Pg: LISTEN + `has_waiting_job` probe + `next_delay` timer, `pg:2176-2271`). `score` = timestamp of next delayed job or 0 ("check now"). `null` = timeout. | v0.1 |
| 89 | `disconnectBlocking` (qb:776) | `(wait?) => Promise<void>` | W:1569 (`whenCurrentJobsFinished`, on `close()` while main loop runs) | interrupt the in-flight `waitForJob` immediately (also one *started after* the call until `reconnectBlocking`, cf. `blockingDisconnected` flag `pg:290`) | v0.1 |
| 90 | `reconnectBlocking` (qb:781) | `() => Promise<void>` | W:899 (error recovery), W:1575 | re-arm after interrupt | v0.1 |

### 2.12 Count

* **v0.1-required: 23** — `waitUntilReady`, `close`, `closing`, `disconnect`, `minimumBlockTimeout`, `on`, `once`, `removeListener`, `qualifiedName`, `keys`, `toKey`, `setQueueMeta`, `addJob`, `moveToActive`, `moveToCompleted`, `moveToFailed`, `moveStalledJobsToWait`, `extendLocks`, `getState`, `getJobData`, `waitForJob`, `disconnectBlocking`, `reconnectBlocking`.
  * Cross-check with Prototip's measured call log (bullmq 6.4.0, Queue.add → Worker → completed/failed → getJob/getState → close): actually invoked = `qualifiedName`, `keys`, `closing`, `minimumBlockTimeout`, `on`, `waitUntilReady`, `setQueueMeta`, `addJob`, `moveStalledJobsToWait`, `moveToActive`, `waitForJob`, `moveToCompleted`, `moveToFailed`, `getJobData`, `getState`, `disconnectBlocking`, `close` (+ `extendLocks` for jobs longer than `lockDuration/2`). The remaining v0.1 entries (`disconnect`, `once`, `removeListener`, `toKey`, `reconnectBlocking`) are not hit on the short path but belong to the same classes' code paths (`QueueBase.disconnect`, `Worker.waitForJob` error recovery, EventEmitter contract, key helper used by flows/getters) and are cheap; they may be implemented trivially.
  * Of the 23, the genuinely behavioural/atomic ones are: `addJob`, `moveToActive`, `moveToCompleted`, `moveToFailed`, `waitForJob` + `disconnectBlocking` + `reconnectBlocking` (one blocking-wait unit), `extendLocks`, `moveStalledJobsToWait`, `getState`/`getJobData`, `setQueueMeta`. The rest are plumbing (EventEmitter, identity, close).
* **v0.2: 29** — `setName`, `maximumBlockTimeout`, `clientName`, `addJobs`, `moveToDelayed`, `retryJob`, `retryFinishedJob`, `promote`, `pause`, `drain`, `cleanJobsByState`, `obliterate`, `extendLock`, `updateData`, `updateProgress`, `addLog`, `remove`, `isFinished`, `isMaxed`, `isJobInState`, `getJobLogs`, `getCounts`, `getRanges`, `getQueueMetaField`, `getQueueMetaFields`, `getQueueMeta`, `removeQueueMetaFields`, `hasQueueMetaField`, `readEvents`.
* **later: 36**, **out-of-scope: 2** (`removeOrphanedJobs`, `removeDeprecatedPriorityKey`): remainder of the table (see priority column).

Tally (script-parsed from the priority column of §2.1-2.11 before this edit gave 22/30/36/2; `setQueueMeta` then moved v0.2→v0.1): **23 + 29 + 36 + 2 = 90**.

---

## 3. Second contract: `IRelayBackend`

* Interface: `src/interfaces/relay-backend.ts` (291 lines; `IRelayBackend` at `rel:148`, factory type `RelayBackendFactory`, options `rel:274`). Implementations: Redis `src/classes/redis-relay-backend.ts`, Postgres `src/postgres/postgres-relay-backend.ts` (462 lines; SQL in `migrations/0004_relay.sql`). Docs: `docs/gitbook/guide/relay/backends.md`.
* What it is: backend for the **`Relay` class** (`src/classes/relay.ts`) — a pub/sub / topic-pattern messaging layer with nodes, heartbeats/leases, subscriptions, retained messages and an inbox (`registerNode`, `heartbeat`, `sweep`, `subscribe`, `publish`, `readInbox`, `trimInbox`, `getCounts`, … `rel:154-271`). `Relay` picks the factory from `opts.backend`, default Redis (`relay.ts:65,205-207`).
* Who needs it: **only `Relay`**. `Queue`, `Worker`, `Job`, `QueueEvents`, `FlowProducer`, `JobScheduler` do not reference it (grep over `classes/queue*.ts`, `worker.ts`, `job*.ts` found no `relay`). `QueueEvents` uses `IQueueBackend.readEvents`, not the relay.
* v0.1 verdict: **not required**. Mark `out-of-scope` for BullLite v0.1; do not implement `IRelayBackend`. (Postgres CI runs relay tests separately: `vitest.postgres.config.ts` includes `tests/relay.test.ts`, `tests/relay_stress.test.ts`.)

---

## 4. How the built-in Postgres backend implements the contract (reference)

Sizes: `postgres-queue-backend.ts` 2284 lines; `postgres-connection.ts` 502; SQL: `commands/*.sql` (one tiny file per op, e.g. `move_to_active.sql` is a single `SELECT * FROM move_to_active($1..$7)`) + `migrations/0001_schema.sql` (348 lines, tables), `0002_functions.sql` (3473 lines, all logic as plpgsql functions), `0003_dedup_stale_key.sql`, `0004_relay.sql`. Redis reference: `redis-queue-backend.ts` 3099 lines + Lua.

**Structure.** `PostgresQueueBackend` extends `EventEmitter`, constructor `(connection: PostgresConnection, queueName, opts, ownsConnection=true, listenClientName?)` (`pg:295`). Every op is `this.run('<command>', params)` → loads `commands/<command>.sql` and executes via `pool.query` (`pg:491-540`). The TS layer is thin; **all multi-step logic lives in SQL functions so each op is one round-trip/one transaction**. The namespace is a Postgres *schema* (`qualifiedName` = bare queue name, `pg:438`; BullMQ's `prefix` is ignored, `pg:422-436`). Tables: `job(queue,id,seq,state,data,opts,priority,delay_ms,attempts_*,lock_token,locked_until_ms,stalled_count,stalled_marked,dedup_id,scheduler_id,parent_*,…)` PK `(queue,id)`, plus `job_log`, `job_dependency`, `event`, `metrics`, `meta`, `rate_limit`, `dedup`, `scheduler` (`0001_schema.sql:29-348`). "paused" is a meta flag, "prioritized" = waiting with `priority>0`, not physical states (`0001_schema.sql:~70-80`). Job order = `ORDER BY priority, seq` where `seq` is a global sequence, **negated for LIFO** (`:168`).

**Claim / locking strategy.**
* Claim: `SELECT id FROM job WHERE queue=$1 AND state='waiting' ORDER BY priority, seq FOR UPDATE SKIP LOCKED LIMIT 1` then `UPDATE … state='active', lock_token, locked_until_ms=now+lockDuration, attempts_started+1` (`0002_functions.sql:278-325`).
* Global concurrency and rate limit are serialized per queue with transaction-scoped advisory locks (`pg_advisory_xact_lock(hashtext('bullmq:concurrency:'||queue))`, `…:limiter:…`, `:244,270`) because READ COMMITTED snapshots would otherwise overshoot; id counter, dedup keys and parent rows use further advisory locks (`:39,83,360,438…`).
* Finish ops lock the job row `FOR UPDATE`, check state and `lock_token` and raise SQLSTATE `BM001` with `DETAIL` = numeric BullMQ error code (−1 missing, −3 not active, −6 lock mismatch, −4/−9 child deps…); TS maps this via `finishedErrors` (`pg:549-563`). Token `'0'` bypasses the lock check (`0002_functions.sql:~372`).
* Completion + next-job claim is **fused** into one transaction (`move_to_completed_fetch`, `move_to_failed_fetch`) like Redis `moveToFinished` (`pg:818-850`).
* Locks: `extend_lock(s)` compare token and bump `locked_until_ms` (`commands/extend_lock*.sql`). Stalled: two-phase mark/sweep with a `stalled-check` throttle in `meta` (`0002_functions.sql:2274-2358`).
* Duplicate id: `INSERT … ON CONFLICT (queue,id) DO NOTHING`; when not inserted, `handle_duplicated_job` announces `duplicated` and the **existing id is returned** (`0002_functions.sql:139-168`).

**Worker wake-up / blocking wait** (`pg:2106-2284`). Producers (and any op that makes a job claimable: add, retry, promote, stalled-reclaim, resume…) call `pg_notify('bullmq_jobs', <queue>)` at commit (~18 call sites, e.g. `0002_functions.sql:168,649,702,916`). A worker backend keeps **one dedicated LISTEN client** (`connection.getListenClient()`, `LISTEN bullmq_jobs`, `commands/listen_jobs.sql`) and `waitForJob` resolves a promise on a notification whose payload equals its queue name; else on timeout. Race closers: after registering the listener it runs `has_waiting_job` (don't lose a NOTIFY that fired before LISTEN) and `next_delay` (shortens the timer to the next delayed job's due time since delayed promotion emits no NOTIFY). `disconnectBlocking` sets `blockingDisconnected=true` and fires `cancelWait`; `reconnectBlocking` clears it and forces a fresh LISTEN. `minimumBlockTimeout = 0.001`, `maximumBlockTimeout = 3600` if TCP keepalive is on else 10 (`pg:400-420`). A dropped LISTEN client emits `listenerinvalidated` → re-LISTEN and cancel waits (`pg:313-325`).

**Events / QueueEvents.** `event(queue, id bigint DEFAULT nextval('event_seq'), event, data jsonb, created_at_ms)` table (`0001_schema.sql:253-260`). Every lifecycle SQL function calls `publish_event(queue, name, jsonb)` which inserts, then `pg_notify('bullmq_events', queue)` (coalesced with `pg_try_advisory_xact_lock` because NOTIFY serializes all commits globally, `0002_functions.sql:~808-832`), then trims to `opts.maxLenEvents` (default 10000) in chunks every 256th event. Event names emitted: `added, waiting, active, delayed, completed, failed, progress, stalled, drained, removed, deduplicated, duplicated, cleaned, retries-exhausted` (grep of `publish_event(` in migrations). `readEvents(id, blockTimeout)` (`pg:2052-2083`): resolve `'$'` to `MAX(id)` (`read_events_max`), `SELECT … WHERE queue=$1 AND id > cursor ORDER BY id LIMIT n`; if empty, `waitForEvent` (LISTEN `bullmq_events`) then re-read; **re-shapes rows into the Redis XREAD structure** `[['events', [[id, ['event', name, k, v, …]]]]]` because `QueueEvents.consumeEvents` parses exactly that (`queue-events.ts:380-415`; it `JSON.parse`s `data` for `progress`, `returnvalue` for `completed`, `Number()`s `delay`). `publishEvent` = `publish_event`.

**Not implemented in Pg (throws):** `trimEvents`, `removeDeprecatedPriorityKey`, `paginate` for non-dependency keys (`notImplemented`, `pg:160,1914,2028,2032`). `removeOrphanedJobs` returns 0.

**Connection:** `PostgresConnection` = `pg.Pool` + separate LISTEN client with keepalive, memoized readiness, schema migration on first connect (`migrator.ts`), `forQueue` returns a non-owning sibling (`pg:422-436`).

---

## 5. The "adapter-conformance" suite

Findings (chief's verification confirmed against source):

* `vitest.adapter-conformance.config.ts` (repo root) includes **only** `tests/adapter-conformance.test.ts`; script `test:adapter-conformance` (package.json). Header of the test: *"Redis Adapter Conformance Test Suite — Validates that an **IRedisClient** implementation … Run this against any new adapter before running the full BullMQ test suite"* (`tests/adapter-conformance.test.ts:1-30`, 827 lines). It builds a node-redis client via `createTestClient()` and exercises raw Redis commands (SET/XREAD/BZPOPMIN/Lua `eval`, streams, pipelines …). It is a **driver-level (IRedisClient) suite, not an `IQueueBackend` suite**; it needs a live Redis (`REDIS_HOST`/`REDIS_PORT`). `tests/bun-adapter-suite.test.ts` (306 lines) is the same idea for Bun's `RedisClient` using `bun:test` and the real `Queue`/`Worker`/`FlowProducer` on Redis. **PROJECT.md's assumption that a backend conformance suite exists is wrong**; `docs/gitbook/guide/connections.md:~283` ("Use the adapter-conformance and full BullMQ test suites") is misleading in the same way.
* **Not shipped.** `package.json` `"files": ["dist"]`; the extracted tarball contains `LICENSE README.md dist package.json` and no `*.test.*`/`tests/`/vitest configs (verified by `find`). Third parties must clone the repo at the matching tag.
* Tests that already target `IQueueBackend` implementations:
  * `tests/postgres-queue-backend.test.ts` (32 lines): unit test with a mocked `run`; Postgres-specific, not reusable.
  * `tests/postgres/with-backend.test.ts` (103 lines): exercises `withBackend(createPostgresBackend)` for all five constructors without changing the global default; a template for our own `withBackend` smoke test but hardwired to `pg`/`getPostgresUrl()`.
  * `tests/postgres/worker.test.ts` (264 lines): real Queue+Worker+QueueEvents e2e via `setDefaultBackendFactory`.
  * `tests/create-backend.test.ts` (95 lines): Redis connection-ownership test; not reusable.
  * `tests/postgres/{migration,operations,sql_loader,relay}.test.ts`: Postgres internals.
* **The real conformance bar is the shared suite** (the ~50 backend-agnostic `tests/*.test.ts` files: `queue`, `worker`, `job`, `events`, `delay`, `flow`, `stalled_jobs`, `getters`, `pause`, `obliterate`, …). The default `vitest.config.ts` (`include tests/**/*.test.ts`, `setupFiles: ./vitest.setup.ts`) excludes `tests/postgres/**`, adapter files and ioredis-specific files. `vitest.postgres.setup.ts` shows exactly how to parameterize it for another backend: (1) `setConnectionFactory(() => inertFakeClient)` because the suite builds a `connection` and calls `connection.quit()` (`tests/utils/connection-factory.ts`); (2) `setDefaultBackendFactory((name, opts, options) => createXBackend(name, {...opts, connection: …}, options))`; (3) `process.env.BULLMQ_TEST_BACKEND='postgres'` marker to skip a few tests (`tests/worker.test.ts:2390`, `tests/sandboxed_process.test.ts:1399`, `tests/utils/relay.ts:23`); cleanup is backend-agnostic via `obliterate({force:true})` (`tests/utils/cleanup-queue.ts`). **Observation:** at this tag `vitest.postgres.config.ts` does **not** reference `vitest.postgres.setup.ts` (no `setupFiles`) and only includes `tests/postgres/**` + relay tests; `vitest.postgres.setup.ts`/`global-setup.ts` are present but I did not find a config that wires them to the full shared suite → [UNKNOWN] whether Taskforce currently runs the entire shared suite on Postgres; do not rely on that claim until verified (e.g. by grepping the CI workflow for a postgres job — only `dotnet/elixir/python` workflows reference Postgres in the files I grepped).
* **How BullLite could use it:** clone `taskforcesh/bullmq@v6.4.0`, write `vitest.bulllite.config.ts` + a setup file modeled on `vitest.postgres.setup.ts` that registers a BullLite factory with `setDefaultBackendFactory` and an inert connection factory, include a curated subset (`queue`, `worker`, `concurrency`, `job` partial, `events`, `stalled_jobs`, `delay`), and run with the repo's tests importing `../src` (they import relative `../src/...`, so the backend and any BullLite alias must be wired through vitest `resolve.alias` or the test files copied). Tests that call raw Redis (`*.redis.test.ts`, `removeAllQueueData`, anything via `queue.getBackend().client`) cannot run. Plan: treat the shared suite as an *opt-in external conformance harness* (not vendored), and maintain our own smoke tests derived from `tests/postgres/worker.test.ts` and `with-backend.test.ts`.

---

## 6. Answers to PROJECT.md §16

1. **Operations needed for the first vertical flow:** 23 of 90 members (list in §2.12; 17 confirmed by Prototip's runtime call log, +`extendLocks` for long jobs). Behaviourally: `addJob`, `setQueueMeta` (Queue constructor write, `queue.ts:193-195`), `moveToActive`, `moveToCompleted`, `moveToFailed`, `waitForJob`/`disconnectBlocking`/`reconnectBlocking`, `extendLocks`, `moveStalledJobsToWait` (timers at Worker start), `getState`/`getJobData`, plus identity/lifecycle plumbing. `Queue.add` → `Job.create` → `addJob` (`job.ts:275`); the Queue constructor additionally issues `setQueueMeta` unless `skipMetasUpdate`. Worker start: `waitUntilReady` → `startStalledCheckTimer` (`moveStalledJobsToWait`) → `lockManager.start` (`extendLocks`) → `mainLoop` (`moveToActive`/`waitForJob`) (`worker.ts:625-645`). Duplicate `jobId` is handled **inside `addJob`** (returns existing id), no extra member.
2. (not assigned.)
3. **QueueEvents semantics:** `QueueEvents` is a pure consumer of `readEvents(lastId, blockMs)`; the backend must emulate a Redis stream: monotonically increasing, string-comparable ids (QueueEvents stores the last id from the response, `queue-events.ts:380-390`; initial id `'$'` or `opts.lastEventId`), return the XREAD-shaped array, return falsy on timeout, and *every state transition must append its event atomically with the transition* (Pg does it inside the same SQL function). A bare `EventEmitter` is not equivalent because `QueueEvents` may start after events were emitted (resume by id), and uses a blocking-read loop. Not needed for v0.1: Worker/Queue emit `completed`/`failed`/`waiting` *locally* (`worker.ts:1185` `this.emit('completed', …)`); only `Job.waitUntilFinished` (`job.ts:1100-1170`, needs `QueueEvents` + `isFinished`) and `QueueEvents` need the stream → v0.2 as in PROJECT.md §5.
4. **`close()` and releasing the namespace store:** the contract only says `close(force?)` closes the backend's own connection(s) and `closing` is truthy once it begins (`qb:83-91`); a `forQueue` sibling is non-owning (`qb:153`, `pg:422-436`). `Worker.close` sequence: wait for current jobs (unless force) → `lockManager.close()` → `childPool.clean()` → `backend.close(force)` (`worker.ts:1395-1410`); before that `whenCurrentJobsFinished` calls `disconnectBlocking(true)` then awaits the main loop (`worker.ts:1560-1576`). So a memory backend should reference-count its namespace store **per backend instance** (each Queue/Worker/QueueEvents creates its own via the factory) and free it when the last owning backend closes *and* only if the store is not explicitly pinned by the user; this is a design choice — the contract does not specify it. [INFERENCE for the policy; the sequence above is source-verified.]
5. **Delayed jobs / scheduler operations:** plain delayed jobs (`opts.delay`) use only `addJob` (routes to delayed), `moveToActive` (promotes due delayed jobs first and reports `delayUntil` in the tuple), `waitForJob` (must wake at/near the next delayed due time: `worker.ts:913-935` shortens the block via the returned `score`), plus `moveToDelayed` (retry backoff / `Job.moveToDelayed`), `promote`, `changeDelay`, `promoteJobs`. **Job schedulers** (repeatable) additionally use `addJobScheduler`, `updateJobSchedulerNextMillis`, `removeJobScheduler`, `getJobScheduler`, `isJobScheduler`, `getJobSchedulerData`, `getJobSchedulersRange`, `getJobSchedulersCount` (members 20, 51-57; callers `job-scheduler.ts:244,286,361-503`); the worker invokes `jobScheduler.upsertJobScheduler` after fetching a scheduler-produced job (`worker.ts:~985-1010`). No separate timer operation exists on the backend: delay promotion is done lazily inside `moveToActive` (Pg `0002_functions.sql:~206-226`) — **the in-memory backend needs no delay timers of its own, but `waitForJob` must be able to sleep until the earliest delayed due time**.

---

## 7. Risks for an in-memory (and SQLite) backend

1. **Multiple connections assumed (shape, not necessity).** `BackendFactory` is invoked per Queue/Worker/QueueEvents instance, each getting its own backend and `withBlockingConnection` for workers (`worker.ts:433`). Instances in one process MUST share state through a namespace store keyed by `connection` options (PROJECT.md §16 Q2); the factory is the only link between instances (no object identity is shared by BullMQ).
2. **Blocking primitive semantics.** `waitForJob` must be wake-able from any other backend instance in the same process (producer's `addJob`), cancellable by `disconnectBlocking`, loss-free across the check-then-wait gap, and honour timeouts in seconds including fractional (`minimumBlockTimeout` is seconds: Pg 0.001, Redis 1 or ceil'd, `redis-queue-backend.ts:~399`). Under fake timers a 0 ms timeout never fires (see the comments at `pg:2237-2250`); avoid arming zero-length timers.
3. **Pub/sub / event stream.** `readEvents` needs ordered ids and blocking reads; events must be published atomically with the transition (§6 Q3). Retention (`maxLenEvents`, default 10000) is a backend duty (`trimEvents` is separate; Pg trims in `publish_event`).
4. **Serialization.** `msgpackr` is imported only by `redis-queue-backend.ts:7` (to pack opts for Lua); it is **not** part of the interface. Data crosses the contract as strings/JSON (`JobJson.data`, `opts` are JSON strings; `progress`, `returnvalue`, `failedReason` handled by the caller as JSON). A memory backend must store/clone JSON text (or deep-clone) to avoid aliasing; it never has to implement msgpack.
5. **Lock renewal / stalled timers.** Real BullMQ timers run in the Worker: `LockManager` (`lockRenewTime = lockDuration/2`, 15 s default) → `extendLocks`; stalled checker every `stalledInterval` 30 s → `moveStalledJobsToWait`. The backend must store `lockToken` + expiry, compare tokens in every finish op (otherwise error codes −3/−6 semantics are lost), and treat token `'0'` as "no check" (`0002_functions.sql:~372`). With long CPU-bound processors the renewal timer can starve → stalled false positives; two-phase marking (see §4) protects against that.
6. **`Worker.run` loop assumptions.** `moveToActive` result is a positional tuple `[jobData|null, id, rateLimitDelay, delayUntil]` where `jobData` is a decoded `JobJson` (the Redis backend decodes hash fields via `raw2NextJobData`, `redis-queue-backend.ts:2929-2938`); `moveToCompleted/Failed` with `fetchNext` return the same tuple as `result` (array ⇒ next job, see `worker.ts:1170-1200`). Getting this shape wrong silently yields "drained" loops.
7. **Typing surface.** Any `IQueueBackend` implementation must compile against all 90 members (including the optional `maximumBlockTimeout`) — stubs that throw are acceptable only if *explicit* (honesty rule). `JobJson`, `MinimalJob`, `ParentKeyOpts`, `StreamReadRaw`, `KeysMap` are exported from root.
8. **Error mapping.** Finish ops must raise the shared canonical errors (`finishedErrors`, `src/classes/finished-errors.ts`) with BullMQ numeric codes (−1…−9); the Worker/Job inspect messages (e.g. lock-lost handling).
9. **Test harness gap.** No ready-made backend conformance suite (§5); the shared suite assumes Redis in many places (`*.redis.test.ts` excluded for pg; raw-client access via `getBackend().client` in some tests). Our compatibility claims must come from our own tests plus a curated external-run subset.
10. **Peer deps.** `pg`, `redis`, `ioredis` are optional peers, so BullLite does not need to install them; but `bullmq` still loads Redis classes at import time (imports are static in `src/index.ts`); importing `bullmq` does not connect to Redis until a Redis-backed class is constructed [INFERENCE — verify in Prototip's spike].
11. **Multi-process / SQLite.** The Pg approach (row locks, advisory locks, LISTEN/NOTIFY, `SKIP LOCKED`) has no direct SQLite equivalent: no LISTEN/NOTIFY (cross-process wake-up needs polling or a side channel), single-writer locking means `BEGIN IMMEDIATE` transactions replace `SKIP LOCKED`. Not relevant to memory v0.1.

## 8. Unknowns (explicit)

* Whether Taskforce CI runs the *whole* shared suite on Postgres (config at this tag does not show it; §5).
* Exact set of shared-suite tests that pass with a non-Redis backend (only the Pg authors know; no pass/fail list shipped).
* Behaviour of Node/`engines` support of BullMQ 6.4 beyond the declared `>=14.17.0`; Node 24.22 is used in this workspace [spike must confirm].
* `QueueEvents` event payload field set per event for each name (only the names are listed here; field shapes are in the SQL functions and Redis Lua, not catalogued).
