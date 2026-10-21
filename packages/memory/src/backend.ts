import { EventEmitter } from 'node:events';
import { ErrorCode, QueueKeys } from 'bullmq';
import type {
  BackendFactory,
  FinishedStatus,
  IQueueBackend,
  JobJson,
  JobProgress,
  JobState,
  JobType,
  JobsOptions,
  KeepJobs,
  KeysMap,
  MinimalJob,
  MoveToDelayedOpts,
  QueueBaseOptions,
  RetryJobOpts,
  RetryOptions,
  StreamReadRaw,
} from 'bullmq';
import { defer } from './defer.ts';
import { finishedError, UnsupportedError } from './errors.ts';
import {
  acquireNamespace,
  releaseNamespace,
  type ClientEntry,
  type MemoryJobState,
  type NamespaceStore,
  type QueueStore,
  type StoredJob,
} from './store.ts';

export interface MemoryConnectionOptions {
  /**
   * Isolation domain. Queue/Worker/QueueEvents instances that use the same
   * namespace (and the same queue name and prefix) share jobs. Default `'default'`.
   */
  namespace?: string;
  /**
   * Keep the namespace's data after the last instance on it closes. By default
   * a namespace is released (all its jobs dropped) when its last
   * Queue/Worker/QueueEvents closes. A retained namespace lives until
   * `resetMemoryStore(namespace)`.
   */
  retain?: boolean;
}

/** The subset of Worker/Queue options this backend reads. */
interface BackendOptions extends QueueBaseOptions<MemoryConnectionOptions> {
  name?: string;
  prefix?: string;
  lockDuration?: number;
  maxStalledCount?: number;
  stalledInterval?: number;
  removeOnComplete?: boolean | number | KeepJobs;
  removeOnFail?: boolean | number | KeepJobs;
  limiter?: unknown;
  metrics?: unknown;
}

type NextJob = [JobJson | undefined, string, number, number];
type Marker = { member: string; score: number };

const DEFAULT_NAMESPACE = 'default';
const DEFAULT_PREFIX = 'bull';
const DEFAULT_LOCK_DURATION = 30_000;
const DEFAULT_STALLED_INTERVAL = 30_000;
const DEFAULT_MAX_STALLED_COUNT = 1;
const STALLED_REASON = 'job stalled more than allowable limit';
const CHECK_NOW: Marker = { member: '0', score: 0 };

/** Redis `LRANGE`/`ZRANGE` index semantics (inclusive, negative = from the end). */
function redisRange<T>(list: readonly T[], start: number, end: number): T[] {
  const len = list.length;
  const s = start < 0 ? Math.max(len + start, 0) : start;
  const e = end < 0 ? len + end : Math.min(end, len - 1);
  if (s > e || s >= len) return [];
  return list.slice(s, e + 1);
}

/** `count: 0` = remove on finish, `count: -1`/undefined = keep all, `age` in seconds. */
type Retention = { count?: number; age?: number };

function normalizeKeep(
  jobKeep: boolean | number | KeepJobs | undefined,
  workerKeep: boolean | number | KeepJobs | undefined,
): Retention {
  // Same precedence as RedisQueueBackend.getKeepJobs: the job option wins, else the worker option.
  const keep = jobKeep === undefined ? workerKeep : jobKeep;
  if (keep === undefined || keep === false) return { count: -1 };
  if (keep === true) return { count: 0 };
  if (typeof keep === 'number') return { count: keep };
  return keep;
}

function cloneJson<T>(value: T): T {
  return value === undefined ? value : (JSON.parse(JSON.stringify(value)) as T);
}

function parseStreamId(id: string): [number, number] {
  const [ms, seq] = id.split('-');
  return [Number(ms) || 0, seq === undefined ? 0 : Number(seq) || 0];
}

class MemoryBackend extends EventEmitter {
  readonly qualifiedName: string;
  readonly keys: KeysMap;
  /** Seconds. The wait primitive is event driven, so any positive value is fine. */
  readonly minimumBlockTimeout = 0.001;
  /** Seconds. `waitForJob` re-arms to the next delayed job itself, so idle workers need not re-poll often. */
  readonly maximumBlockTimeout = 3600;

  private readonly prefix: string;
  private readonly ns: NamespaceStore;
  private readonly store: QueueStore;
  private readonly opts: BackendOptions;
  private readonly client: ClientEntry;
  private closingPromise: Promise<void> | undefined;
  private closed = false;
  private blockingDisconnected = false;
  private cancelJobWait: (() => void) | undefined;
  private cancelEventWait: (() => void) | undefined;
  /** Operations issued before the simulated handshake completed (cf. ioredis' offline queue). */
  private readonly queuedOps: (() => void)[] = [];
  private isConnected = false;
  private readonly connected: Promise<void>;

  constructor(
    queueName: string,
    opts: BackendOptions,
    factoryOptions: { blocking?: boolean; withBlockingConnection?: boolean } = {},
  ) {
    super();
    if (opts.limiter) throw new UnsupportedError('Worker', 'the Worker `limiter` option (rate limiting)');
    if (opts.metrics) throw new UnsupportedError('Worker', 'the Worker `metrics` option (getMetrics)');
    this.opts = opts;
    this.prefix = opts.prefix ?? DEFAULT_PREFIX;
    this.qualifiedName = `${this.prefix}:${queueName}`;
    this.keys = new QueueKeys(this.prefix).getKeys(queueName);
    const connection = opts.connection ?? {};
    this.ns = acquireNamespace(connection.namespace ?? DEFAULT_NAMESPACE, connection.retain === true);
    this.store = this.ns.queue(this.qualifiedName);
    // Same client names BullMQ's Redis/Postgres factories give workers and QueueEvents (Queue.getWorkers()).
    let name: string | undefined;
    if (factoryOptions.withBlockingConnection) name = this.clientName(opts.name ? `:w:${opts.name}` : '');
    else if (factoryOptions.blocking) name = this.clientName(':qe');
    this.client = { name };
    this.ns.clients.add(this.client);
    // Like a new client connection, an instance can execute nothing until one
    // macrotask after it was created. This keeps BullMQ's implicit Redis-era
    // ordering: a worker that is already running beats one created just now.
    this.connected = new Promise((resolve) =>
      defer(() => {
        this.isConnected = true;
        for (const op of this.queuedOps.splice(0)) op();
        resolve();
      }),
    );
  }

  /**
   * Runs a synchronous store transition as one atomic step. The result is
   * delivered on a later macrotask (see `defer`), like a reply from a
   * datastore: code written against Redis relies on every `await backend.op()`
   * yielding to the event loop (e.g. a Worker registers a job with its lock
   * manager while the caller of a getter is still waiting for the reply).
   */
  private run<T>(fn: () => T): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const exec = () => {
        if (this.closed) {
          defer(() => reject(new Error('Connection is closed.')));
          return;
        }
        try {
          const value = fn();
          defer(() => resolve(value));
        } catch (err) {
          defer(() => reject(err));
        }
      };
      if (this.isConnected) exec();
      else this.queuedOps.push(exec);
    });
  }

  private reply<T>(value: T): Promise<T> {
    return new Promise<T>((resolve) => defer(() => resolve(value)));
  }

  // ---- lifecycle -----------------------------------------------------------

  get closing(): Promise<void> | undefined {
    return this.closingPromise;
  }

  /** Resolves once the simulated connection handshake has completed (see the constructor). */
  waitUntilReady(): Promise<void> {
    return this.connected;
  }

  close(_force?: boolean): Promise<void> {
    if (!this.closingPromise) {
      this.closed = true;
      this.cancelJobWait?.();
      this.cancelEventWait?.();
      this.ns.clients.delete(this.client);
      releaseNamespace(this.ns);
      this.closingPromise = Promise.resolve();
      this.emit('close');
    }
    return this.closingPromise;
  }

  disconnect(): Promise<void> {
    return this.close(true);
  }

  setName(name: string): Promise<void> {
    this.client.name = name;
    return Promise.resolve();
  }

  // ---- identity / keys -----------------------------------------------------

  forQueue(queueName: string, _prefix?: string): IQueueBackend {
    throw new UnsupportedError('forQueue', `IQueueBackend.forQueue("${queueName}") (FlowProducer)`);
  }

  toKey(type: string): string {
    return `${this.qualifiedName}:${type}`;
  }

  parseNodeKey(key: string): { prefix: string; queueName: string; id: string } {
    const parts = key.split(':');
    const id = parts.pop() ?? '';
    const queueName = parts.pop() ?? '';
    return { prefix: parts.join(':'), queueName, id };
  }

  clientName(suffix = ''): string {
    return `${this.qualifiedName}${suffix}`;
  }

  // ---- adding ----------------------------------------------------------------

  addJob(job: JobJson, jobId: string): Promise<string> {
    return this.run(() => {
      this.assertAddable(job);
      return this.insertJob(job, jobId);
    });
  }

  addJobs(entries: { job: JobJson; jobId: string }[]): Promise<string[]> {
    return this.run(() => {
      for (const entry of entries) this.assertAddable(entry.job); // all-or-nothing
      return entries.map((entry) => this.insertJob(entry.job, entry.jobId));
    });
  }

  private assertAddable(job: JobJson): void {
    const opts = (job.opts ?? {}) as JobsOptions & { repeat?: unknown; debounce?: unknown };
    if (opts.parent || job.parent || job.parentKey) throw new UnsupportedError('addJob', 'flows (the `parent` job option)');
    if (opts.deduplication || opts.debounce || job.deduplicationId) {
      throw new UnsupportedError('addJob', 'the `deduplication` / `debounce` job option');
    }
    if (opts.repeat || job.repeatJobKey) throw new UnsupportedError('addJob', 'repeatable jobs / job schedulers');
  }

  private insertJob(json: JobJson, jobId: string): string {
    const store = this.store;
    // BullMQ semantics: a custom jobId that already exists is NOT added again; the
    // existing job's id is returned, the stored job stays untouched and `duplicated` is emitted.
    if (jobId && store.jobs.has(jobId)) {
      store.publish('duplicated', { jobId });
      return jobId;
    }
    const id = jobId || store.nextJobId();
    const opts = structuredClone(json.opts ?? {});
    const now = Date.now();
    const job: StoredJob = {
      id,
      name: json.name,
      data: json.data ?? '{}',
      opts,
      timestamp: json.timestamp ?? now,
      delay: Math.max(opts.delay ?? 0, 0),
      priority: opts.priority ?? 0,
      progress: cloneJson(json.progress ?? 0),
      attemptsMade: 0,
      attemptsStarted: 0,
      stalledCounter: 0,
      stacktrace: json.stacktrace,
      returnvalue: json.returnvalue,
      state: 'wait',
      ticket: 0,
      order: 0,
      dueAt: 0,
      stalledMarked: false,
      logs: [],
    };
    store.insert(job);
    store.publish('added', { jobId: id, name: job.name });
    if (job.delay > 0) {
      store.enqueueDelayed(job, job.timestamp + job.delay);
      store.publish('delayed', { jobId: id, delay: job.dueAt });
    } else {
      store.enqueueClaimable(job, !!opts.lifo && job.priority === 0);
      store.publish('waiting', { jobId: id });
    }
    store.signalJobs();
    return id;
  }

  // ---- claiming / finishing --------------------------------------------------

  moveToActive(token: string, name?: string): Promise<NextJob> {
    return this.run(() => this.claim(token, name, Date.now()));
  }

  private get lockDuration(): number {
    return this.opts.lockDuration ?? DEFAULT_LOCK_DURATION;
  }

  private isMaxedSync(): boolean {
    const raw = this.store.meta.get('concurrency');
    return raw !== undefined && this.store.count('active') >= Number(raw);
  }

  /** Promotes due delayed jobs (regardless of pause, like Redis/Postgres). */
  private promoteDue(now: number): void {
    for (const job of this.store.takeDueDelayed(now)) {
      job.delay = 0;
      this.store.enqueueClaimable(job);
      this.store.publish('waiting', { jobId: job.id, prev: 'delayed' });
    }
  }

  private emptyResult(): NextJob {
    return [undefined, '', 0, this.store.nextDelayedDue() ?? 0];
  }

  /** Atomic claim: promote due delayed jobs, then pick, lock and activate the next job. */
  private claim(token: string, workerName: string | undefined, now: number): NextJob {
    this.promoteDue(now);
    if (this.store.paused || this.isMaxedSync()) return this.emptyResult();
    const job = this.store.takeNextClaimable();
    if (!job) return this.emptyResult();
    this.store.setState(job, 'active');
    job.lock = { token, until: now + this.lockDuration };
    job.stalledMarked = false;
    job.processedOn = now;
    job.attemptsStarted++;
    if (workerName) job.processedBy = workerName;
    this.store.publish('active', { jobId: job.id, prev: 'waiting' });
    return [this.toJobJson(job), job.id, 0, 0];
  }

  /** Lock/state check shared by every "leave active" transition (Redis `removeLock` + `LREM active`). */
  private requireActive(jobId: string, token: string | undefined, command: string): StoredJob {
    const job = this.store.jobs.get(jobId);
    if (!job) throw finishedError(ErrorCode.JobNotExist, jobId, command, 'active');
    this.requireLock(job, token, command);
    if (job.state !== 'active') throw finishedError(ErrorCode.JobNotInState, jobId, command, 'active');
    return job;
  }

  /** Redis `removeLock` check: token '0' skips it; an expired lock counts as missing (key TTL). */
  private requireLock(job: StoredJob, token: string | undefined, command: string): void {
    if (token === undefined || token === '0') return;
    if (!job.lock || job.lock.until <= Date.now()) {
      throw finishedError(ErrorCode.JobLockNotExist, job.id, command, 'active');
    }
    if (job.lock.token !== token) throw finishedError(ErrorCode.JobLockMismatch, job.id, command, 'active');
  }

  moveToCompleted<T = any, R = any, N extends string = string>(
    job: MinimalJob<T, R, N>,
    returnValue: R,
    removeOnComplete: boolean | number | KeepJobs,
    token: string,
    fetchNext: boolean,
  ): Promise<{ result: void | any[]; finishedOn: number }> {
    return this.run(() => {
      const stored = this.requireActive(job.id ?? '', token, 'moveToFinished');
      const returnvalue = JSON.stringify(returnValue === undefined ? null : returnValue);
      stored.returnvalue = returnvalue;
      return this.finish(stored, 'completed', normalizeKeep(removeOnComplete, this.opts.removeOnComplete), token, fetchNext, {
        jobId: stored.id,
        returnvalue,
        prev: 'active',
      });
    });
  }

  moveToFailed<T = any, R = any, N extends string = string>(
    job: MinimalJob<T, R, N>,
    failedReason: string,
    removeOnFail: boolean | number | KeepJobs,
    token: string,
    fetchNext: boolean,
    fieldsToUpdate?: Record<string, any>,
  ): Promise<{ result: void | any[]; finishedOn: number }> {
    return this.run(() => {
      const stored = this.requireActive(job.id ?? '', token, 'moveToFinished');
      stored.failedReason = failedReason;
      if (typeof fieldsToUpdate?.stacktrace === 'string') stored.stacktrace = fieldsToUpdate.stacktrace;
      stored.deferredFailure = undefined;
      return this.finish(stored, 'failed', normalizeKeep(removeOnFail, this.opts.removeOnFail), token, fetchNext, {
        jobId: stored.id,
        failedReason,
        prev: 'active',
      });
    });
  }

  private finish(
    job: StoredJob,
    target: 'completed' | 'failed',
    keep: Retention,
    token: string,
    fetchNext: boolean,
    event: Record<string, string>,
  ): { result: void | any[]; finishedOn: number } {
    const store = this.store;
    const finishedOn = Date.now();
    job.finishedOn = finishedOn;
    job.attemptsMade++;
    job.lock = undefined;
    job.stalledMarked = false;
    if (keep.count === 0) store.delete(job);
    else {
      store.setState(job, target);
      this.applyRetention(target, keep, finishedOn);
    }
    store.publish(target, event);
    if (target === 'failed' && job.attemptsMade >= (job.opts.attempts ?? 0)) {
      store.publish('retries-exhausted', { jobId: job.id, attemptsMade: job.attemptsMade });
    }
    if (store.meta.has('concurrency')) store.signalJobs();

    let result: void | any[] = undefined;
    if (fetchNext && !this.closingPromise) {
      result = this.claim(token, this.opts.name, finishedOn);
      if (result[0]) return { result, finishedOn };
    }
    if (store.count('wait') === 0 && store.count('active') === 0 && store.count('prioritized') === 0) {
      store.publish('drained');
    }
    return { result, finishedOn };
  }

  /** removeOnComplete/removeOnFail with `age`/`count` (Redis `removeJobsByMaxAge` / `removeJobsByMaxCount`). */
  private applyRetention(state: 'completed' | 'failed', keep: Retention, now: number): void {
    const store = this.store;
    // The index Set is in finish order, i.e. oldest first.
    if (keep.age !== undefined) {
      const cutoff = now - keep.age * 1000;
      for (const id of store.idsIn(state)) {
        const job = store.jobs.get(id)!;
        if ((job.finishedOn ?? 0) > cutoff) break;
        store.delete(job);
      }
    }
    if (keep.count !== undefined && keep.count > 0) {
      let excess = store.count(state) - keep.count;
      for (const id of store.idsIn(state)) {
        if (excess-- <= 0) break;
        store.delete(store.jobs.get(id)!);
      }
    }
  }

  moveToDelayed(
    jobId: string,
    timestamp: number,
    delay: number,
    token = '0',
    opts: MoveToDelayedOpts = {},
  ): Promise<void | any[]> {
    return this.run(() => {
      const job = this.requireActive(jobId, token, 'moveToDelayed');
      this.applyFields(job, opts.fieldsToUpdate);
      if (!opts.skipAttempt) job.attemptsMade++;
      job.delay = delay;
      this.store.enqueueDelayed(job, timestamp + Math.max(delay, 0));
      this.store.publish('delayed', { jobId, delay: job.dueAt });
      this.store.signalJobs();
      if (opts.fetchNext && !this.closingPromise && token !== '0') {
        const next = this.claim(token, this.opts.name, Date.now());
        if (next[0]) return next;
      }
      return [];
    });
  }

  private applyFields(job: StoredJob, fields: Record<string, any> | undefined): void {
    if (typeof fields?.failedReason === 'string') job.failedReason = fields.failedReason;
    if (typeof fields?.stacktrace === 'string') job.stacktrace = fields.stacktrace;
  }

  retryJob(jobId: string, lifo: boolean, token = '0', opts: RetryJobOpts = {}): Promise<void> {
    return this.run(() => {
      const job = this.requireActive(jobId, token, 'retryJob');
      // retryJob-11.lua promotes due delayed jobs before re-queueing the retried one.
      this.promoteDue(Date.now());
      this.applyFields(job, opts.fieldsToUpdate);
      job.attemptsMade++;
      this.store.enqueueClaimable(job, lifo && job.priority === 0);
      this.store.publish('waiting', { jobId, prev: 'active' });
      this.store.signalJobs();
    });
  }

  moveJobFromActiveToWait(jobId: string, token = '0'): Promise<number> {
    return this.run(() => {
      const job = this.store.jobs.get(jobId);
      if (!job) throw finishedError(ErrorCode.JobNotExist, jobId, 'moveJobFromActiveToWait');
      // moveJobFromActiveToWait-9.lua: removeLock first (−2 missing/expired, −6 other owner) …
      this.requireLock(job, token, 'moveJobFromActiveToWait');
      // … then only an active job moves (LREM active); otherwise nothing happens.
      if (job.state === 'active') {
        // Back to the head of its list (Redis RPUSH / pushBackJobWithPriority): it keeps its turn.
        this.store.enqueueClaimable(job, true);
        this.store.publish('waiting', { jobId, prev: 'active' });
        this.store.signalJobs();
      }
      return 0; // no rate limiter in this backend → no remaining window
    });
  }

  retryFinishedJob<T = any, R = any, N extends string = string>(
    minimal: MinimalJob<T, R, N>,
    state: 'failed' | 'completed',
    opts: RetryOptions = {},
  ): Promise<void> {
    return this.run(() => {
      const id = minimal.id ?? '';
      const job = this.store.jobs.get(id);
      if (!job) throw finishedError(ErrorCode.JobNotExist, id, 'reprocessJob', state);
      if (job.state !== state) throw finishedError(ErrorCode.JobNotInState, id, 'reprocessJob', state);
      job.finishedOn = undefined;
      job.processedOn = undefined;
      if (state === 'completed') job.returnvalue = undefined;
      else job.failedReason = undefined;
      if (opts.resetAttemptsMade) job.attemptsMade = 0;
      if (opts.resetAttemptsStarted) job.attemptsStarted = 0;
      this.store.enqueueClaimable(job, !!job.opts.lifo && job.priority === 0);
      this.store.publish('waiting', { jobId: job.id, prev: state });
      this.store.signalJobs();
    });
  }

  promote(jobId: string): Promise<void> {
    return this.run(() => {
      const job = this.store.jobs.get(jobId);
      if (!job) throw finishedError(ErrorCode.JobNotExist, jobId, 'promote', 'delayed');
      if (job.state !== 'delayed') throw finishedError(ErrorCode.JobNotInState, jobId, 'promote', 'delayed');
      job.delay = 0;
      this.store.enqueueClaimable(job);
      this.store.publish('waiting', { jobId, prev: 'delayed' });
      this.store.signalJobs();
    });
  }

  // ---- locks / stalled -------------------------------------------------------

  extendLock(jobId: string, token: string, duration: number): Promise<number> {
    return this.run(() => (this.extend(jobId, token, duration, Date.now()) ? 1 : 0));
  }

  extendLocks(jobIds: string[], tokens: string[], duration: number): Promise<string[]> {
    return this.run(() => {
      const now = Date.now();
      return jobIds.filter((id, i) => !this.extend(id, tokens[i] ?? '', duration, now));
    });
  }

  private extend(jobId: string, token: string, duration: number, now: number): boolean {
    const job = this.store.jobs.get(jobId);
    // An expired lock is gone (Redis key TTL): it cannot be renewed (extendLock-2.lua GET → nil).
    if (job?.state !== 'active' || !job.lock || job.lock.token !== token || job.lock.until <= now) return false;
    job.lock.until = now + duration;
    job.stalledMarked = false; // Redis extendLock also SREMs the job from the stalled set
    return true;
  }

  /**
   * Two-phase mark/sweep, throttled to once per `stalledInterval` per queue
   * (Redis `moveStalledJobsToWait` / Postgres `move_stalled_jobs_to_wait`): an
   * active job that was marked on the previous pass and whose lock has expired
   * goes back to wait; past `maxStalledCount` it carries a deferred failure the
   * worker turns into "job stalled more than allowable limit".
   */
  moveStalledJobsToWait(): Promise<string[]> {
    return this.run(() => {
      const store = this.store;
      const now = Date.now();
      const interval = this.opts.stalledInterval ?? DEFAULT_STALLED_INTERVAL;
      if (store.lastStalledCheck !== undefined && now < store.lastStalledCheck + interval) return [];
      store.lastStalledCheck = now;
      const maxStalled = this.opts.maxStalledCount ?? DEFAULT_MAX_STALLED_COUNT;
      const stalled: StoredJob[] = [];
      for (const id of store.idsIn('active')) {
        const job = store.jobs.get(id)!;
        if (job.stalledMarked && (!job.lock || job.lock.until < now)) stalled.push(job);
      }
      for (const job of stalled) {
        job.stalledMarked = false;
        job.stalledCounter++;
        if (job.stalledCounter > maxStalled) job.deferredFailure = STALLED_REASON;
        // moveStalledJobsToWait-9.lua: moveJobToWait(…, "RPUSH") puts the job at the head of
        // the plain wait list (even if it has a priority) and emits `waiting`, then `stalled`.
        store.enqueueWait(job, true);
        store.publish('waiting', { jobId: job.id, prev: 'active' });
        store.publish('stalled', { jobId: job.id });
      }
      for (const id of store.idsIn('active')) store.jobs.get(id)!.stalledMarked = true;
      if (stalled.length) store.signalJobs();
      return stalled.map((j) => j.id);
    });
  }

  // ---- bulk admin --------------------------------------------------------------

  retryFinishedJobs(state: FinishedStatus = 'failed', count = 1000, timestamp = Date.now()): Promise<number> {
    return this.run(() => {
      const jobs = this.store.ordered(state).filter((j) => (j.finishedOn ?? 0) <= timestamp).slice(0, count);
      for (const job of jobs) {
        job.finishedOn = undefined;
        job.processedOn = undefined;
        if (state === 'completed') job.returnvalue = undefined;
        else job.failedReason = undefined;
        this.store.enqueueWait(job);
        this.store.publish('waiting', { jobId: job.id, prev: state });
      }
      if (jobs.length) this.store.signalJobs();
      return jobs.length >= count ? 1 : 0;
    });
  }

  promoteJobs(count = 1000): Promise<number> {
    return this.run(() => {
      const jobs = this.store.ordered('delayed').slice(0, count);
      for (const job of jobs) {
        job.delay = 0;
        this.store.enqueueWait(job);
        this.store.publish('waiting', { jobId: job.id, prev: 'delayed' });
      }
      if (jobs.length) this.store.signalJobs();
      return jobs.length >= count ? 1 : 0;
    });
  }

  pause(pause: boolean): Promise<void> {
    return this.run(() => {
      if (pause) this.store.meta.set('paused', '1');
      else this.store.meta.delete('paused');
      this.store.publish(pause ? 'paused' : 'resumed');
      if (!pause) this.store.signalJobs();
    });
  }

  drain(delayed: boolean): Promise<void> {
    return this.run(() => {
      const states: MemoryJobState[] = delayed ? ['wait', 'prioritized', 'delayed'] : ['wait', 'prioritized'];
      for (const state of states) {
        for (const id of [...this.store.idsIn(state)]) this.store.delete(this.store.jobs.get(id)!);
      }
    });
  }

  cleanJobsByState(state: string, timestamp: number, limit = 0): Promise<string[]> {
    return this.run(() => {
      let candidates: StoredJob[];
      const now = Date.now();
      switch (state) {
        case 'completed':
        case 'failed':
          candidates = this.store.ordered(state).filter((j) => (j.finishedOn ?? 0) <= timestamp);
          break;
        // cleanJobsInSet-3.lua: the job's age is the first set field of the listed attributes,
        // so a job processed (or retried) recently stays within the grace period.
        case 'wait':
        case 'waiting':
          candidates = this.store.ordered('wait').filter((j) => (j.finishedOn ?? j.processedOn ?? j.timestamp) <= timestamp);
          break;
        case 'active': // cleanList: locked jobs are skipped
          candidates = this.store
            .ordered('active')
            .filter((j) => (j.finishedOn ?? j.processedOn ?? j.timestamp) <= timestamp && !(j.lock && j.lock.until > now));
          break;
        case 'delayed':
          candidates = this.store.ordered('delayed').filter((j) => (j.processedOn ?? j.timestamp) <= timestamp);
          break;
        case 'prioritized':
          candidates = this.store.ordered('prioritized').filter((j) => j.timestamp <= timestamp);
          break;
        default:
          return [];
      }
      if (limit > 0) candidates = candidates.slice(0, limit);
      for (const job of candidates) this.store.delete(job);
      if (candidates.length) this.store.publish('cleaned', { count: candidates.length });
      return candidates.map((j) => j.id);
    });
  }

  obliterate(opts: { force: boolean; count: number }): Promise<number> {
    return this.run(() => {
      if (!this.store.paused) throw new Error('Cannot obliterate non-paused queue');
      if (!opts.force && this.store.count('active') > 0) throw new Error('Cannot obliterate queue with active jobs');
      this.store.clear();
      return 0;
    });
  }

  /** Jobs and their state live in one record, so orphans cannot exist. */
  removeOrphanedJobs(_count?: number, _limit?: number): Promise<number> {
    return this.run(() => 0);
  }

  // ---- job mutations -----------------------------------------------------------

  private requireJob(jobId: string, command: string): StoredJob {
    const job = this.store.jobs.get(jobId);
    if (!job) throw finishedError(ErrorCode.JobNotExist, jobId, command);
    return job;
  }

  updateData<T = any, R = any, N extends string = string>(job: MinimalJob<T, R, N>, data: T): Promise<void> {
    return this.run(() => {
      this.requireJob(job.id ?? '', 'updateData').data = JSON.stringify(data === undefined ? {} : data);
    });
  }

  updateProgress(jobId: string, progress: JobProgress): Promise<void> {
    return this.run(() => {
      const job = this.requireJob(jobId, 'updateProgress');
      job.progress = cloneJson(progress);
      this.store.publish('progress', { jobId, data: JSON.stringify(progress) });
    });
  }

  addLog(jobId: string, logRow: string, keepLogs?: number): Promise<number> {
    return this.run(() => {
      const job = this.requireJob(jobId, 'addLog');
      job.logs.push(logRow);
      if (keepLogs && job.logs.length > keepLogs) job.logs.splice(0, job.logs.length - keepLogs);
      return job.logs.length;
    });
  }

  clearLogs(jobId: string, keepLogs?: number): Promise<void> {
    return this.run(() => {
      const job = this.store.jobs.get(jobId);
      if (!job) return;
      job.logs = keepLogs ? job.logs.slice(-keepLogs) : [];
    });
  }

  changeDelay(jobId: string, delay: number): Promise<void> {
    return this.run(() => {
      const job = this.store.jobs.get(jobId);
      if (!job) throw finishedError(ErrorCode.JobNotExist, jobId, 'changeDelay', 'delayed');
      if (job.state !== 'delayed') throw finishedError(ErrorCode.JobNotInState, jobId, 'changeDelay', 'delayed');
      job.delay = delay;
      this.store.enqueueDelayed(job, Date.now() + delay);
      this.store.publish('delayed', { jobId, delay: job.dueAt });
      this.store.signalJobs();
    });
  }

  changePriority(jobId: string, priority = 0, lifo = false): Promise<void> {
    return this.run(() => {
      const job = this.requireJob(jobId, 'changePriority');
      job.priority = priority;
      job.opts = { ...job.opts, priority };
      if (job.state === 'wait' || job.state === 'prioritized') {
        // Redis: lifo puts the job at the head of its list / of its priority group.
        this.store.enqueueClaimable(job, lifo);
        this.store.signalJobs();
      }
    });
  }

  remove(jobId: string, _removeChildren: boolean): Promise<number> {
    return this.run(() => {
      const job = this.store.jobs.get(jobId);
      if (!job) return 1; // Redis removeJob-2.lua: nothing to remove counts as removed
      if (job.lock && job.lock.until > Date.now()) return 0;
      const prev = job.state;
      this.store.delete(job);
      this.store.publish('removed', { jobId, prev });
      return 1;
    });
  }

  // ---- queries -----------------------------------------------------------------

  getState(jobId: string): Promise<JobState | 'unknown'> {
    return this.run(() => {
      const state = this.store.jobs.get(jobId)?.state;
      if (state === undefined) return 'unknown';
      return state === 'wait' ? 'waiting' : state;
    });
  }

  isFinished(jobId: string, returnValue?: boolean): Promise<number | [number, string]> {
    return this.run(() => {
      const job = this.store.jobs.get(jobId);
      let status = 0;
      let value = '';
      if (!job) {
        status = -1;
        value = `Missing key for job ${this.toKey(jobId)}. isFinished`;
      } else if (job.state === 'completed') {
        status = 1;
        value = job.returnvalue ?? 'null';
      } else if (job.state === 'failed') {
        status = 2;
        value = job.failedReason ?? '';
      }
      return returnValue ? ([status, value] as [number, string]) : status;
    });
  }

  isMaxed(): Promise<boolean> {
    return this.run(() => this.isMaxedSync());
  }

  isJobInState(state: string, jobId: string): Promise<boolean> {
    return this.run(() => {
      const actual = this.store.jobs.get(jobId)?.state;
      switch (state) {
        // BullMQ 6 pauses with a meta flag only: paused jobs stay in `wait`, and the
        // legacy `paused` list (pre-v6 data) never exists in this backend.
        case 'wait':
        case 'waiting':
          return actual === 'wait';
        case 'paused':
          return false;
        case 'active':
        case 'prioritized':
        case 'delayed':
        case 'completed':
        case 'failed':
          return actual === state;
        case 'waiting-children':
          return false;
        default:
          throw new Error(`Unknown job state: ${state}`);
      }
    });
  }

  getJobData(jobId: string): Promise<JobJson | undefined> {
    return this.run(() => {
      const job = this.store.jobs.get(jobId);
      return job ? this.toJobJson(job) : undefined;
    });
  }

  private toJobJson(job: StoredJob): JobJson {
    const json: Record<string, unknown> = {
      id: job.id,
      name: job.name,
      data: job.data,
      opts: structuredClone(job.opts),
      progress: cloneJson(job.progress),
      attemptsMade: job.attemptsMade,
      attemptsStarted: job.attemptsStarted,
      stalledCounter: job.stalledCounter,
      timestamp: job.timestamp,
      delay: job.delay,
      priority: job.priority,
      finishedOn: job.finishedOn,
      processedOn: job.processedOn,
      failedReason: job.failedReason,
      stacktrace: job.stacktrace,
      returnvalue: job.returnvalue,
      processedBy: job.processedBy,
      deferredFailure: job.deferredFailure,
    };
    for (const key of Object.keys(json)) if (json[key] === undefined) delete json[key];
    return json as unknown as JobJson;
  }

  getJobLogs(jobId: string, start: number, end: number, asc: boolean): Promise<{ logs: string[]; count: number }> {
    return this.run(() => {
      const logs = this.store.jobs.get(jobId)?.logs ?? [];
      return { logs: redisRange(asc ? logs : [...logs].reverse(), start, end), count: logs.length };
    });
  }

  getRateLimitTtl(maxJobs?: number): Promise<number> {
    // No limiter can exist on this backend (setRateLimit / `limiter` are unsupported):
    // Redis answers 0 for "not limited" when maxJobs is given, else -2 (PTTL of a missing key).
    return this.run(() => (maxJobs ? 0 : -2));
  }

  getCounts(types: JobType[]): Promise<number[]> {
    return this.run(() => {
      return types.map((type) => {
        switch (type as string) {
          case 'wait':
          case 'waiting':
            return this.store.count('wait');
          case 'active':
          case 'prioritized':
          case 'delayed':
          case 'completed':
          case 'failed':
            return this.store.count(type as MemoryJobState);
          default:
            return 0;
        }
      });
    });
  }

  getCountsPerPriority(priorities: number[]): Promise<number[]> {
    return this.run(() => {
      const perPriority = new Map<number, number>();
      for (const id of this.store.idsIn('prioritized')) {
        const p = this.store.jobs.get(id)!.priority;
        perPriority.set(p, (perPriority.get(p) ?? 0) + 1);
      }
      return priorities.map((p) => (p === 0 ? this.store.count('wait') : (perPriority.get(p) ?? 0)));
    });
  }

  getRanges(types: JobType[], start = 0, end = -1, asc = false): Promise<[string][]> {
    return this.run(() => {
      const result = types.map((type) => {
        switch (type as string) {
          case 'wait':
          case 'waiting':
            return this.listRange('wait', start, end, asc);
          case 'active':
            return this.listRange('active', start, end, asc);
          case 'prioritized':
          case 'delayed':
          case 'completed':
          case 'failed': {
            const ids = this.store.ordered(type as MemoryJobState).map((j) => j.id);
            return redisRange(asc ? ids : ids.reverse(), start, end);
          }
          default:
            return [];
        }
      });
      return result as unknown as [string][];
    });
  }

  /**
   * Emulates `getRanges-1.lua` for the Redis *lists* (`wait`, `active`): the
   * list holds the newest entry at index 0, and `asc` reads it from the tail.
   * QueueGetters reverses list results when `asc` is set.
   */
  private listRange(state: 'wait' | 'active', start: number, end: number, asc: boolean): string[] {
    const list = this.store.ordered(state).map((j) => j.id).reverse();
    if (!asc) return redisRange(list, start, end);
    const from = end === -1 ? 0 : -(end + 1);
    const to = start === -1 ? 0 : -(start + 1);
    return redisRange(list, from, to);
  }

  getClientList(): Promise<string[]> {
    return this.run(() => {
      const lines: string[] = [];
      let i = 0;
      for (const client of this.ns.clients) if (client.name) lines.push(`id=${++i} name=${client.name}`);
      return [lines.join('\n')];
    });
  }

  // ---- queue metadata ----------------------------------------------------------

  setQueueMeta(values: Record<string, string | number>): Promise<number> {
    return this.run(() => {
      if ('max' in values || 'duration' in values) {
        throw new UnsupportedError('setQueueMeta', 'global rate limiting (Queue.setGlobalRateLimit)');
      }
      let added = 0;
      for (const [field, value] of Object.entries(values)) {
        if (!this.store.meta.has(field)) added++;
        this.store.meta.set(field, String(value));
      }
      if ('concurrency' in values) this.store.signalJobs();
      return added;
    });
  }

  getQueueMetaField(field: string): Promise<string | null> {
    return this.run(() => this.store.meta.get(field) ?? null);
  }

  getQueueMetaFields(fields: string[]): Promise<(string | null)[]> {
    return this.run(() => fields.map((f) => this.store.meta.get(f) ?? null));
  }

  getQueueMeta(): Promise<Record<string, string>> {
    return this.run(() => Object.fromEntries(this.store.meta));
  }

  removeQueueMetaFields(fields: string[]): Promise<number> {
    return this.run(() => {
      let removed = 0;
      for (const f of fields) if (this.store.meta.delete(f)) removed++;
      if (fields.includes('concurrency') || fields.includes('paused')) this.store.signalJobs();
      return removed;
    });
  }

  hasQueueMetaField(field: string): Promise<boolean> {
    return this.run(() => this.store.meta.has(field));
  }

  /** No rate-limit key can exist on this backend. */
  removeRateLimitKey(): Promise<number> {
    return this.run(() => 0);
  }

  /** The deprecated Redis priority helper key never exists here. */
  removeDeprecatedPriorityKey(): Promise<number> {
    return this.run(() => 0);
  }

  trimEvents(maxLength: number): Promise<number> {
    return this.run(() => this.store.trimEvents(maxLength));
  }

  // ---- event stream ------------------------------------------------------------

  publishEvent(fields: Record<string, string | number>, maxEvents: number): Promise<string> {
    return this.run(() => {
      const { event, ...rest } = fields;
      const id = this.store.publish(String(event), rest);
      this.store.trimEvents(maxEvents);
      return id;
    });
  }

  /**
   * `XREAD BLOCK` equivalent: events strictly after `id` (`'$'` = the tail when
   * the read starts), waiting up to `blockTimeout` ms (0 = forever) for
   * the first one. Resolves `null` on timeout or when the instance closes.
   */
  async readEvents(id: string, blockTimeout: number): Promise<StreamReadRaw> {
    // '$' = the stream tail when the XREAD is issued, i.e. at call time, even if the
    // command then waits for the connect gate (a QueueEvents issues its first read
    // right after construction, so events produced after it was created are seen).
    const [ms, seq] = parseStreamId(id === '$' ? this.store.lastEventId : id);
    if (!this.isConnected) await this.connected;
    if (this.closingPromise) return this.reply(null as unknown as StreamReadRaw);
    let events = this.store.eventsAfter(ms, seq);
    if (events.length === 0) {
      await new Promise<void>((resolve) => {
        let timer: NodeJS.Timeout | undefined;
        const finish = () => {
          clearTimeout(timer);
          unsubscribe();
          this.cancelEventWait = undefined;
          resolve();
        };
        const unsubscribe = this.store.onEvent(finish);
        if (blockTimeout > 0) timer = setTimeout(finish, blockTimeout);
        this.cancelEventWait = finish;
      });
      if (this.closingPromise) return null as unknown as StreamReadRaw;
      events = this.store.eventsAfter(ms, seq);
    }
    if (events.length === 0) return null as unknown as StreamReadRaw;
    return this.reply([[this.keys.events!, events.map((e) => [e.id, [...e.fields]])]] as unknown as StreamReadRaw);
  }

  // ---- blocking wait -----------------------------------------------------------

  private claimableNow(now: number): boolean {
    const due = this.store.nextDelayedDue();
    if (due !== undefined && due <= now) return true;
    return this.store.hasClaimable() && !this.store.paused && !this.isMaxedSync();
  }

  /**
   * Check-and-block (no lost wake-up: the check and the subscription happen in
   * the same synchronous step). Resolves `{ score: 0 }` ("check now") when a
   * job may be claimable, `null` on timeout (shortened to the next delayed
   * job's due time) or when interrupted by `disconnectBlocking`/`close`.
   */
  waitForJob(blockTimeout: number): Promise<Marker | null> {
    if (!this.isConnected) return this.connected.then(() => this.waitForJob(blockTimeout));
    if (this.closingPromise || this.blockingDisconnected) return this.reply(null);
    const now = Date.now();
    if (this.claimableNow(now)) return this.reply(CHECK_NOW);
    let waitMs = Math.max(blockTimeout, 0) * 1000;
    const due = this.store.nextDelayedDue();
    if (due !== undefined) waitMs = Math.min(waitMs, due - now);
    return new Promise((resolve) => {
      const finish = (value: Marker | null) => {
        clearTimeout(timer);
        unsubscribe();
        this.cancelJobWait = undefined;
        resolve(value);
      };
      const unsubscribe = this.store.onJobSignal(() => finish(CHECK_NOW));
      // Never arm a 0 ms timer (under fake timers it would never fire).
      const timer = setTimeout(() => finish(null), Math.max(waitMs, 1));
      this.cancelJobWait = () => finish(null);
    });
  }

  disconnectBlocking(_wait?: boolean): Promise<void> {
    this.blockingDisconnected = true;
    this.cancelJobWait?.();
    return Promise.resolve();
  }

  reconnectBlocking(): Promise<void> {
    this.blockingDisconnected = false;
    return Promise.resolve();
  }
}

/** Every `IQueueBackend` member that has no implementation. Calling one rejects with {@link UnsupportedError}. */
const UNSUPPORTED = [
  // flows
  'addFlow', 'moveToWaitingChildren', 'removeUnprocessedChildren', 'removeChildDependency',
  'getDependencyCounts', 'getDependencies', 'getProcessedChildrenValues', 'getIgnoredChildrenFailures',
  'paginate',
  // job schedulers / repeatable jobs
  'addJobScheduler', 'updateJobSchedulerNextMillis', 'removeJobScheduler', 'getJobScheduler',
  'isJobScheduler', 'getJobSchedulerData', 'getJobSchedulersRange', 'getJobSchedulersCount',
  // deduplication
  'removeDeduplicationKey', 'deleteDeduplicationKey', 'getDeduplicationJobId',
  // rate limiting / metrics
  'setRateLimit', 'getMetrics',
] as const;

type UnsupportedName = (typeof UNSUPPORTED)[number];
// Declares the unsupported members on the class type so the factory's IQueueBackend check sees them.
interface MemoryBackend extends Pick<IQueueBackend, UnsupportedName> {}

for (const name of UNSUPPORTED) {
  if (Object.hasOwn(MemoryBackend.prototype, name)) {
    throw new Error(`memory backend: ${name} is both implemented and listed as unsupported`);
  }
  Object.defineProperty(MemoryBackend.prototype, name, {
    value: () => Promise.reject(new UnsupportedError(name)),
    writable: true,
    configurable: true,
  });
}

/**
 * BullMQ `BackendFactory` for the in-memory backend. Use with
 * `withBackend(createMemoryBackend)` or `setDefaultBackendFactory(createMemoryBackend)`.
 */
export const createMemoryBackend: BackendFactory<IQueueBackend, MemoryConnectionOptions> = (name, opts, options) =>
  new MemoryBackend(name, opts, options);
