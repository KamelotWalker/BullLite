import { EventEmitter } from 'node:events';
import { QueueKeys } from 'bullmq';
import type {
  BackendFactory,
  IQueueBackend,
  JobJson,
  JobState,
  KeepJobs,
  KeysMap,
  QueueBaseOptions,
} from 'bullmq';
import { getStore, type QueueStore, type StoredJob } from './store.ts';

export interface MemoryConnectionOptions {
  /** Isolation domain. Backends created with the same namespace and queue name share jobs. */
  namespace?: string;
}

/** Thrown (as a rejected promise) by every `IQueueBackend` member this backend does not implement. */
export class UnsupportedError extends Error {
  readonly method: string;
  constructor(method: string, detail?: string) {
    super(
      `@bulllite/memory does not support ${detail ?? `IQueueBackend.${method}()`}. ` +
        `Only the Queue.add -> Worker (single attempt, no delay/priority) path is implemented.`,
    );
    this.name = 'UnsupportedError';
    this.method = method;
  }
}

type WorkerishOptions = QueueBaseOptions<MemoryConnectionOptions> & {
  lockDuration?: number;
  maxStalledCount?: number;
};

type NextJob = [JobJson | undefined, string, number, number];

const DEFAULT_NAMESPACE = 'default';
const DEFAULT_LOCK_DURATION = 30_000;
const DEFAULT_MAX_STALLED_COUNT = 1;

class MemoryBackend extends EventEmitter {
  readonly qualifiedName: string;
  readonly keys: KeysMap;
  /** Seconds. The wait primitive is event driven, so any positive value is fine. */
  readonly minimumBlockTimeout = 0.001;

  private readonly namespace: string;
  private readonly queueName: string;
  private readonly store: QueueStore;
  private readonly lockDuration: number;
  private readonly maxStalledCount: number;
  private readonly unsubscribe: () => void;
  private closingPromise: Promise<void> | undefined;
  /** Set when a job was added since the last `waitForJob` consumed it (prevents a lost wake-up). */
  private signaled = false;
  private waiter: { resolve: (v: { member: string; score: number } | null) => void; timer: NodeJS.Timeout } | undefined;

  constructor(queueName: string, opts: WorkerishOptions) {
    super();
    this.queueName = queueName;
    this.namespace = opts.connection?.namespace ?? DEFAULT_NAMESPACE;
    this.store = getStore(this.namespace, queueName);
    this.lockDuration = opts.lockDuration ?? DEFAULT_LOCK_DURATION;
    this.maxStalledCount = opts.maxStalledCount ?? DEFAULT_MAX_STALLED_COUNT;
    this.qualifiedName = `${this.namespace}:${queueName}`;
    this.keys = new QueueKeys(this.namespace).getKeys(queueName);
    this.unsubscribe = this.store.subscribe(() => {
      this.signaled = true;
      this.wake({ member: '0', score: 0 });
    });
  }

  // ---- lifecycle -----------------------------------------------------------

  get closing(): Promise<void> | undefined {
    return this.closingPromise;
  }

  waitUntilReady(): Promise<void> {
    return Promise.resolve();
  }

  close(_force?: boolean): Promise<void> {
    this.closingPromise ??= (async () => {
      this.unsubscribe();
      this.wake(null);
      this.emit('close');
    })();
    return this.closingPromise;
  }

  disconnect(): Promise<void> {
    return this.close(true);
  }

  setName(_name: string): Promise<void> {
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

  setQueueMeta(values: Record<string, string | number>): Promise<number> {
    let added = 0;
    for (const [field, value] of Object.entries(values)) {
      if (!this.store.meta.has(field)) added++;
      this.store.meta.set(field, String(value));
    }
    return Promise.resolve(added);
  }

  // ---- adding ----------------------------------------------------------------

  addJob(job: JobJson, jobId: string): Promise<string> {
    return Promise.resolve(this.addJobSync(job, jobId));
  }

  private addJobSync(job: JobJson, jobId: string): string {
    const opts = job.opts ?? {};
    if (opts.delay) throw new UnsupportedError('addJob', 'the `delay` job option');
    if (opts.priority) throw new UnsupportedError('addJob', 'the `priority` job option');
    if (opts.lifo) throw new UnsupportedError('addJob', 'the `lifo` job option');
    if (opts.parent || job.parentKey) throw new UnsupportedError('addJob', 'flows (`parent` job option)');
    if (opts.deduplication || job.deduplicationId) throw new UnsupportedError('addJob', 'the `deduplication` job option');
    if ((opts as { repeat?: unknown }).repeat || job.repeatJobKey) throw new UnsupportedError('addJob', 'repeatable jobs / job schedulers');
    if (opts.attempts !== undefined && opts.attempts > 1) {
      throw new UnsupportedError('addJob', 'retries (`attempts` > 1)');
    }

    // BullMQ semantics: a custom jobId that already exists is NOT added again; the
    // existing job's id is returned and the stored job stays untouched.
    if (jobId && this.store.jobs.has(jobId)) return jobId;

    const id = jobId || this.store.nextId();
    const stored: StoredJob = { json: { ...structuredClone(job), id }, state: 'wait' };
    this.store.jobs.set(id, stored);
    this.store.pushWait(id);
    this.store.notify();
    return id;
  }

  // ---- claiming / finishing --------------------------------------------------

  moveToActive(token: string, name?: string): Promise<NextJob> {
    return Promise.resolve(this.claim(token, name));
  }

  /** Atomic claim: pick, mutate and lock in one synchronous step. */
  private claim(token: string, workerName?: string): NextJob {
    const id = this.store.shiftWait();
    if (id === undefined) return [undefined, '', 0, 0];
    const stored = this.store.jobs.get(id)!;
    const now = Date.now();
    stored.state = 'active';
    stored.lock = { token, until: now + this.lockDuration };
    stored.json.processedOn = now;
    stored.json.attemptsStarted = (stored.json.attemptsStarted ?? 0) + 1;
    if (workerName) stored.json.processedBy = workerName;
    return [structuredClone(stored.json), id, 0, 0];
  }

  moveToCompleted(
    job: { id?: string },
    returnValue: unknown,
    removeOnComplete: boolean | number | KeepJobs,
    token: string,
    fetchNext: boolean,
  ): Promise<{ result: void | any[]; finishedOn: number }> {
    return Promise.resolve(this.finish(job.id, 'completed', removeOnComplete, token, fetchNext, (json) => {
      json.returnvalue = JSON.stringify(returnValue === undefined ? null : returnValue);
    }));
  }

  moveToFailed(
    job: { id?: string },
    failedReason: string,
    removeOnFail: boolean | number | KeepJobs,
    token: string,
    fetchNext: boolean,
    fieldsToUpdate?: Record<string, any>,
  ): Promise<{ result: void | any[]; finishedOn: number }> {
    return Promise.resolve(this.finish(job.id, 'failed', removeOnFail, token, fetchNext, (json) => {
      json.failedReason = failedReason;
      if (typeof fieldsToUpdate?.stacktrace === 'string') json.stacktrace = fieldsToUpdate.stacktrace;
    }));
  }

  private finish(
    id: string | undefined,
    target: 'completed' | 'failed',
    remove: boolean | number | KeepJobs,
    token: string,
    fetchNext: boolean,
    apply: (json: JobJson) => void,
  ): { result: void | any[]; finishedOn: number } {
    if (remove !== undefined && remove !== true && remove !== false) {
      throw new UnsupportedError(
        target === 'completed' ? 'moveToCompleted' : 'moveToFailed',
        `removeOn${target === 'completed' ? 'Complete' : 'Fail'} with a count/age (only true/false are supported)`,
      );
    }
    const stored = id === undefined ? undefined : this.store.jobs.get(id);
    if (!stored) throw new Error(`Missing key for job ${id}. moveToFinished`);
    if (stored.state !== 'active' || stored.lock?.token !== token) {
      throw new Error(`Missing lock for job ${id}. moveToFinished`);
    }
    const finishedOn = Date.now();
    stored.json.finishedOn = finishedOn;
    stored.json.attemptsMade = (stored.json.attemptsMade ?? 0) + 1;
    apply(stored.json);
    stored.lock = undefined;
    if (remove === true) this.store.jobs.delete(stored.json.id);
    else stored.state = target;

    if (!fetchNext) return { result: undefined, finishedOn };
    const next = this.claim(token);
    return { result: next[0] ? next : [], finishedOn };
  }

  // ---- locks / stalled -------------------------------------------------------

  extendLocks(jobIds: string[], tokens: string[], duration: number): Promise<string[]> {
    return Promise.resolve(jobIds.filter((id, i) => !this.extend(id, tokens[i] ?? '', duration)));
  }

  private extend(jobId: string, token: string, duration: number): boolean {
    const stored = this.store.jobs.get(jobId);
    if (stored?.state !== 'active' || stored.lock?.token !== token) return false;
    stored.lock.until = Date.now() + duration;
    return true;
  }

  moveStalledJobsToWait(): Promise<string[]> {
    const now = Date.now();
    const moved: string[] = [];
    for (const [id, stored] of this.store.jobs) {
      if (stored.state !== 'active' || !stored.lock || stored.lock.until > now) continue;
      stored.lock = undefined;
      stored.json.stalledCounter = (stored.json.stalledCounter ?? 0) + 1;
      if (stored.json.stalledCounter > this.maxStalledCount) {
        stored.state = 'failed';
        stored.json.failedReason = 'job stalled more than allowable limit';
        stored.json.finishedOn = now;
      } else {
        stored.state = 'wait';
        this.store.pushWait(id);
        moved.push(id);
      }
    }
    if (moved.length) this.store.notify();
    return Promise.resolve(moved);
  }

  // ---- reads -----------------------------------------------------------------

  getState(jobId: string): Promise<JobState | 'unknown'> {
    const state = this.store.jobs.get(jobId)?.state;
    return Promise.resolve(state === 'wait' ? 'waiting' : (state ?? 'unknown'));
  }

  getJobData(jobId: string): Promise<JobJson | undefined> {
    const stored = this.store.jobs.get(jobId);
    return Promise.resolve(stored ? structuredClone(stored.json) : undefined);
  }

  // ---- blocking wait ---------------------------------------------------------

  /** Resolves on the next add signal, on timeout (`null`) or on disconnectBlocking/close (`null`). */
  waitForJob(blockTimeout: number): Promise<{ member: string; score: number } | null> {
    if (this.signaled) {
      this.signaled = false;
      return Promise.resolve({ member: '0', score: 0 });
    }
    if (this.closingPromise) return Promise.resolve(null);
    this.wake(null);
    return new Promise((resolve) => {
      const timer = setTimeout(() => this.wake(null), Math.max(0, blockTimeout * 1000));
      this.waiter = { resolve, timer };
    });
  }

  private wake(value: { member: string; score: number } | null): void {
    const waiter = this.waiter;
    if (!waiter) return;
    this.waiter = undefined;
    clearTimeout(waiter.timer);
    if (value) this.signaled = false;
    waiter.resolve(value);
  }

  disconnectBlocking(_wait?: boolean): Promise<void> {
    this.wake(null);
    return Promise.resolve();
  }

  reconnectBlocking(): Promise<void> {
    return Promise.resolve();
  }
}

/** Every `IQueueBackend` member that has no implementation. Calling one rejects with {@link UnsupportedError}. */
const UNSUPPORTED = [
  'addJobs', 'addFlow', 'extendLock', 'addJobScheduler', 'moveToDelayed', 'moveToWaitingChildren',
  'moveJobFromActiveToWait', 'retryJob', 'retryFinishedJob', 'promote', 'retryFinishedJobs',
  'promoteJobs', 'pause', 'drain', 'cleanJobsByState', 'obliterate', 'removeOrphanedJobs',
  'updateData', 'updateProgress', 'addLog', 'clearLogs', 'changeDelay', 'changePriority', 'remove',
  'removeUnprocessedChildren', 'removeChildDependency', 'removeDeduplicationKey',
  'deleteDeduplicationKey', 'updateJobSchedulerNextMillis', 'removeJobScheduler', 'getJobScheduler',
  'isJobScheduler', 'getJobSchedulerData', 'getJobSchedulersRange', 'getJobSchedulersCount',
  'isFinished', 'isMaxed', 'isJobInState', 'getDeduplicationJobId', 'getJobLogs', 'getRateLimitTtl',
  'getCounts', 'getCountsPerPriority', 'getRanges', 'getDependencyCounts', 'getDependencies',
  'getProcessedChildrenValues', 'getIgnoredChildrenFailures', 'getMetrics', 'getClientList',
  'paginate', 'getQueueMetaField', 'getQueueMetaFields', 'getQueueMeta',
  'removeQueueMetaFields', 'hasQueueMetaField', 'setRateLimit', 'removeRateLimitKey',
  'removeDeprecatedPriorityKey', 'trimEvents', 'publishEvent', 'readEvents',
] as const;

type UnsupportedName = (typeof UNSUPPORTED)[number];
// Declares the unsupported members on the class type so `implements`-style checks below see them.
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

export const createMemoryBackend: BackendFactory<IQueueBackend, MemoryConnectionOptions> = (name, opts) =>
  new MemoryBackend(name, opts);
