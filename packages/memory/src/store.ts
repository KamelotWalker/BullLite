import type { JobProgress, JobsOptions } from 'bullmq';
import { Deque, Heap } from './structures.ts';

/**
 * Internal job states. `wait` and `prioritized` are both reported as "waiting"
 * by BullMQ's getters except where BullMQ itself distinguishes them
 * (`getState` returns `'prioritized'`, `getJobCounts` has a `prioritized` bucket).
 */
export type MemoryJobState = 'wait' | 'prioritized' | 'delayed' | 'active' | 'completed' | 'failed';

export interface StoredJob {
  readonly id: string;
  name: string;
  /** JSON text, exactly as produced by `Job.asJSON()`. */
  data: string;
  opts: JobsOptions;
  timestamp: number;
  delay: number;
  priority: number;
  progress: JobProgress;
  attemptsMade: number;
  attemptsStarted: number;
  stalledCounter: number;
  processedOn?: number;
  finishedOn?: number;
  failedReason?: string;
  /** JSON text (an array of strings). */
  stacktrace?: string;
  /** JSON text. */
  returnvalue?: string;
  processedBy?: string;
  deferredFailure?: string;
  state: MemoryJobState;
  /** Changes every time the job is (re)queued; stale queue entries carry an older ticket. */
  ticket: number;
  /** Ordering key inside its state (negative = "front"). */
  order: number;
  /** Absolute due time while delayed. */
  dueAt: number;
  lock?: { token: string; until: number };
  /** Set by a stalled-check pass; a job still marked and unlocked on the next pass is stalled. */
  stalledMarked: boolean;
  logs: string[];
}

interface Entry {
  readonly id: string;
  readonly ticket: number;
}
interface PrioritizedEntry extends Entry {
  readonly priority: number;
  readonly order: number;
}
interface DelayedEntry extends Entry {
  readonly dueAt: number;
  readonly order: number;
}

/**
 * Redis prioritized score order: `priority * 2^32 + counter`; jobs pushed to the
 * front of their priority group (pushBackJobWithPriority.lua) all get counter 0
 * (`order` 0 here) and, with equal scores, sort lexicographically by job id.
 */
function comparePrioritized(a: { priority: number; order: number; id: string }, b: typeof a): number {
  return a.priority - b.priority || a.order - b.order || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
}

export interface StreamEvent {
  readonly ms: number;
  readonly seq: number;
  readonly id: string;
  readonly fields: string[];
}

const DEFAULT_MAX_EVENTS = 10_000;

/**
 * All state of one queue (`prefix:name`) inside one namespace.
 *
 * Every mutation is performed by synchronous code with no `await` between the
 * check and the write, which is what makes each backend operation atomic on
 * the single-threaded event loop.
 */
export class QueueStore {
  readonly jobs = new Map<string, StoredJob>();
  readonly meta = new Map<string, string>();
  /** Timestamp of the last stalled-check pass (throttle, like Redis' `stalled-check` key). */
  lastStalledCheck: number | undefined;

  private readonly index: Record<MemoryJobState, Set<string>> = {
    wait: new Set(),
    prioritized: new Set(),
    delayed: new Set(),
    active: new Set(),
    completed: new Set(),
    failed: new Set(),
  };
  private readonly waitList = new Deque<Entry>();
  private readonly prioritizedHeap = new Heap<PrioritizedEntry>(comparePrioritized);
  private readonly delayedHeap = new Heap<DelayedEntry>((a, b) => a.dueAt - b.dueAt || a.order - b.order);
  private idCounter = 0;
  private ticketCounter = 0;

  private events: StreamEvent[] = [];
  private lastEventMs = 0;
  private lastEventSeq = -1;

  private readonly jobWaiters = new Set<() => void>();
  private readonly eventWaiters = new Set<() => void>();

  // ---- ids ---------------------------------------------------------------------

  nextJobId(): string {
    let id: string;
    do id = String(++this.idCounter);
    while (this.jobs.has(id));
    return id;
  }

  // ---- state index -------------------------------------------------------------

  count(state: MemoryJobState): number {
    return this.index[state].size;
  }

  idsIn(state: MemoryJobState): ReadonlySet<string> {
    return this.index[state];
  }

  get paused(): boolean {
    return this.meta.has('paused');
  }

  /** Inserts a brand-new job record in `state` bookkeeping. Caller must route it with an enqueue/set method. */
  insert(job: StoredJob): void {
    this.jobs.set(job.id, job);
    this.index[job.state].add(job.id);
  }

  delete(job: StoredJob): void {
    if (this.jobs.get(job.id) !== job) return;
    this.jobs.delete(job.id);
    this.index[job.state].delete(job.id);
    job.lock = undefined;
  }

  /** Moves `job` to `state` in the index. Ordered states must use the enqueue helpers below. */
  setState(job: StoredJob, state: MemoryJobState): void {
    this.index[job.state].delete(job.id);
    job.state = state;
    // Re-adding moves the id to the end of the Set: active/completed/failed keep transition order.
    this.index[state].add(job.id);
  }

  /** Routes a job that becomes claimable to `wait` (FIFO / LIFO) or `prioritized`. */
  enqueueClaimable(job: StoredJob, front = false): void {
    if (job.priority > 0) this.enqueuePrioritized(job, front);
    else this.enqueueWait(job, front);
  }

  enqueueWait(job: StoredJob, front = false): void {
    this.setState(job, 'wait');
    const ticket = ++this.ticketCounter;
    job.ticket = ticket;
    job.order = front ? -ticket : ticket;
    job.lock = undefined;
    if (front) this.waitList.unshift({ id: job.id, ticket });
    else this.waitList.push({ id: job.id, ticket });
    if (this.waitList.length > 64 && this.waitList.length > 2 * this.index.wait.size) {
      this.waitList.filterInPlace((e) => this.isLive(e, 'wait'));
    }
  }

  enqueuePrioritized(job: StoredJob, front = false): void {
    this.setState(job, 'prioritized');
    const ticket = ++this.ticketCounter;
    job.ticket = ticket;
    job.order = front ? 0 : ticket;
    job.lock = undefined;
    this.prioritizedHeap.push({ id: job.id, ticket, priority: job.priority, order: job.order });
    if (this.prioritizedHeap.length > 64 && this.prioritizedHeap.length > 2 * this.index.prioritized.size) {
      this.prioritizedHeap.filterInPlace((e) => this.isLive(e, 'prioritized'));
    }
  }

  enqueueDelayed(job: StoredJob, dueAt: number): void {
    this.setState(job, 'delayed');
    const ticket = ++this.ticketCounter;
    job.ticket = ticket;
    job.order = ticket;
    job.dueAt = dueAt;
    job.lock = undefined;
    this.delayedHeap.push({ id: job.id, ticket, dueAt, order: ticket });
    if (this.delayedHeap.length > 64 && this.delayedHeap.length > 2 * this.index.delayed.size) {
      this.delayedHeap.filterInPlace((e) => this.isLive(e, 'delayed'));
    }
  }

  private isLive(entry: Entry, state: MemoryJobState): boolean {
    const job = this.jobs.get(entry.id);
    return job !== undefined && job.state === state && job.ticket === entry.ticket;
  }

  /** Removes and returns the next claimable job: plain waiting jobs first (FIFO), then by priority. */
  takeNextClaimable(): StoredJob | undefined {
    for (let e = this.waitList.shift(); e; e = this.waitList.shift()) {
      if (this.isLive(e, 'wait')) return this.jobs.get(e.id);
    }
    for (let e = this.prioritizedHeap.pop(); e; e = this.prioritizedHeap.pop()) {
      if (this.isLive(e, 'prioritized')) return this.jobs.get(e.id);
    }
    return undefined;
  }

  hasClaimable(): boolean {
    return this.index.wait.size > 0 || this.index.prioritized.size > 0;
  }

  /** Due time of the earliest delayed job, or `undefined`. Drops stale heap tops. */
  nextDelayedDue(): number | undefined {
    for (let e = this.delayedHeap.peek(); e; e = this.delayedHeap.peek()) {
      if (this.isLive(e, 'delayed')) return e.dueAt;
      this.delayedHeap.pop();
    }
    return undefined;
  }

  /** Pops every delayed job due at `now` (in due order). The caller re-routes them. */
  takeDueDelayed(now: number): StoredJob[] {
    const due: StoredJob[] = [];
    for (let e = this.delayedHeap.peek(); e && e.dueAt <= now; e = this.delayedHeap.peek()) {
      this.delayedHeap.pop();
      if (this.isLive(e, 'delayed')) due.push(this.jobs.get(e.id)!);
    }
    return due;
  }

  /** Job ids of a state in BullMQ's "natural" order (claim order for lists, score order for sets). */
  ordered(state: MemoryJobState): StoredJob[] {
    const jobs = [...this.index[state]].map((id) => this.jobs.get(id)!);
    switch (state) {
      case 'wait':
        return jobs.sort((a, b) => a.order - b.order);
      case 'prioritized':
        return jobs.sort(comparePrioritized);
      case 'delayed':
        return jobs.sort((a, b) => a.dueAt - b.dueAt || a.order - b.order);
      case 'completed':
      case 'failed':
        return jobs.sort((a, b) => (a.finishedOn ?? 0) - (b.finishedOn ?? 0));
      case 'active':
        return jobs; // Set order = activation order
    }
  }

  // ---- wake-ups ----------------------------------------------------------------

  /** Registers a one-shot callback fired when a job may have become claimable (or due). */
  onJobSignal(fn: () => void): () => void {
    this.jobWaiters.add(fn);
    return () => void this.jobWaiters.delete(fn);
  }

  signalJobs(): void {
    if (this.jobWaiters.size === 0) return;
    const waiters = [...this.jobWaiters];
    this.jobWaiters.clear();
    for (const fn of waiters) fn();
  }

  // ---- event stream ------------------------------------------------------------

  get lastEventId(): string {
    return this.lastEventSeq < 0 ? '0-0' : `${this.lastEventMs}-${this.lastEventSeq}`;
  }

  /** Appends one event (XADD equivalent) and wakes blocked readers. Returns the new id. */
  publish(event: string, fields: Record<string, string | number | undefined> = {}): string {
    const now = Date.now();
    let ms: number;
    let seq: number;
    if (now > this.lastEventMs) {
      ms = now;
      seq = 0;
    } else {
      ms = this.lastEventMs;
      seq = this.lastEventSeq + 1;
    }
    this.lastEventMs = ms;
    this.lastEventSeq = seq;
    const id = `${ms}-${seq}`;
    const flat = ['event', event];
    for (const [k, v] of Object.entries(fields)) if (v !== undefined) flat.push(k, String(v));
    this.events.push({ ms, seq, id, fields: flat });
    this.trimEvents(this.maxEvents);
    if (this.eventWaiters.size > 0) {
      const waiters = [...this.eventWaiters];
      this.eventWaiters.clear();
      for (const fn of waiters) fn();
    }
    return id;
  }

  get maxEvents(): number {
    const raw = Number(this.meta.get('opts.maxLenEvents'));
    return Number.isFinite(raw) && raw >= 0 ? raw : DEFAULT_MAX_EVENTS;
  }

  /** Drops the oldest events beyond `maxLength`. Returns how many were removed. */
  trimEvents(maxLength: number): number {
    const excess = this.events.length - Math.max(0, maxLength);
    if (excess <= 0) return 0;
    this.events = this.events.slice(excess);
    return excess;
  }

  /** Events strictly after `(ms, seq)`, oldest first. */
  eventsAfter(ms: number, seq: number): StreamEvent[] {
    let lo = 0;
    let hi = this.events.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      const e = this.events[mid]!;
      if (e.ms < ms || (e.ms === ms && e.seq <= seq)) lo = mid + 1;
      else hi = mid;
    }
    return this.events.slice(lo);
  }

  onEvent(fn: () => void): () => void {
    this.eventWaiters.add(fn);
    return () => void this.eventWaiters.delete(fn);
  }

  // ---- reset -------------------------------------------------------------------

  /**
   * Drops every job, meta field, event and counter (obliterate / reset). Event ids
   * stay monotonic so a connected QueueEvents never sees an id go backwards.
   */
  clear(): void {
    for (const job of this.jobs.values()) job.lock = undefined;
    this.jobs.clear();
    this.meta.clear();
    for (const set of Object.values(this.index)) set.clear();
    this.waitList.clear();
    this.prioritizedHeap.clear();
    this.delayedHeap.clear();
    this.idCounter = 0;
    this.lastStalledCheck = undefined;
    this.events = [];
    this.signalJobs();
  }
}

/** A client registered by an open backend, reported through `getClientList` (Queue.getWorkers). */
export interface ClientEntry {
  name: string | undefined;
}

/** Every queue of one `connection.namespace`, plus its reference count. */
export class NamespaceStore {
  readonly queues = new Map<string, QueueStore>();
  readonly clients = new Set<ClientEntry>();
  refs = 0;
  retained = false;

  readonly name: string;

  constructor(name: string) {
    this.name = name;
  }

  queue(qualifiedName: string): QueueStore {
    let store = this.queues.get(qualifiedName);
    if (!store) {
      store = new QueueStore();
      this.queues.set(qualifiedName, store);
    }
    return store;
  }
}

const namespaces = new Map<string, NamespaceStore>();

/** Called by every owning backend on construction. */
export function acquireNamespace(name: string, retain: boolean): NamespaceStore {
  let ns = namespaces.get(name);
  if (!ns) {
    ns = new NamespaceStore(name);
    namespaces.set(name, ns);
  }
  ns.refs++;
  if (retain) ns.retained = true;
  return ns;
}

/** Called once by every owning backend on close. The namespace is dropped when no owner is left, unless retained. */
export function releaseNamespace(ns: NamespaceStore): void {
  ns.refs--;
  if (ns.refs <= 0 && !ns.retained && namespaces.get(ns.name) === ns) namespaces.delete(ns.name);
}

/**
 * Deletes all data of `namespace` (every namespace when omitted) and drops
 * `retain` pins. Instances that are still open on the namespace keep working
 * against the now-empty queues (jobs they were processing are gone, so
 * finishing them fails with BullMQ's "Missing key" error).
 */
export function resetMemoryStore(namespace?: string): void {
  const targets = namespace === undefined ? [...namespaces.values()] : [namespaces.get(namespace)].filter((n) => !!n);
  for (const ns of targets) {
    for (const queue of ns.queues.values()) queue.clear();
    ns.retained = false;
    if (ns.refs <= 0) {
      namespaces.delete(ns.name);
    }
  }
}

/** Names of namespaces currently held in memory (open instances or `retain`). Diagnostic helper. */
export function memoryNamespaces(): string[] {
  return [...namespaces.keys()];
}
