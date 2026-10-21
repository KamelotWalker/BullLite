import { MessageChannel } from 'node:worker_threads';

/**
 * Runs callbacks on later macrotasks, in FIFO order, through a MessageChannel.
 *
 * Backend replies go through here so that every `await backend.op()` yields to
 * the event loop like a reply from a real datastore does. A MessageChannel is
 * used instead of `setImmediate` because fake-timer libraries (vitest, Jest,
 * sinon) patch `setImmediate`/`setTimeout` by default, and a Queue.add() must
 * not hang just because a test installed fake timers. The port is only
 * ref'd while replies are pending, so an idle backend never keeps the process alive.
 */
const pending: (() => void)[] = [];
let channel: MessageChannel | undefined;

function deliver(): void {
  const fn = pending.shift();
  if (pending.length === 0) channel!.port1.unref();
  fn?.();
}

export function defer(fn: () => void): void {
  if (!channel) {
    channel = new MessageChannel();
    channel.port1.on('message', deliver);
  }
  if (pending.length === 0) channel.port1.ref();
  pending.push(fn);
  channel.port2.postMessage(null);
}
