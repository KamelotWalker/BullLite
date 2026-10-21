import { ErrorCode } from 'bullmq';

/** Thrown (or returned as a rejected promise) by every BullMQ operation/option this backend does not implement. */
export class UnsupportedError extends Error {
  /** The `IQueueBackend` member, class or option that is not supported. */
  readonly method: string;
  constructor(method: string, detail?: string) {
    super(
      `bullmq-memory-backend does not support ${detail ?? `IQueueBackend.${method}()`}. ` +
        'See docs/compatibility.md for the supported subset.',
    );
    this.name = 'UnsupportedError';
    this.method = method;
  }
}

/**
 * Builds the same `Error` BullMQ's own backends raise for a negative status code
 * (messages copied from bullmq@6.4.0 `src/classes/finished-errors.ts`, MIT), so
 * callers see identical messages and the numeric `code` property.
 */
export function finishedError(code: ErrorCode, jobId: string, command: string, state?: string): Error {
  let error: Error;
  switch (code) {
    case ErrorCode.JobNotExist:
      error = new Error(`Missing key for job ${jobId}. ${command}`);
      break;
    case ErrorCode.JobLockNotExist:
      error = new Error(`Missing lock for job ${jobId}. ${command}`);
      break;
    case ErrorCode.JobNotInState:
      error = new Error(`Job ${jobId} is not in the ${state} state. ${command}`);
      break;
    case ErrorCode.JobLockMismatch:
      error = new Error(`Lock mismatch for job ${jobId}. Cmd ${command} from ${state}`);
      break;
    default:
      error = new Error(`Unknown code ${code} error for ${jobId}. ${command}`);
  }
  (error as Error & { code: number }).code = code;
  return error;
}
