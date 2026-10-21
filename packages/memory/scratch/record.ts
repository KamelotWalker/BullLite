// Discovery tool: wraps a backend factory in a Proxy that logs every member access/call (in order).
//  - inner === null: "pure" mode; every member throws NotImplemented(<name>) except those in `defaults`.
//  - inner given:    delegates to the real backend; `allow` (if non-empty) restricts callable members,
//                    others throw NotImplemented(<name>).
import type { BackendFactory, IQueueBackend } from 'bullmq';

export interface Call { seq: number; who: string; kind: 'get' | 'call'; name: string }

export function recordingFactory(
  inner: BackendFactory<IQueueBackend, any> | null,
  log: Call[],
  opts: { allow?: Set<string>; defaults?: Record<string, unknown> } = {},
): BackendFactory<IQueueBackend, any> {
  let seq = 0;
  const { allow, defaults = {} } = opts;
  return (name, o, flags) => {
    const real: any = inner ? inner(name, o, flags) : {};
    const who = flags?.withBlockingConnection ? 'worker' : flags?.blocking ? 'events' : 'queue';
    return new Proxy(real, {
      get(t, p) {
        if (typeof p !== 'string') return Reflect.get(t, p);
        const v = p in defaults ? defaults[p] : t[p];
        if (typeof v === 'function') {
          return (...a: unknown[]) => {
            log.push({ seq: seq++, who, kind: 'call', name: p });
            if (allow && !allow.has(p)) throw new Error(`NotImplemented: ${p}`);
            return v.apply(p in defaults ? t : t, a);
          };
        }
        log.push({ seq: seq++, who, kind: 'get', name: p });
        if (inner === null && !(p in defaults)) throw new Error(`NotImplemented(prop): ${p}`);
        if (allow && !(p in t) && !(p in defaults)) throw new Error(`NotImplemented(prop): ${p}`);
        return v;
      },
    }) as IQueueBackend;
  };
}
