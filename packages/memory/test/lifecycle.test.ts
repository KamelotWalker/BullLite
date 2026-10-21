import { execFile } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const script = fileURLToPath(new URL('./fixtures/lifecycle.mjs', import.meta.url));
const run = (arg: string, timeout: number) =>
  new Promise<{ code: number | null; killed: boolean; stdout: string; stderr: string }>((resolve) => {
    execFile(process.execPath, [script, arg], { timeout }, (err, stdout, stderr) => {
      const e = err as (Error & { code?: number; killed?: boolean }) | null;
      resolve({ code: e ? (e.code ?? null) : 0, killed: e?.killed === true, stdout, stderr });
    });
  });

describe('close() leaves no active handles (built package, child process)', () => {
  it('exits by itself after worker/queue/queueEvents close, even with a far-future delayed job', async () => {
    const r = await run('clean', 15_000);
    expect(r.stderr).toBe('');
    expect(r.stdout).toContain('closed');
    expect(r.killed).toBe(false);
    expect(r.code).toBe(0);
  });

  it('control: with the worker left open the process does NOT exit by itself', async () => {
    const r = await run('leak', 3_000);
    expect(r.stdout).toContain('closed');
    expect(r.killed).toBe(true);
  });
});
