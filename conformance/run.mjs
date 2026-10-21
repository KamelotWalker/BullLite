// Runs BullMQ v6.4.0's own shared test suite against bullmq-memory-backend.
//
//   BULLMQ_SRC=/path/to/bullmq-checkout-at-v6.4.0 node run.mjs [file filters...]
//
// Without BULLMQ_SRC the tag is cloned into .work/bullmq-src (needs git and network).
// The checkout is never modified: src/ and tests/ are copied into .work/bullmq and
// the suite runs there with BullMQ's own Postgres-run pattern (vitest.postgres.setup.ts):
// an inert test connection plus setDefaultBackendFactory(createMemoryBackend).
// Output: per-file pass/fail/skip table on stdout, .work/results.json, .work/summary.md.
import { execFileSync, spawnSync } from 'node:child_process';
import { cpSync, existsSync, readdirSync, readFileSync, rmSync, writeFileSync, mkdirSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const work = join(here, '.work');
const target = join(work, 'bullmq');
const TAG = 'v6.4.0';

let source = process.env.BULLMQ_SRC;
if (!source) {
  source = join(work, 'bullmq-src');
  if (!existsSync(source)) {
    mkdirSync(work, { recursive: true });
    execFileSync('git', ['clone', '--depth', '1', '--branch', TAG, 'https://github.com/taskforcesh/bullmq.git', source], {
      stdio: 'inherit',
    });
  }
}
// package.json at the tag still says 6.3.19 (semantic-release bumps it after tagging), so check the tag.
const tag = execFileSync('git', ['-C', source, 'describe', '--tags', '--exact-match'], { encoding: 'utf8' }).trim();
if (tag !== TAG) throw new Error(`${source} is at ${tag}, expected ${TAG}`);

rmSync(target, { recursive: true, force: true });
cpSync(join(source, 'src'), join(target, 'src'), { recursive: true });
cpSync(join(source, 'tests'), join(target, 'tests'), { recursive: true });
// src/scripts (Lua) and src/postgres/sql-scripts are build outputs (BullMQ's `pretest`
// generators need ts-node etc.). The published bullmq@6.4.0 tarball ships the same
// generated modules, so take them from there.
const npmBullmq = join(dirname(createRequire(resolve(here, '../packages/memory/package.json')).resolve('bullmq/package.json')), 'dist/esm');
cpSync(join(npmBullmq, 'scripts'), join(target, 'src/scripts'), { recursive: true });
cpSync(join(npmBullmq, 'postgres/sql-scripts.js'), join(target, 'src/postgres/sql-scripts.js'));

// Files that cannot apply to a non-Redis IQueueBackend, with the reason (reported, not run).
const EXCLUDED = [
  [/\.redis\.test\.ts$/, 'raw Redis commands / Redis key layout'],
  [/^(cluster|connection|worker\.ioredis|relay\.ioredis|ioredis-client|node-redis|valkey-glide-client|create-backend|connection-closed-error)\.test\.ts$/, 'Redis client/connection specific'],
  [/^(adapter-conformance|bun-.*)\.test\.ts$/, 'IRedisClient adapter suites (need Redis/Bun)'],
  [/^(script_loader|scripts)\.test\.ts$/, 'Redis Lua script loader'],
  [/^(relay.*)\.test\.ts$/, 'IRelayBackend (Relay class) is out of scope'],
  [/^postgres-.*\.test\.ts$/, 'PostgreSQL backend internals'],
  [/^sandboxed_process\.test\.ts$/, 'excluded by BullMQ’s default config too (ioredis-only run)'],
];
const all = readdirSync(join(target, 'tests')).filter((f) => f.endsWith('.test.ts')).sort();
const excluded = [];
const candidates = [];
for (const file of all) {
  const hit = EXCLUDED.find(([re]) => re.test(file));
  if (hit) excluded.push([file, hit[1]]);
  else if (!/new (Queue|Worker|FlowProducer|QueueEvents)\b/.test(readFileSync(join(target, 'tests', file), 'utf8'))) {
    excluded.push([file, 'does not exercise a backend (pure unit test)']);
  } else candidates.push(file);
}
const filters = process.argv.slice(2);
const included = filters.length ? candidates.filter((f) => filters.some((x) => f.includes(x))) : candidates;

const backendEntry = resolve(here, '../packages/memory/src/index.ts').replaceAll('\\', '/');
writeFileSync(
  join(target, 'vitest.bulllite.setup.ts'),
  `import { setConnectionFactory } from './tests/utils/connection-factory';
import { setDefaultBackendFactory } from './src/utils/create-backend';
import { createMemoryBackend } from '${backendEntry}';

// Inert test connection (same as BullMQ's vitest.postgres.setup.ts): the shared suite
// builds a connection and calls quit() on it, but the memory backend never uses it.
setConnectionFactory(() => {
  const client: Record<string, unknown> = {
    isCluster: false, status: 'ready', options: {},
    connect: async () => undefined, disconnect: () => undefined, quit: async () => 'OK',
    duplicate: () => client, on: () => client, once: () => client, off: () => client,
    removeListener: () => client, emit: () => false,
  };
  return client as never;
});
// The suite's only backend marker: 'postgres' means "non-Redis harness with an inert
// client" (it skips 3 Redis-connection-lifecycle / raw-Redis-fixture tests).
process.env.BULLMQ_TEST_BACKEND = 'postgres';
// One retained namespace per test file process = Redis-like persistence across
// Queue instances; isolation comes from the suite's random queue names.
setDefaultBackendFactory((name, opts, options) =>
  createMemoryBackend(name, { ...opts, connection: { namespace: 'conformance', retain: true } } as never, options) as never,
);
`,
);
writeFileSync(
  join(target, 'vitest.bulllite.config.ts'),
  `import { resolve } from 'node:path';
import { defineConfig } from 'vitest/config';
export default defineConfig({
  resolve: { alias: { bullmq: resolve(__dirname, 'src/index.ts') } },
  test: {
    include: ${JSON.stringify(included.map((f) => `tests/${f}`))},
    setupFiles: ['./vitest.bulllite.setup.ts'],
    testTimeout: 10000,
    hookTimeout: 10000,
    sequence: { concurrent: false },
  },
});
`,
);

const vitest = join(dirname(createRequire(join(here, 'package.json')).resolve('vitest/package.json')), 'vitest.mjs');
const resultsFile = join(work, 'results.json');
rmSync(resultsFile, { force: true });
spawnSync(process.execPath, [vitest, 'run', '--config', 'vitest.bulllite.config.ts', '--reporter=json', `--outputFile=${resultsFile}`, '--reporter=dot'], {
  cwd: target,
  stdio: 'inherit',
});

const results = JSON.parse(readFileSync(resultsFile, 'utf8'));
const rows = [];
const totals = { passed: 0, failed: 0, skipped: 0 };
for (const file of results.testResults) {
  const name = file.name.replaceAll('\\', '/').split('/tests/').pop();
  const c = { passed: 0, failed: 0, skipped: 0 };
  for (const t of file.assertionResults) {
    const k = t.status === 'passed' ? 'passed' : t.status === 'failed' ? 'failed' : 'skipped';
    c[k]++;
    totals[k]++;
  }
  if (file.assertionResults.length === 0 && file.status === 'failed') c.failed = NaN; // suite-level error
  rows.push([name, c]);
}
rows.sort((a, b) => a[0].localeCompare(b[0]));
const lines = [
  `# BullMQ ${TAG} shared suite on bullmq-memory-backend`,
  '',
  `Ran ${new Date().toISOString()} on Node ${process.version}.`,
  '',
  '| File | Passed | Failed | Skipped |',
  '|---|---:|---:|---:|',
  ...rows.map(([n, c]) => `| ${n} | ${c.passed} | ${Number.isNaN(c.failed) ? 'suite error' : c.failed} | ${c.skipped} |`),
  `| **Total** | **${totals.passed}** | **${totals.failed}** | **${totals.skipped}** |`,
  '',
  '## Not run',
  '',
  '| File | Reason |',
  '|---|---|',
  ...excluded.map(([f, r]) => `| ${f} | ${r} |`),
];
writeFileSync(join(work, 'summary.md'), `${lines.join('\n')}\n`);
console.log(`\n${lines.join('\n')}`);
