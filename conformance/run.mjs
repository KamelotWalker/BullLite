// Runs BullMQ's own shared test suite against bullmq-memory-backend and gates the
// result against a committed baseline of passing tests (conformance/baseline.json).
//
//   node run.mjs [file filters...]                      # run + gate (exit 1 on regression)
//   node run.mjs --update-baseline                     # run + rewrite baseline.json from this run
//   BULLMQ_SRC=/path/to/bullmq-checkout node run.mjs   # use an existing checkout
//
// The BullMQ version is the one installed in packages/memory (see `pnpm bullmq:use`);
// its git tag v<version> is cloned into .work/bullmq-src-<version> (needs git and network)
// unless BULLMQ_SRC points at a checkout of that tag. The checkout is never modified:
// src/ and tests/ are copied into .work/bullmq and the suite runs there with BullMQ's own
// Postgres-run pattern (vitest.postgres.setup.ts): an inert test connection plus
// setDefaultBackendFactory(createMemoryBackend).
//
// Gate (see docs/conformance.md):
// - every baseline test that runs must pass; a failure is re-run once (whole file) and
//   counts as a regression only if it fails again (timing-sensitive upstream tests);
// - a test file that errors before running its tests is a regression;
// - baseline tests that do not exist in this BullMQ version (renamed, removed, not yet
//   added) are reported; they fail the gate only when the version equals the baseline's;
// - tests that are not in the baseline never fail the gate (new upstream tests, known
//   unsupported features); newly passing ones are listed so the baseline can be refreshed;
// - tests listed under `ignored` in baseline.json (timing-dependent, with a reason) are
//   never gated; the list survives --update-baseline.
// Output: per-file table and gate report on stdout, .work/results.json, .work/summary.md.
import { execFileSync, spawnSync } from 'node:child_process';
import { cpSync, existsSync, readdirSync, readFileSync, rmSync, writeFileSync, mkdirSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const work = join(here, '.work');
const target = join(work, 'bullmq');
const baselineFile = join(here, 'baseline.json');

const args = process.argv.slice(2);
const updateBaseline = args.includes('--update-baseline');
const filters = args.filter((a) => !a.startsWith('--'));
if (updateBaseline && filters.length) throw new Error('--update-baseline needs a full run (no file filters)');

const bullmqPkg = createRequire(resolve(here, '../packages/memory/package.json')).resolve('bullmq/package.json');
const VERSION = JSON.parse(readFileSync(bullmqPkg, 'utf8')).version;
const TAG = `v${VERSION}`;

let source = process.env.BULLMQ_SRC;
if (!source) {
  source = join(work, `bullmq-src-${VERSION}`);
  if (!existsSync(source)) {
    mkdirSync(work, { recursive: true });
    execFileSync('git', ['clone', '--depth', '1', '--branch', TAG, 'https://github.com/taskforcesh/bullmq.git', source], {
      stdio: 'inherit',
    });
  }
}
// package.json at a tag still has the previous version (semantic-release bumps it after
// tagging), so check the tag itself.
const tag = execFileSync('git', ['-C', source, 'describe', '--tags', '--exact-match'], { encoding: 'utf8' }).trim();
if (tag !== TAG) throw new Error(`${source} is at ${tag}, expected ${TAG} (the installed bullmq)`);

rmSync(target, { recursive: true, force: true });
cpSync(join(source, 'src'), join(target, 'src'), { recursive: true });
cpSync(join(source, 'tests'), join(target, 'tests'), { recursive: true });
// src/scripts (Lua) and, from 6.4.0, src/postgres/sql-scripts are build outputs (BullMQ's
// `pretest` generators need ts-node etc.). The published tarball of the same version ships
// the same generated modules, so take them from there.
const npmBullmq = join(dirname(bullmqPkg), 'dist/esm');
cpSync(join(npmBullmq, 'scripts'), join(target, 'src/scripts'), { recursive: true });
if (existsSync(join(npmBullmq, 'postgres/sql-scripts.js'))) {
  cpSync(join(npmBullmq, 'postgres/sql-scripts.js'), join(target, 'src/postgres/sql-scripts.js'));
}

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

/** Runs vitest on `files` (test file names) and returns Map<file, {error, tests: Map<name, status>}>. */
function runSuite(files, outFile) {
  rmSync(outFile, { force: true });
  spawnSync(
    process.execPath,
    [vitest, 'run', '--config', 'vitest.bulllite.config.ts', '--reporter=json', `--outputFile=${outFile}`, '--reporter=dot', ...files.map((f) => `tests/${f}`)],
    { cwd: target, stdio: 'inherit' },
  );
  if (!existsSync(outFile)) throw new Error(`vitest wrote no results (${outFile})`);
  const byFile = new Map();
  for (const file of JSON.parse(readFileSync(outFile, 'utf8')).testResults) {
    const name = file.name.replaceAll('\\', '/').split('/tests/').pop();
    const tests = new Map();
    for (const t of file.assertionResults) {
      const id = [...t.ancestorTitles, t.title].join(' > ');
      const status = t.status === 'passed' ? 'passed' : t.status === 'failed' ? 'failed' : 'skipped';
      // Same-named tests: the name counts as passed only if every occurrence passed.
      const prev = tests.get(id);
      tests.set(id, prev === 'failed' || status === 'failed' ? 'failed' : prev === 'skipped' || status === 'skipped' ? 'skipped' : status);
    }
    // A file that fails before running tests (import/collection error) has no results.
    const error = file.assertionResults.length === 0 && file.status === 'failed' ? file.message || 'suite error' : null;
    byFile.set(name, { error, tests, counts: countStatuses(file.assertionResults) });
  }
  return byFile;
}

function countStatuses(assertionResults) {
  const c = { passed: 0, failed: 0, skipped: 0 };
  for (const t of assertionResults) c[t.status === 'passed' ? 'passed' : t.status === 'failed' ? 'failed' : 'skipped']++;
  return c;
}

const results = runSuite(included, join(work, 'results.json'));

const rows = [...results].sort((a, b) => a[0].localeCompare(b[0]));
const totals = { passed: 0, failed: 0, skipped: 0 };
for (const [, r] of rows) for (const k of Object.keys(totals)) totals[k] += r.counts[k];
const lines = [
  `# BullMQ ${TAG} shared suite on bullmq-memory-backend`,
  '',
  `Ran ${new Date().toISOString()} on Node ${process.version}.`,
  '',
  '| File | Passed | Failed | Skipped |',
  '|---|---:|---:|---:|',
  ...rows.map(([n, r]) => `| ${n} | ${r.counts.passed} | ${r.error ? 'suite error' : r.counts.failed} | ${r.counts.skipped} |`),
  `| **Total** | **${totals.passed}** | **${totals.failed}** | **${totals.skipped}** |`,
];

// `ignored` (kept across refreshes, edited by hand): { file: { test name: reason } } for tests
// whose outcome is decided by timing rather than by the backend. They never fail the gate
// and never enter `passing`.
const previous = existsSync(baselineFile) ? JSON.parse(readFileSync(baselineFile, 'utf8')) : {};
const ignored = previous.ignored ?? {};
const isIgnored = (file, id) => Object.hasOwn(ignored[file] ?? {}, id);

let gateFailed = false;
if (updateBaseline) {
  const passing = {};
  for (const [file, r] of rows) {
    const ids = [...r.tests].filter(([id, s]) => s === 'passed' && !isIgnored(file, id)).map(([id]) => id).sort();
    if (ids.length) passing[file] = ids;
  }
  const count = Object.values(passing).reduce((n, ids) => n + ids.length, 0);
  writeFileSync(
    baselineFile,
    `${JSON.stringify({ bullmq: VERSION, generated: new Date().toISOString().slice(0, 10), count, ignored, passing }, null, 2)}\n`,
  );
  lines.push('', `Baseline rewritten: ${count} passing tests recorded for bullmq ${VERSION} (conformance/baseline.json).`);
} else {
  const baseline = previous;
  if (!baseline.passing) throw new Error(`${baselineFile} is missing; create it with --update-baseline`);
  const sameVersion = baseline.bullmq === VERSION;
  const failing = []; // [file, id]
  const missing = [];
  const skippedNow = [];
  const brokenFiles = [];
  for (const [file, ids] of Object.entries(baseline.passing)) {
    if (!included.includes(file)) {
      if (!filters.length) missing.push(...ids.map((id) => [file, id, 'file not in this BullMQ version']));
      continue;
    }
    const r = results.get(file);
    if (!r || r.error) {
      brokenFiles.push([file, r?.error ?? 'no results']);
      continue;
    }
    for (const id of ids.filter((x) => !isIgnored(file, x))) {
      const s = r.tests.get(id);
      if (s === undefined) missing.push([file, id, 'test not in this BullMQ version']);
      else if (s === 'skipped') skippedNow.push([file, id]);
      else if (s === 'failed') failing.push([file, id]);
    }
  }
  // Re-run the files of failing baseline tests once; only a second failure is a regression.
  let regressions = failing;
  let flaky = [];
  if (failing.length) {
    const retryFiles = [...new Set(failing.map(([f]) => f))];
    console.log(`\nRe-running ${retryFiles.length} file(s) with ${failing.length} failing baseline test(s): ${retryFiles.join(', ')}`);
    const retry = runSuite(retryFiles, join(work, 'results-retry.json'));
    regressions = failing.filter(([f, id]) => retry.get(f)?.tests.get(id) !== 'passed');
    flaky = failing.filter(([f, id]) => retry.get(f)?.tests.get(id) === 'passed');
  }
  const baselineIds = new Set(Object.entries(baseline.passing).flatMap(([f, ids]) => ids.map((id) => `${f}\u0000${id}`)));
  const improved = rows.flatMap(([f, r]) =>
    [...r.tests]
      .filter(([id, s]) => s === 'passed' && !baselineIds.has(`${f}\u0000${id}`) && !isIgnored(f, id))
      .map(([id]) => [f, id]),
  );
  const missingFails = sameVersion && missing.length > 0;
  gateFailed = regressions.length > 0 || brokenFiles.length > 0 || missingFails;

  const list = (items, fmt) => items.map((x) => `- ${fmt(x)}`);
  lines.push(
    '',
    `## Gate: ${gateFailed ? 'FAILED' : 'passed'}`,
    '',
    `Baseline: ${baseline.count} passing tests recorded on bullmq ${baseline.bullmq} (${baseline.generated}); this run: bullmq ${VERSION}.`,
    '',
    `- Regressions (baseline test failed twice): **${regressions.length}**`,
    `- Broken files (suite error): **${brokenFiles.length}**`,
    `- Flaky (failed, passed on re-run): ${flaky.length}`,
    `- Baseline tests absent from this version: ${missing.length}${missing.length ? (missingFails ? ' (fails the gate: same BullMQ version as the baseline)' : ' (allowed: different BullMQ version)') : ''}`,
    `- Baseline tests skipped by the suite: ${skippedNow.length}`,
    `- Passing tests not in the baseline: ${improved.length}${improved.length ? ' (refresh with `pnpm conformance --update-baseline`)' : ''}`,
    `- Ignored (timing-dependent, see baseline.json): ${Object.values(ignored).reduce((n, o) => n + Object.keys(o).length, 0)}`,
  );
  const section = (title, items, fmt) => (items.length ? ['', `### ${title}`, '', ...list(items, fmt)] : []);
  lines.push(
    ...section('Regressions', regressions, ([f, id]) => `${f} — ${id}`),
    ...section('Broken files', brokenFiles, ([f, e]) => `${f} — ${String(e).split('\n')[0]}`),
    ...section('Flaky', flaky, ([f, id]) => `${f} — ${id}`),
    ...section('Absent from this version', missing, ([f, id, why]) => `${f} — ${id} (${why})`),
    ...section('Skipped by the suite', skippedNow, ([f, id]) => `${f} — ${id}`),
    ...section('Passing, not in the baseline', improved, ([f, id]) => `${f} — ${id}`),
  );
}

lines.push(
  '',
  '## Not run',
  '',
  '| File | Reason |',
  '|---|---|',
  ...excluded.map(([f, r]) => `| ${f} | ${r} |`),
);
writeFileSync(join(work, 'summary.md'), `${lines.join('\n')}\n`);
console.log(`\n${lines.join('\n')}`);
if (gateFailed) process.exit(1);
