// Installs another bullmq version into the whole workspace (CI matrix, local checks):
//
//   pnpm bullmq:use oldest   # lowest version of the peer range in packages/memory/package.json
//   pnpm bullmq:use latest   # newest published version inside the peer range
//   pnpm bullmq:use 6.3.12   # an exact version (may lie outside the range, e.g. to probe it)
//
// It writes a pnpm `overrides` entry into pnpm-workspace.yaml and runs
// `pnpm install --no-frozen-lockfile`, so it changes pnpm-workspace.yaml and
// pnpm-lock.yaml: meant for throwaway CI checkouts; locally, revert both files
// (`git checkout pnpm-workspace.yaml pnpm-lock.yaml && pnpm install`).
import { execFileSync } from 'node:child_process';
import { appendFileSync, readFileSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const memoryPkg = join(root, 'packages/memory/package.json');
const range = JSON.parse(readFileSync(memoryPkg, 'utf8')).peerDependencies.bullmq;
const which = process.argv[2];
if (!which) throw new Error('usage: node scripts/use-bullmq.mjs <oldest|latest|x.y.z>');

// The peer range must be a caret range: ^x.y.z = >=x.y.z <(x+1).0.0.
const m = /^\^(\d+)\.(\d+)\.(\d+)$/.exec(range);
if (!m) throw new Error(`unsupported peer range "${range}" (expected ^x.y.z)`);
const floor = m.slice(1).map(Number);
const cmp = (a, b) => a[0] - b[0] || a[1] - b[1] || a[2] - b[2];

let version;
if (which === 'oldest') version = floor.join('.');
else if (which === 'latest') {
  const res = await fetch('https://registry.npmjs.org/bullmq', { headers: { accept: 'application/vnd.npm.install-v1+json' } });
  if (!res.ok) throw new Error(`npm registry: HTTP ${res.status}`);
  const inRange = Object.keys((await res.json()).versions)
    .filter((v) => /^\d+\.\d+\.\d+$/.test(v)) // no prereleases
    .map((v) => v.split('.').map(Number))
    .filter((v) => v[0] === floor[0] && cmp(v, floor) >= 0)
    .sort(cmp);
  if (!inRange.length) throw new Error(`no published bullmq version satisfies ${range}`);
  version = inRange.at(-1).join('.');
} else if (/^\d+\.\d+\.\d+$/.test(which)) version = which;
else throw new Error(`unknown version selector "${which}"`);

// pnpm-workspace.yaml: drop a previous override block, then pin bullmq everywhere and
// exempt the version from minimumReleaseAge (a release younger than a day would be refused).
const wsFile = join(root, 'pnpm-workspace.yaml');
const ws = readFileSync(wsFile, 'utf8')
  .replace(/\r\n/g, '\n')
  .replace(/\n# use-bullmq[\s\S]*$/, '\n')
  .replace(/^ {2}- bullmq@\S+ # use-bullmq\n/gm, '');
if (!/^minimumReleaseAgeExclude:\n/m.test(ws)) throw new Error('pnpm-workspace.yaml has no minimumReleaseAgeExclude list');
const withExclude = ws.replace(/^minimumReleaseAgeExclude:\n/m, `minimumReleaseAgeExclude:\n  - bullmq@${version} # use-bullmq\n`);
writeFileSync(wsFile, `${withExclude.trimEnd()}\n# use-bullmq (generated, do not commit)\noverrides:\n  bullmq: ${version}\n`);

// npm_execpath is the pnpm that runs this script: a JS entry (pnpm ≤ 10) or a native binary.
const pnpm = process.env.npm_execpath;
if (!pnpm) throw new Error('run through pnpm (`pnpm bullmq:use ...`) so the same pnpm binary is reused');
const [cmd, args] = /\.[cm]?js$/.test(pnpm) ? [process.execPath, [pnpm]] : [pnpm, []];
execFileSync(cmd, [...args, 'install', '--no-frozen-lockfile'], { cwd: root, stdio: 'inherit' });

const installed = JSON.parse(
  readFileSync(createRequire(memoryPkg).resolve('bullmq/package.json'), 'utf8'),
).version;
if (installed !== version) throw new Error(`requested bullmq ${version}, but ${installed} is installed`);
console.log(`bullmq ${installed} installed (peer range ${range})`);
if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `version=${installed}\n`);
