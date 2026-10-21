// Extracts every ```ts block from the repository README, typechecks the blocks
// against the *built* bullmq-memory-backend package (as an npm consumer would see
// it) and runs each one with Node. A block passes when it exits with code 0 on
// its own within the timeout, so a leaked handle (an unclosed Worker, a timer)
// fails the check. Also smoke-tests the CommonJS entry point.
import { execFileSync, spawnSync } from 'node:child_process';
import { mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const outDir = join(root, '.generated');
const readme = readFileSync(join(root, '..', 'README.md'), 'utf8');
const blocks = [...readme.matchAll(/```ts\r?\n([\s\S]*?)```/g)].map((m) => m[1]);
if (blocks.length === 0) throw new Error('README.md has no ```ts blocks');

rmSync(outDir, { recursive: true, force: true });
mkdirSync(outDir, { recursive: true });
const files = blocks.map((code, i) => {
  const file = join(outDir, `readme-${i + 1}.ts`);
  writeFileSync(file, code);
  return file;
});
writeFileSync(
  join(outDir, 'tsconfig.json'),
  JSON.stringify({
    compilerOptions: {
      target: 'ES2022',
      module: 'NodeNext',
      moduleResolution: 'NodeNext',
      strict: true,
      skipLibCheck: true,
      types: ['node'],
      outDir: 'js',
    },
    include: ['*.ts'],
  }),
);
writeFileSync(join(outDir, 'package.json'), '{ "type": "module" }\n');

const tsc = createRequire(import.meta.url).resolve('typescript/bin/tsc');
execFileSync(process.execPath, [tsc, '-p', join(outDir, 'tsconfig.json')], { stdio: 'inherit' });
console.log(`typechecked ${files.length} README block(s)`);

let failed = 0;
for (let i = 0; i < files.length; i++) {
  const js = join(outDir, 'js', `readme-${i + 1}.js`);
  const started = Date.now();
  const r = spawnSync(process.execPath, [js], { cwd: outDir, encoding: 'utf8', timeout: 30_000 });
  const ok = r.status === 0 && !r.error;
  if (!ok) failed++;
  console.log(`${ok ? 'ok  ' : 'FAIL'} README block ${i + 1} (${Date.now() - started} ms)`);
  if (r.stdout) console.log(r.stdout.replace(/^/gm, '     | '));
  if (!ok) console.log(r.stderr || r.error?.message || `exit ${r.status} / signal ${r.signal}`);
}

// CommonJS consumers (e.g. Jest) resolve the `require` export condition.
const cjs = spawnSync(
  process.execPath,
  ['-e', "const m = require('bullmq-memory-backend'); if (typeof m.createMemoryBackend !== 'function') process.exit(1);"],
  { cwd: root, encoding: 'utf8' },
);
if (cjs.status !== 0) {
  failed++;
  console.log(`FAIL require('bullmq-memory-backend')\n${cjs.stderr}`);
} else console.log("ok   require('bullmq-memory-backend') (CommonJS)");

if (failed) {
  console.error(`${failed} README check(s) failed`);
  process.exit(1);
}
