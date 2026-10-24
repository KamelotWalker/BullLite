// Builds dist/esm (ES modules) and dist/cjs (CommonJS) from src/ with tsc.
import { execFileSync } from 'node:child_process';
import { copyFileSync, rmSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('..', import.meta.url));
const tsc = createRequire(import.meta.url).resolve('typescript/bin/tsc');

rmSync(new URL('../dist', import.meta.url), { recursive: true, force: true });
for (const project of ['tsconfig.build.json', 'tsconfig.build.cjs.json']) {
  execFileSync(process.execPath, [tsc, '-p', project], { cwd: root, stdio: 'inherit' });
}
writeFileSync(new URL('../dist/cjs/package.json', import.meta.url), '{ "type": "commonjs" }\n');
copyFileSync(new URL('../../../LICENSE', import.meta.url), new URL('../LICENSE', import.meta.url));
copyFileSync(new URL('../../../CHANGELOG.md', import.meta.url), new URL('../CHANGELOG.md', import.meta.url));
copyFileSync(new URL('../../../README.md', import.meta.url), new URL('../README.md', import.meta.url));
