import { spawnSync } from "node:child_process";
import { mkdirSync, writeFileSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
const root = fileURLToPath(new URL("../../", import.meta.url));
const generated = path.join(root, "tools/opencode-tests/.generated");
mkdirSync(generated, { recursive: true });
writeFileSync(path.join(generated, "package.json"), '{"type":"commonjs"}\n');
const localTsc = path.join(root, "node_modules/.bin/tsc");
const tsc = existsSync(localTsc) ? localTsc : "tsc";
const compile = spawnSync(
  tsc,
  [
    "--strict",
    "--target",
    "ES2022",
    "--module",
    "CommonJS",
    "--lib",
    "ES2023,DOM",
    "--outDir",
    generated,
    "packages/runtime/src/agent/opencode/bridge.ts",
  ],
  { cwd: root, stdio: "inherit" },
);
if (compile.status !== 0) process.exit(compile.status ?? 1);
const test = spawnSync(
  process.execPath,
  ["--test", "--test-timeout=10000", "tools/opencode-tests/bridge.test.mjs"],
  { cwd: root, stdio: "inherit" },
);
process.exit(test.status ?? 1);
