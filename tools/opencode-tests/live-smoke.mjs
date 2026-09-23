/**
 * Real CopilotKit runtime + real OpenCode + a deterministic local model fixture.
 * OpenCode must execute its native read tool against a temporary file, then
 * continue its own model/tool loop. This test does NOT benchmark an actual LLM.
 * Prerequisites are strict: missing runtime build or OpenCode is a failure.
 */
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { spawn, spawnSync } from "node:child_process";
import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { once } from "node:events";

const EXPECTED_VERSION = "1.18.32";
const executable = process.env.OPENCODE_EXECUTABLE ?? "opencode";
const version = spawnSync(executable, ["--version"], {
  encoding: "utf8",
  timeout: 15000,
});
if (
  version.error ||
  version.status !== 0 ||
  version.stdout.trim() !== EXPECTED_VERSION
) {
  throw new Error(
    `Install opencode-ai@${EXPECTED_VERSION} before the live smoke test. No test was run.`,
  );
}
const { OpenCodeAgent, CopilotRuntime, createCopilotEndpoint } =
  await import("../../packages/runtime/dist/v2/index.mjs");
const root = await mkdtemp(join(tmpdir(), "copilotkit-opencode-smoke-"));
const workspace = join(root, "workspace");
const sentinel = `COPILOTKIT_NATIVE_READ_${crypto.randomUUID()}`;
const password = crypto.randomUUID();
const authorization = `Basic ${Buffer.from(`opencode:${password}`).toString("base64")}`;
let child;
let processError;
let processLog = "";
let modelRequests = 0;
let sawReadResult = false;
let endpoint;
let agent;
let cleanupError;
const threadId = crypto.randomUUID();

const provider = createServer(async (req, res) => {
  try {
    if (req.url !== "/v1/chat/completions" || req.method !== "POST") {
      res.writeHead(404).end();
      return;
    }
    let raw = "";
    for await (const chunk of req) {
      raw += chunk;
      if (raw.length > 2_000_000)
        throw new Error("model fixture request too large");
    }
    const input = JSON.parse(raw);
    if (++modelRequests > 40)
      throw new Error("unexpected model loop in smoke test");
    const read = input.tools?.find((t) => t.function?.name === "read");
    const result = input.messages?.find(
      (m) => m.role === "tool" && JSON.stringify(m.content).includes(sentinel),
    );
    if (result) sawReadResult = true;
    const toolCall = read && !result;
    const content = read
      ? "The workspace file was read successfully."
      : "OpenCode smoke test";
    const id = `chatcmpl_${crypto.randomUUID()}`;
    const tool = {
      id: `call_${crypto.randomUUID()}`,
      type: "function",
      function: {
        name: "read",
        arguments: JSON.stringify({
          filePath: join(workspace, "sentinel.txt"),
        }),
      },
    };
    if (input.stream) {
      res.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
      });
      const chunk = (delta, finish_reason = null) =>
        res.write(
          `data: ${JSON.stringify({
            id,
            object: "chat.completion.chunk",
            created: Math.floor(Date.now() / 1000),
            model: "fixture",
            choices: [{ index: 0, delta, finish_reason }],
          })}\n\n`,
        );
      chunk({ role: "assistant" });
      chunk(toolCall ? { tool_calls: [{ index: 0, ...tool }] } : { content });
      chunk({}, toolCall ? "tool_calls" : "stop");
      res.end("data: [DONE]\n\n");
    } else {
      res.writeHead(200, { "content-type": "application/json" }).end(
        JSON.stringify({
          id,
          object: "chat.completion",
          created: Math.floor(Date.now() / 1000),
          model: "fixture",
          choices: [
            {
              index: 0,
              message: {
                role: "assistant",
                content: toolCall ? null : content,
                ...(toolCall ? { tool_calls: [tool] } : {}),
              },
              finish_reason: toolCall ? "tool_calls" : "stop",
            },
          ],
          usage: { prompt_tokens: 10, completion_tokens: 10, total_tokens: 20 },
        }),
      );
    }
  } catch (error) {
    processError = error;
    if (!res.headersSent) res.writeHead(500);
    res.end("Local smoke provider failed");
  }
});

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  return server.address().port;
}
try {
  await mkdir(workspace);
  await mkdir(join(root, "home"));
  await writeFile(join(workspace, "sentinel.txt"), sentinel);
  const modelPort = await listen(provider);
  // Reserve then release an ephemeral loopback port for the child process.
  const probe = createServer();
  const port = await listen(probe);
  await new Promise((resolve) => probe.close(resolve));
  const config = {
    $schema: "https://opencode.ai/config.json",
    model: "copilotkit-test/fixture",
    small_model: "copilotkit-test/fixture",
    enabled_providers: ["copilotkit-test"],
    share: "disabled",
    autoupdate: false,
    permission: { "*": "deny", read: "allow" },
    provider: {
      "copilotkit-test": {
        npm: "@ai-sdk/openai-compatible",
        name: "CopilotKit local test model",
        options: {
          baseURL: `http://127.0.0.1:${modelPort}/v1`,
          apiKey: "local-test-not-a-secret",
        },
        models: {
          fixture: {
            name: "Fixture",
            tool_call: true,
            limit: { context: 128000, output: 8192 },
          },
        },
      },
    },
  };
  const configPath = join(root, "opencode.json");
  await writeFile(configPath, JSON.stringify(config));
  // No developer/CI model credentials are inherited into the agent subprocess.
  child = spawn(
    executable,
    ["serve", "--hostname", "127.0.0.1", "--port", String(port)],
    {
      cwd: workspace,
      env: {
        PATH: process.env.PATH,
        HOME: join(root, "home"),
        CI: "true",
        XDG_CONFIG_HOME: join(root, "config"),
        XDG_DATA_HOME: join(root, "data"),
        XDG_CACHE_HOME: join(root, "cache"),
        OPENCODE_CONFIG: configPath,
        OPENCODE_SERVER_PASSWORD: password,
        OPENCODE_SERVER_USERNAME: "opencode",
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  child.on("error", (error) => {
    processError = error;
  });
  for (const stream of [child.stdout, child.stderr])
    stream.on("data", (chunk) => {
      processLog = (processLog + chunk).slice(-32000);
    });
  const baseUrl = `http://127.0.0.1:${port}`;
  let healthy = false;
  for (let attempt = 0; attempt < 120; attempt++) {
    if (processError || child.exitCode !== null)
      throw processError ?? new Error(`OpenCode exited: ${processLog}`);
    try {
      const response = await fetch(`${baseUrl}/global/health`, {
        headers: { authorization },
        signal: AbortSignal.timeout(500),
      });
      if (response.ok && (await response.json()).healthy) {
        healthy = true;
        break;
      }
    } catch {
      /* Retry only startup health checks, never agent runs. */
    }
    await delay(500);
  }
  assert.ok(healthy, `OpenCode did not become healthy: ${processLog}`);
  agent = new OpenCodeAgent({
    baseUrl,
    directory: workspace,
    scope: "isolated-smoke",
    model: { providerID: "copilotkit-test", modelID: "fixture" },
    headers: { authorization },
    runTimeoutMs: 60000,
  });
  endpoint = createCopilotEndpoint({
    basePath: "/api/copilotkit",
    runtime: new CopilotRuntime({ agents: { default: agent } }),
  });
  const response = await endpoint.fetch(
    new Request("http://local/api/copilotkit/agent/default/run", {
      method: "POST",
      headers: { "content-type": "application/json" },
      signal: AbortSignal.timeout(75000),
      body: JSON.stringify({
        threadId,
        runId: crypto.randomUUID(),
        tools: [],
        context: [],
        state: {},
        forwardedProps: {},
        messages: [
          {
            id: crypto.randomUUID(),
            role: "user",
            content:
              "Read sentinel.txt using the read tool, then report completion.",
          },
        ],
      }),
    }),
  );
  assert.equal(response.status, 200);
  const output = await response.text();
  assert.doesNotMatch(output, /RUN_ERROR/);
  assert.match(output, /RUN_STARTED/);
  assert.match(output, /TOOL_CALL_RESULT/);
  assert.ok(
    output.includes(sentinel),
    "The actual OpenCode read tool must read the temporary file",
  );
  assert.ok(
    sawReadResult,
    "OpenCode must return the tool result to its own model loop",
  );
  assert.match(output, /RUN_FINISHED/);
  if (processError) throw processError;
  console.log(
    JSON.stringify(
      {
        result: "PASS",
        opencodeVersion: EXPECTED_VERSION,
        actualCopilotKitRuntime: true,
        actualOpenCodeProcess: true,
        actualNativeFileRead: true,
        model: "deterministic local HTTP fixture (not an LLM)",
        browserTested: false,
        modelRequests,
      },
      null,
      2,
    ),
  );
} finally {
  if (agent) await agent.disposeThread(threadId).catch(() => {});
  if (child && child.exitCode === null && child.signalCode === null) {
    const exited = once(child, "exit").catch(() => {});
    child.kill("SIGTERM");
    await Promise.race([exited, delay(2000)]);
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
      await Promise.race([exited, delay(2000)]);
    }
    if (child.exitCode === null && child.signalCode === null) {
      cleanupError = new Error(
        "OpenCode smoke process did not exit after SIGKILL",
      );
    }
  }
  provider.closeAllConnections();
  if (provider.listening)
    await new Promise((resolve) => provider.close(resolve));
  await rm(root, { recursive: true, force: true });
}
if (cleanupError) throw cleanupError;
