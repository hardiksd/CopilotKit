import { test } from "node:test";
import assert from "node:assert/strict";
import { startFixture } from "./fixture.mjs";

// Deliberately load the real built runtime. Missing dependencies/builds FAIL;
// no mock of BuiltInAgent, the runtime, AG-UI, or RxJS substitutes for this test.
const { OpenCodeAgent, CopilotRuntime, createCopilotEndpoint } =
  await import("../../packages/runtime/dist/v2/index.mjs");

function input(extra = {}) {
  return {
    threadId: crypto.randomUUID(),
    runId: crypto.randomUUID(),
    messages: [{ id: crypto.randomUUID(), role: "user", content: "Hello" }],
    tools: [],
    context: [],
    state: {},
    forwardedProps: {},
    ...extra,
  };
}
function run(agent, request) {
  return new Promise((resolve, reject) => {
    const events = [];
    agent.run(request).subscribe({
      next: (e) => events.push(e),
      error: reject,
      complete: () => resolve(events),
    });
  });
}
async function fixture(t, options) {
  const f = await startFixture(options);
  t.after(() => f.close());
  return f;
}

test("actual BuiltInAgent wrapper emits one outer lifecycle pair", async (t) => {
  const f = await fixture(t);
  const agent = new OpenCodeAgent({ baseUrl: f.baseUrl, scope: "test" });
  const events = await run(agent, input());
  assert.equal(events[0].type, "RUN_STARTED");
  assert.equal(events.at(-1).type, "RUN_FINISHED");
  assert.equal(events.filter((e) => e.type === "RUN_STARTED").length, 1);
  assert.equal(events.filter((e) => e.type === "RUN_FINISHED").length, 1);
  assert.equal(
    events
      .filter((e) => e.type === "TEXT_MESSAGE_CONTENT")
      .map((e) => e.delta)
      .join(""),
    "Hello 🌍",
  );
});

test("clones preserve shared session ownership and accurate capabilities", async (t) => {
  const f = await fixture(t);
  const agent = new OpenCodeAgent({ baseUrl: f.baseUrl, scope: "test" });
  agent.agentId = "opencode";
  const first = input();
  await run(agent.clone(), first);
  const clone = agent.clone();
  assert.notEqual(clone, agent);
  assert.equal(clone.agentId, "opencode");
  assert.equal((await clone.getCapabilities()).tools.clientProvided, false);
  await run(clone, {
    ...first,
    runId: crypto.randomUUID(),
    messages: [
      ...first.messages,
      { id: crypto.randomUUID(), role: "user", content: "Next" },
    ],
  });
  assert.equal(f.calls.filter((c) => c.path === "/session").length, 1);
});

test("clones preserve middleware configuration without sharing the list", async (t) => {
  const f = await fixture(t);
  const agent = new OpenCodeAgent({ baseUrl: f.baseUrl, scope: "test" });
  // This deliberately tests the same private-field contract used by the base
  // implementation. No middleware behavior is mocked or claimed here.
  const middleware = { marker: "clone-regression" };
  agent.middlewares = [middleware];
  const clone = agent.clone();
  assert.deepEqual(clone.middlewares, [middleware]);
  assert.notEqual(clone.middlewares, agent.middlewares);
});

test("native permissions become AG-UI interrupts, not run errors", async (t) => {
  const f = await fixture(t, {
    onPrompt({ s, emit, info }) {
      emit(info(s));
      emit({
        type: "permission.asked",
        properties: {
          sessionID: s.id,
          id: "permission-test",
          permission: "bash",
          patterns: ["echo hello"],
        },
      });
    },
    onReply({ sessions, finish }) {
      finish([...sessions.values()][0], "approved");
    },
  });
  const agent = new OpenCodeAgent({ baseUrl: f.baseUrl, scope: "test" });
  const request = input();
  const first = await run(agent.clone(), request);
  assert.equal(first.at(-1).type, "RUN_FINISHED");
  assert.equal(first.at(-1).outcome.type, "interrupt");
  assert.equal(first.at(-1).outcome.interrupts[0].id, "permission-test");
  assert.equal(first.at(-1).outcome.interrupts[0].metadata.permission, "bash");
  const resumed = await run(agent.clone(), {
    ...request,
    runId: crypto.randomUUID(),
    resume: [
      {
        interruptId: "permission-test",
        status: "resolved",
        payload: { approved: true },
      },
    ],
  });
  assert.equal(resumed.at(-1).type, "RUN_FINISHED");
  assert.equal(
    f.calls.filter((c) => c.path.endsWith("/prompt_async")).length,
    1,
  );
});

test("actual runtime endpoint advertises and streams the OpenCode agent", async (t) => {
  const f = await fixture(t);
  const endpoint = createCopilotEndpoint({
    basePath: "/api/copilotkit",
    runtime: new CopilotRuntime({
      agents: {
        default: new OpenCodeAgent({ baseUrl: f.baseUrl, scope: "test" }),
      },
    }),
  });
  const info = await endpoint.fetch(
    new Request("http://local/api/copilotkit/info"),
  );
  assert.equal(info.status, 200);
  const body = await info.json();
  assert.equal(body.agents.default.capabilities.tools.clientProvided, false);
  const response = await endpoint.fetch(
    new Request("http://local/api/copilotkit/agent/default/run", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(input()),
    }),
  );
  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type"), /text\/event-stream/);
  const stream = await response.text();
  assert.match(stream, /RUN_STARTED/);
  assert.match(stream, /TEXT_MESSAGE_CONTENT/);
  assert.match(stream, /RUN_FINISHED/);
  assert.doesNotMatch(stream, /RUN_ERROR/);
});
