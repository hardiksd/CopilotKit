import { test } from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { startFixture } from "./fixture.mjs";
const require = createRequire(import.meta.url);
const {
  createOpenCodeBridge,
  OpenCodeEventTranslator,
  OpenCodeInterrupt,
  OpenCodeSessionStore,
  readOpenCodeSse,
} = require("./.generated/bridge.js");

function input(overrides = {}) {
  return {
    threadId: "thread1",
    runId: "run1",
    messages: [{ id: "user1", role: "user", content: "Hello" }],
    tools: [],
    state: {},
    ...overrides,
  };
}
async function collect(
  bridge,
  request = input(),
  signal = new AbortController().signal,
) {
  return Array.fromAsync(
    bridge.stream({ input: request, abortSignal: signal }),
  );
}
async function fixture(t, config = {}) {
  const f = await startFixture(config);
  t.after(() => f.close());
  return f;
}
const transcript = (events) =>
  events
    .filter((e) => e.type === "TEXT_MESSAGE_CONTENT")
    .map((e) => e.delta)
    .join("");
const code = (expected) => (error) => error.code === expected;
const translatorTextPart = (value) => ({
  type: "message.part.updated",
  properties: {
    part: {
      id: "t",
      sessionID: "s",
      messageID: "a",
      type: "text",
      text: value,
    },
  },
});

test("SSE: fragmented UTF-8, CRLF, comments and multi-line data", async () => {
  const encoded = new TextEncoder().encode(
    ': ping\r\ndata: {"type":"text",\r\ndata: "value":"🌍"}\r\n\r\n',
  );
  const body = new ReadableStream({
    start(c) {
      for (const b of encoded) c.enqueue(new Uint8Array([b]));
      c.close();
    },
  });
  assert.deepEqual(await Array.fromAsync(readOpenCodeSse(body)), [
    { type: "text", value: "🌍" },
  ]);
});
test("SSE rejects malformed JSON", async () => {
  await assert.rejects(
    Array.fromAsync(readOpenCodeSse(new Response("data: broken\n\n").body)),
    code("INVALID_SSE"),
  );
});
test("SSE rejects a truncated final frame", async () => {
  await assert.rejects(
    Array.fromAsync(readOpenCodeSse(new Response('data: {"type":"x"}').body)),
    code("TRUNCATED_SSE"),
  );
});
test("SSE enforces an event-size bound", async () => {
  await assert.rejects(
    Array.fromAsync(
      readOpenCodeSse(new Response("data: " + "x".repeat(100)).body, 30),
    ),
    code("EVENT_LIMIT"),
  );
});
test("requires a server-derived scope and safe URL", () => {
  assert.throws(
    () => createOpenCodeBridge({ baseUrl: "file:///etc/passwd", scope: "u" }),
    TypeError,
  );
  assert.throws(
    () =>
      createOpenCodeBridge({
        baseUrl: "https://user:pw@example.org",
        scope: "u",
      }),
    TypeError,
  );
  assert.throws(
    () => createOpenCodeBridge({ baseUrl: "http://localhost:4096", scope: "" }),
    TypeError,
  );
  assert.throws(
    () =>
      createOpenCodeBridge({
        baseUrl: "http://localhost:4096",
        scope: "u",
        runTimeoutMs: 0,
      }),
    TypeError,
  );
});
test("real HTTP fixture streams an assistant message with matching lifecycle", async (t) => {
  const f = await fixture(t);
  const events = await collect(
    createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" }),
  );
  assert.equal(transcript(events), "Hello 🌍");
  assert.equal(events.filter((e) => e.type === "TEXT_MESSAGE_START").length, 1);
  assert.equal(events.filter((e) => e.type === "TEXT_MESSAGE_END").length, 1);
  assert.ok(
    !events.some((e) => e.type.startsWith("RUN_")),
    "BuiltInAgent owns run lifecycle",
  );
});
test("never renders user echoes, other sessions or other prompts", async (t) => {
  const f = await fixture(t, {
    onPrompt({ s, emit, part, finish }) {
      emit({
        type: "message.updated",
        properties: { info: { id: "user", sessionID: s.id, role: "user" } },
      });
      emit(part(s, { type: "text", messageID: "user", text: "USER_ECHO" }));
      emit({
        type: "message.updated",
        properties: {
          info: {
            id: "other",
            sessionID: s.id,
            role: "assistant",
            parentID: "other-prompt",
          },
        },
      });
      emit(part(s, { type: "text", messageID: "other", text: "OTHER_PROMPT" }));
      emit(
        part(s, { type: "text", sessionID: "different", text: "OTHER_USER" }),
      );
      finish(s, "correct");
    },
  });
  assert.equal(
    transcript(
      await collect(createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" })),
    ),
    "correct",
  );
});
test("preserves snapshot/delta text without duplication", async (t) => {
  const f = await fixture(t, {
    onPrompt({ s, emit, info, part, idle }) {
      emit(info(s));
      emit(part(s, { type: "text", text: "A" }));
      emit({
        type: "message.part.delta",
        properties: {
          sessionID: s.id,
          messageID: s.assistant,
          partID: "text-" + s.id,
          field: "text",
          delta: "B",
        },
      });
      emit(part(s, { type: "text", text: "AB" }));
      emit(part(s, { type: "text", text: "ABC" }));
      idle(s);
    },
  });
  assert.equal(
    transcript(
      await collect(createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" })),
    ),
    "ABC",
  );
});
test("part-before-message ordering does not lose text", async (t) => {
  const f = await fixture(t, {
    onPrompt({ s, emit, info, part, idle }) {
      emit(part(s, { type: "text", text: "ordered" }));
      emit(info(s));
      idle(s);
    },
  });
  assert.equal(
    transcript(
      await collect(createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" })),
    ),
    "ordered",
  );
});
test("maps native backend tool calls and results exactly once", async (t) => {
  const f = await fixture(t, {
    onPrompt({ s, emit, info, part, idle }) {
      emit(info(s));
      const tool = { type: "tool", id: "tool1", callID: "call1", tool: "read" };
      const running = {
        ...tool,
        state: { status: "running", input: { filePath: "README.md" } },
      };
      emit(part(s, running));
      emit(part(s, running));
      const done = {
        ...tool,
        state: {
          status: "completed",
          input: { filePath: "README.md" },
          output: "file content",
        },
      };
      emit(part(s, done));
      emit(part(s, done));
      idle(s);
    },
  });
  const events = await collect(
    createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" }),
  );
  assert.deepEqual(
    events.map((e) => e.type),
    ["TOOL_CALL_START", "TOOL_CALL_ARGS", "TOOL_CALL_END", "TOOL_CALL_RESULT"],
  );
  assert.equal(events[3].content, "file content");
});
test("keeps native tool failures as results for the OpenCode loop", async (t) => {
  const f = await fixture(t, {
    onPrompt({ s, emit, info, part, idle }) {
      emit(info(s));
      emit(
        part(s, {
          type: "tool",
          callID: "failed",
          tool: "read",
          state: { status: "error", input: {}, error: "missing" },
        }),
      );
      idle(s);
    },
  });
  const events = await collect(
    createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" }),
  );
  assert.deepEqual(JSON.parse(events.at(-1).content), { error: "missing" });
});
test("context, read-only state, model, agent and server auth are forwarded", async (t) => {
  const f = await fixture(t, { authorization: "Basic configured-secret" });
  const headers = { authorization: "Basic configured-secret" };
  const bridge = createOpenCodeBridge({
    baseUrl: f.baseUrl,
    scope: "u",
    directory: "/workspace",
    headers,
    model: { providerID: "test", modelID: "fixture" },
    agent: "build",
    systemPrompt: "Server prompt",
  });
  headers.authorization = "mutated";
  await collect(
    bridge,
    input({
      state: { count: 4 },
      context: [{ description: "selected", value: "file.txt" }],
      forwardedProps: {
        baseUrl: "http://evil",
        scope: "other",
        directory: "/etc",
      },
    }),
  );
  const request = f.calls.find((c) => c.path.endsWith("prompt_async"));
  assert.equal(request.directory, "/workspace");
  assert.deepEqual(request.body.model, {
    providerID: "test",
    modelID: "fixture",
  });
  assert.equal(request.body.agent, "build");
  assert.match(request.body.system, /read-only/);
  assert.match(request.body.system, /file.txt/);
  assert.equal(request.headers.authorization, "Basic configured-secret");
});
test("reuses a session and only sends the new user message on later turns", async (t) => {
  const f = await fixture(t);
  const bridge = createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" });
  await collect(bridge);
  await collect(
    bridge,
    input({
      runId: "run2",
      messages: [
        ...input().messages,
        { id: "assistant1", role: "assistant", content: "Hello" },
        { id: "user2", role: "user", content: "Next" },
      ],
    }),
  );
  assert.equal(f.calls.filter((c) => c.path === "/session").length, 1);
  const prompts = f.calls.filter((c) => c.path.endsWith("prompt_async"));
  assert.equal(prompts.length, 2);
  assert.equal(prompts[1].body.parts[0].text, "Next");
});
test("seeds imported textual history only once", async (t) => {
  const f = await fixture(t);
  const bridge = createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" });
  await collect(
    bridge,
    input({
      messages: [
        { id: "old", role: "assistant", content: "Prior answer" },
        ...input().messages,
      ],
    }),
  );
  const seed = f.calls.filter((c) => c.path.endsWith("/message"));
  assert.equal(seed.length, 1);
  assert.equal(seed[0].body.noReply, true);
  assert.match(seed[0].body.parts[0].text, /Prior answer/);
});
test("rejects duplicate user IDs rather than repeating side effects", async (t) => {
  const f = await fixture(t);
  const bridge = createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" });
  await collect(bridge);
  await assert.rejects(
    collect(bridge, input({ runId: "retry" })),
    code("DUPLICATE_TURN"),
  );
  assert.equal(
    f.calls.filter((c) => c.path.endsWith("prompt_async")).length,
    1,
  );
});
test("parallel scopes cannot share a session even with identical thread IDs", async (t) => {
  const f = await fixture(t);
  const store = new OpenCodeSessionStore();
  const a = createOpenCodeBridge({
    baseUrl: f.baseUrl,
    scope: "tenant-A",
    sessions: store,
  });
  const b = createOpenCodeBridge({
    baseUrl: f.baseUrl,
    scope: "tenant-B",
    sessions: store,
  });
  const results = await Promise.all([collect(a), collect(b)]);
  assert.equal(f.sessions.size, 2);
  assert.deepEqual(results.map(transcript), ["Hello 🌍", "Hello 🌍"]);
});
test("parallel threads are supported; overlapping turns on one thread are rejected", async (t) => {
  const f = await fixture(t);
  const bridge = createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" });
  const first = collect(bridge);
  await assert.rejects(collect(bridge), code("THREAD_BUSY"));
  const others = [1, 2, 3, 4].map((n) =>
    collect(bridge, input({ threadId: "parallel" + n })),
  );
  assert.equal((await first).length > 0, true);
  assert.equal((await Promise.all(others)).length, 4);
});
test("aborting the consumer invokes OpenCode session.abort", async (t) => {
  const f = await fixture(t, {
    onPrompt({ s, emit, info, part }) {
      emit(info(s));
      emit(part(s, { type: "text", text: "working" }));
    },
  });
  const bridge = createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" });
  const controller = new AbortController();
  const events = bridge.stream({
    input: input(),
    abortSignal: controller.signal,
  });
  await events.next();
  controller.abort(new Error("user cancelled"));
  await assert.rejects(Array.fromAsync(events), /user cancelled/);
  assert.equal(f.calls.filter((c) => c.path.endsWith("/abort")).length, 1);
});
test("early iterator return also cancels the upstream agent", async (t) => {
  const f = await fixture(t, {
    onPrompt({ s, emit, info, part }) {
      emit(info(s));
      emit(part(s, { type: "text", text: "working" }));
    },
  });
  const bridge = createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" });
  const stream = bridge.stream({
    input: input(),
    abortSignal: new AbortController().signal,
  });
  await stream.next();
  await stream.return();
  assert.equal(f.calls.filter((c) => c.path.endsWith("/abort")).length, 1);
});
test("run timeout cancels OpenCode rather than merely closing SSE", async (t) => {
  const f = await fixture(t, { onPrompt() {} });
  await assert.rejects(
    collect(
      createOpenCodeBridge({
        baseUrl: f.baseUrl,
        scope: "u",
        runTimeoutMs: 100,
      }),
    ),
    code("RUN_TIMEOUT"),
  );
  assert.ok(f.calls.some((c) => c.path.endsWith("/abort")));
});
test("handshake timeout prevents submitting a prompt", async (t) => {
  const f = await fixture(t, { noHandshake: true });
  await assert.rejects(
    collect(
      createOpenCodeBridge({
        baseUrl: f.baseUrl,
        scope: "u",
        requestTimeoutMs: 75,
      }),
    ),
    code("CONNECT_TIMEOUT"),
  );
  assert.equal(
    f.calls.filter((c) => c.path.endsWith("prompt_async")).length,
    0,
  );
});
test("SSE disconnect is an error, not successful completion", async (t) => {
  const f = await fixture(t, {
    onPrompt({ s, emit, info, streams }) {
      emit(info(s));
      for (const stream of streams) stream.end();
    },
  });
  await assert.rejects(
    collect(createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" })),
    code("STREAM_DISCONNECTED"),
  );
});
test("upstream errors do not reflect private response bodies", async (t) => {
  const f = await fixture(t, { authorization: "required" });
  await assert.rejects(
    collect(createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" })),
    (error) =>
      error.code === "UPSTREAM_HTTP" &&
      !error.message.includes("secret-upstream"),
  );
});
test("provider error event aborts and reports failure", async (t) => {
  const f = await fixture(t, {
    onPrompt({ s, emit }) {
      emit({
        type: "session.error",
        properties: {
          sessionID: s.id,
          error: { name: "ProviderAuthError", data: { message: "SECRET" } },
        },
      });
    },
  });
  await assert.rejects(
    collect(createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" })),
    (error) =>
      error.code === "UPSTREAM_AGENT_ERROR" &&
      !error.message.includes("SECRET"),
  );
});
test("frontend tools and attachments fail explicitly before upstream execution", async (t) => {
  const f = await fixture(t);
  const bridge = createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" });
  await assert.rejects(
    collect(bridge, input({ tools: [{ name: "editBrowser" }] })),
    code("FRONTEND_TOOLS_UNSUPPORTED"),
  );
  await assert.rejects(
    collect(
      bridge,
      input({
        messages: [
          {
            id: "image",
            role: "user",
            content: [{ type: "image", url: "file:///secret" }],
          },
        ],
      }),
    ),
    code("UNSUPPORTED_ATTACHMENT"),
  );
  assert.equal(f.calls.length, 0);
});
test("input and session limits are enforced", async (t) => {
  const f = await fixture(t);
  const bridge = createOpenCodeBridge({
    baseUrl: f.baseUrl,
    scope: "u",
    sessions: new OpenCodeSessionStore(1),
  });
  await collect(bridge);
  await assert.rejects(
    collect(bridge, input({ threadId: "overflow" })),
    code("SESSION_LIMIT"),
  );
  await assert.rejects(
    collect(
      createOpenCodeBridge({
        baseUrl: f.baseUrl,
        scope: "u",
        maxInputBytes: 10,
      }),
    ),
    code("INPUT_LIMIT"),
  );
});

function permissionFixture() {
  return {
    onPrompt({ s, emit, info, part }) {
      emit(info(s));
      emit(part(s, { type: "text", text: "Before" }));
      emit(
        part(s, {
          id: "tool1",
          type: "tool",
          callID: "bash1",
          tool: "bash",
          state: { status: "running", input: { command: "echo hello" } },
        }),
      );
      emit({
        type: "permission.asked",
        properties: {
          id: "per1",
          sessionID: s.id,
          permission: "bash",
          patterns: ["echo hello"],
          metadata: {},
        },
      });
    },
    onReply({ sessions, emit, part, idle }) {
      const s = [...sessions.values()][0];
      emit(
        part(s, {
          id: "tool1",
          type: "tool",
          callID: "bash1",
          tool: "bash",
          state: {
            status: "completed",
            input: { command: "echo hello" },
            output: "hello",
          },
        }),
      );
      emit(part(s, { type: "text", text: "BeforeAfter" }));
      idle(s);
    },
  };
}
test("native permission pauses without abort; explicit resume reuses session and original tool ID", async (t) => {
  const f = await fixture(t, permissionFixture());
  const bridge = createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" });
  const initial = [];
  await assert.rejects(
    (async () => {
      for await (const e of bridge.stream({
        input: input(),
        abortSignal: new AbortController().signal,
      }))
        initial.push(e);
    })(),
    (error) =>
      error instanceof OpenCodeInterrupt && error.pending.id === "per1",
  );
  assert.equal(f.calls.filter((c) => c.path.endsWith("/abort")).length, 0);
  const resumed = await collect(
    bridge,
    input({
      runId: "run2",
      resume: [
        {
          interruptId: "per1",
          status: "resolved",
          payload: { approved: true },
        },
      ],
    }),
  );
  assert.equal(transcript(initial), "Before");
  assert.equal(transcript(resumed), "After");
  assert.equal(
    resumed.find((e) => e.type === "TOOL_CALL_RESULT").toolCallId,
    initial.find((e) => e.type === "TOOL_CALL_START").toolCallId,
  );
  assert.equal(
    f.calls.find((c) => c.path === "/permission/per1/reply").body.reply,
    "once",
  );
  assert.equal(
    f.calls.filter((c) => c.path.endsWith("prompt_async")).length,
    1,
  );
});
test("permission cancellation cannot become an approval", async (t) => {
  const f = await fixture(t, permissionFixture());
  const bridge = createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" });
  await assert.rejects(collect(bridge), OpenCodeInterrupt);
  await collect(
    bridge,
    input({
      runId: "run2",
      resume: [
        {
          interruptId: "per1",
          status: "cancelled",
          payload: { approved: true },
        },
      ],
    }),
  );
  assert.equal(
    f.calls.find((c) => c.path === "/permission/per1/reply").body.reply,
    "reject",
  );
});
test("unrelated resume IDs cannot consume or cancel a pending approval", async (t) => {
  const f = await fixture(t, permissionFixture());
  const bridge = createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" });
  await assert.rejects(collect(bridge), OpenCodeInterrupt);
  await assert.rejects(
    collect(
      bridge,
      input({
        resume: [
          {
            interruptId: "someone-else",
            status: "resolved",
            payload: { approved: true },
          },
        ],
      }),
    ),
    code("UNKNOWN_INTERRUPT"),
  );
  assert.ok(
    !f.calls.some(
      (c) => c.path.startsWith("/permission") || c.path.endsWith("/abort"),
    ),
  );
  await assert.rejects(collect(bridge), OpenCodeInterrupt);
});
test("native question maps to a pending interrupt and resumes with structured answers", async (t) => {
  const f = await fixture(t, {
    onPrompt({ s, emit, info }) {
      emit(info(s));
      emit({
        type: "question.asked",
        properties: {
          sessionID: s.id,
          id: "q1",
          questions: [{ question: "Pick a branch" }],
        },
      });
    },
    onReply({ sessions, finish }) {
      finish([...sessions.values()][0], "answered");
    },
  });
  const bridge = createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" });
  await assert.rejects(
    collect(bridge),
    (error) =>
      error instanceof OpenCodeInterrupt && error.pending.kind === "question",
  );
  await assert.rejects(
    collect(
      bridge,
      input({
        resume: [
          {
            interruptId: "q1",
            status: "resolved",
            payload: { answers: "bad" },
          },
        ],
      }),
    ),
    code("INVALID_RESUME"),
  );
  const events = await collect(
    bridge,
    input({
      runId: "r2",
      resume: [
        {
          interruptId: "q1",
          status: "resolved",
          payload: { answers: [["main"]] },
        },
      ],
    }),
  );
  assert.equal(transcript(events), "answered");
  assert.deepEqual(f.calls.find((c) => c.path === "/question/q1/reply").body, {
    answers: [["main"]],
  });
});
test("disposing a thread removes its upstream session and allows a new one", async (t) => {
  const f = await fixture(t);
  const bridge = createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" });
  await collect(bridge);
  await bridge.disposeThread("thread1");
  await collect(bridge);
  assert.equal(f.calls.filter((c) => c.path === "/session").length, 2);
  assert.equal(f.calls.filter((c) => c.method === "DELETE").length, 1);
});
test("disposing an active thread is rejected", async (t) => {
  const f = await fixture(t);
  const bridge = createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" });
  const run = collect(bridge);
  await assert.rejects(bridge.disposeThread("thread1"), code("THREAD_BUSY"));
  await run;
});
test("non-append text and late text fail instead of corrupting the chat transcript", () => {
  const tr = new OpenCodeEventTranslator("s", "p", "r");
  const info = {
    type: "message.updated",
    properties: {
      info: { id: "a", sessionID: "s", parentID: "p", role: "assistant" },
    },
  };
  tr.translate(info);
  tr.translate(translatorTextPart("old"));
  assert.throws(
    () => tr.translate(translatorTextPart("rewritten")),
    code("NON_APPEND_TEXT"),
  );
  tr.finish();
  assert.throws(
    () => tr.translate(translatorTextPart("old plus")),
    code("LATE_TEXT"),
  );
});

test("failed validation does not leak capacity in the bounded store", async (t) => {
  const f = await fixture(t);
  const bridge = createOpenCodeBridge({
    baseUrl: f.baseUrl,
    scope: "u",
    sessions: new OpenCodeSessionStore(1),
  });
  await assert.rejects(
    collect(bridge, input({ threadId: "bad", messages: [] })),
    code("INVALID_INPUT"),
  );
  assert.equal(
    transcript(await collect(bridge, input({ threadId: "good" }))),
    "Hello 🌍",
  );
});
test("a false abort acknowledgement fails closed instead of reusing the session", async (t) => {
  const f = await fixture(t, { abortReply: false, onPrompt() {} });
  const bridge = createOpenCodeBridge({
    baseUrl: f.baseUrl,
    scope: "u",
    runTimeoutMs: 40,
  });
  await assert.rejects(collect(bridge), code("CANCEL_FAILED"));
  await assert.rejects(
    collect(bridge, input({ runId: "r2" })),
    code("CANCEL_FAILED"),
  );
  assert.equal(f.calls.filter((c) => c.path === "/session").length, 2);
});
test("error names are not reflected from upstream into the browser", async (t) => {
  const f = await fixture(t, {
    onPrompt({ s, emit, info }) {
      emit(
        info(s, {
          error: {
            name: "credential-secret",
            data: { message: "secret-content" },
          },
        }),
      );
    },
  });
  await assert.rejects(
    collect(createOpenCodeBridge({ baseUrl: f.baseUrl, scope: "u" })),
    (error) => {
      assert.equal(error.code, "UPSTREAM_AGENT_ERROR");
      assert.ok(!error.message.includes("secret"));
      return true;
    },
  );
});
