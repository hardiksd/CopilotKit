/**
 * OpenCode HTTP -> AG-UI bridge. Intentionally uses only Web APIs so its
 * protocol and cancellation behavior can be tested without a model or SDK.
 * OpenCode owns the agentic loop; this module never invokes a model itself.
 */
export interface OpenCodeInput {
  threadId: string;
  runId: string;
  messages: Array<{ id: string; role: string; content?: unknown }>;
  tools?: unknown[];
  state?: unknown;
  context?: Array<{ description: string; value: string }>;
  resume?: Array<{ interruptId: string; status?: string; payload?: unknown }>;
}

export interface OpenCodeEvent {
  type: string;
  [key: string]: unknown;
}

type ObjectValue = Record<string, unknown>;
type PendingInput = {
  kind: "permission" | "question";
  id: string;
  payload: ObjectValue;
};
type Session = {
  id?: string;
  busy: boolean;
  consumed: Set<string>;
  pending?: PendingInput;
  promptId?: string;
  checkpoint?: TranslatorCheckpoint;
};

export class OpenCodeBridgeError extends Error {
  constructor(
    public readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "OpenCodeBridgeError";
  }
}

/** Caught by OpenCodeAgent and converted to ctx.interrupt(), not RUN_ERROR. */
export class OpenCodeInterrupt extends Error {
  constructor(public readonly pending: PendingInput) {
    super(`OpenCode requires ${pending.kind} input`);
    this.name = "OpenCodeInterrupt";
  }
}

/**
 * Process-local session ownership AND run locks. Share this object across
 * request-specific agents/clones. It is not distributed storage or auth.
 * No automatic eviction: silently losing an active/paused coding session is unsafe.
 */
export class OpenCodeSessionStore {
  private readonly sessions = new Map<string, Session>();
  constructor(private readonly maxSessions = 256) {
    positiveInteger(maxSessions, "maxSessions");
  }

  acquire(key: string): Session {
    let session = this.sessions.get(key);
    if (session?.busy)
      fail("THREAD_BUSY", "This OpenCode thread is already running.");
    if (!session) {
      if (this.sessions.size >= this.maxSessions) {
        fail(
          "SESSION_LIMIT",
          "OpenCode session capacity reached; dispose unused sessions.",
        );
      }
      session = { busy: false, consumed: new Set() };
      this.sessions.set(key, session);
    }
    session.busy = true;
    return session;
  }

  release(key: string): void {
    const session = this.sessions.get(key);
    if (session) session.busy = false;
  }

  get(key: string): Session | undefined {
    return this.sessions.get(key);
  }
  delete(key: string): void {
    this.sessions.delete(key);
  }
}

export interface OpenCodeOptions {
  /** Trusted server configuration. Never derive from forwardedProps or tool arguments. */
  baseUrl: string;
  /** Authenticated tenant/user namespace, supplied by your server, never the browser. */
  scope: string;
  directory?: string;
  model?: { providerID: string; modelID: string };
  agent?: string;
  headers?: Record<string, string>;
  systemPrompt?: string;
  sessions?: OpenCodeSessionStore;
  fetch?: typeof globalThis.fetch;
  requestTimeoutMs?: number;
  runTimeoutMs?: number;
  maxEventBytes?: number;
  maxRunBytes?: number;
  maxInputBytes?: number;
}

function fail(code: string, message: string): never {
  throw new OpenCodeBridgeError(code, message);
}
function object(value: unknown): ObjectValue {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as ObjectValue)
    : {};
}
function text(value: unknown): string {
  return typeof value === "string" ? value : "";
}
function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0)
    throw new TypeError(`${name} must be a positive integer`);
  return value;
}
function bytes(value: string): number {
  return new TextEncoder().encode(value).length;
}
function messageText(content: unknown): string {
  if (content === undefined) return "";
  if (typeof content === "string") return content;
  if (
    Array.isArray(content) &&
    content.every(
      (p) => object(p).type === "text" && typeof object(p).text === "string",
    )
  ) {
    return content.map((p) => object(p).text).join("\n");
  }
  return fail(
    "UNSUPPORTED_ATTACHMENT",
    "This OpenCode adapter supports text only; attachments are not silently discarded.",
  );
}

/** Bounded SSE decoder, including split UTF-8, CRLF and multi-line data fields. */
export async function* readOpenCodeSse(
  body: ReadableStream<Uint8Array>,
  maxEventBytes = 1024 * 1024,
): AsyncGenerator<unknown> {
  const reader = body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let buffer = "";
  let data: string[] = [];
  let eventBytes = 0;
  try {
    while (true) {
      const { value, done } = await reader.read();
      buffer += done
        ? decoder.decode()
        : decoder.decode(value, { stream: true });
      let newline: number;
      while ((newline = buffer.indexOf("\n")) >= 0) {
        let line = buffer.slice(0, newline);
        buffer = buffer.slice(newline + 1);
        if (line.endsWith("\r")) line = line.slice(0, -1);
        if (line === "") {
          if (data.length) {
            const payload = data.join("\n");
            data = [];
            eventBytes = 0;
            try {
              yield JSON.parse(payload);
            } catch (error) {
              if (error instanceof SyntaxError)
                fail(
                  "INVALID_SSE",
                  "OpenCode sent invalid JSON in its event stream.",
                );
              throw error;
            }
          }
        } else if (line.startsWith("data:")) {
          const part = line.slice(5).replace(/^ /, "");
          eventBytes += bytes(part) + 1;
          if (eventBytes > maxEventBytes)
            fail("EVENT_LIMIT", "OpenCode event exceeds the configured limit.");
          data.push(part);
        }
      }
      if (bytes(buffer) + eventBytes > maxEventBytes)
        fail("EVENT_LIMIT", "OpenCode event exceeds the configured limit.");
      if (done) {
        // A partial final frame is not a successful end of an agent run.
        if (buffer.trim() || data.length)
          fail("TRUNCATED_SSE", "OpenCode closed an incomplete event frame.");
        return;
      }
    }
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

interface TranslatorCheckpoint {
  assistants: string[];
  parts: Array<[string, { message: string; kind: string; value: string }]>;
  calls: Array<[string, { done: boolean; id: string }]>;
}

/** Translates only messages belonging to the current session AND prompt. */
export class OpenCodeEventTranslator {
  private readonly assistants = new Set<string>();
  private readonly messages = new Set<string>();
  private readonly endedMessages = new Set<string>();
  private readonly parts = new Map<
    string,
    { message: string; kind: string; value: string }
  >();
  private readonly calls = new Map<string, { done: boolean; id: string }>();
  private readonly deferred: ObjectValue[] = [];
  seenAssistant = false;

  constructor(
    private readonly sessionId: string,
    private readonly promptId: string,
    private readonly runId: string,
    checkpoint?: TranslatorCheckpoint,
  ) {
    for (const id of checkpoint?.assistants ?? []) this.assistants.add(id);
    for (const [id, part] of checkpoint?.parts ?? [])
      this.parts.set(id, { ...part });
    for (const [id, call] of checkpoint?.calls ?? [])
      this.calls.set(id, { ...call });
    this.seenAssistant = this.assistants.size > 0;
  }

  checkpoint(): TranslatorCheckpoint {
    return {
      assistants: [...this.assistants],
      parts: [...this.parts].map(([id, part]) => [id, { ...part }]),
      calls: [...this.calls].map(([id, call]) => [id, { ...call }]),
    };
  }

  translate(raw: unknown): OpenCodeEvent[] {
    const event = object(raw);
    const properties = object(event.properties);
    const info = object(properties.info);
    const part = object(properties.part);
    const sessionId =
      text(properties.sessionID) ||
      text(info.sessionID) ||
      text(part.sessionID);
    if (sessionId !== this.sessionId) return [];

    if (event.type === "message.updated") {
      if (info.role !== "assistant" || info.parentID !== this.promptId)
        return [];
      const id = text(info.id);
      if (!id) fail("INVALID_MESSAGE", "OpenCode assistant message has no ID.");
      if (info.error)
        fail(
          "UPSTREAM_AGENT_ERROR",
          "OpenCode reported an agent error; inspect protected server logs for details.",
        );
      this.seenAssistant = true;
      this.assistants.add(id);
      const out: OpenCodeEvent[] = [];
      for (let i = 0; i < this.deferred.length; ) {
        const candidate = this.deferred[i];
        const props = object(candidate.properties);
        if (
          (text(object(props.part).messageID) || text(props.messageID)) !== id
        ) {
          i++;
          continue;
        }
        this.deferred.splice(i, 1);
        out.push(...this.translate(candidate));
      }
      if (
        object(info.time).completed &&
        this.messages.has(id) &&
        !this.endedMessages.has(id)
      ) {
        this.endedMessages.add(id);
        out.push({ type: "TEXT_MESSAGE_END", messageId: this.messageId(id) });
      }
      return out;
    }

    if (
      event.type !== "message.part.updated" &&
      event.type !== "message.part.delta"
    )
      return [];
    const messageId = text(part.messageID) || text(properties.messageID);
    if (!this.assistants.has(messageId)) {
      // User echo, other prompt, or part-before-message ordering. Bounded, never displayed until proven assistant-owned.
      if (this.deferred.length >= 512)
        fail("EVENT_LIMIT", "Too many unassociated OpenCode message parts.");
      this.deferred.push(event);
      return [];
    }
    const out: OpenCodeEvent[] = [];
    const partId = text(part.id) || text(properties.partID);
    if (!partId) fail("INVALID_PART", "OpenCode message part has no ID.");

    if (event.type === "message.part.delta") {
      const existing = this.parts.get(partId);
      if (!existing || existing.kind !== "text" || properties.field !== "text")
        return [];
      const delta = text(properties.delta);
      existing.value += delta;
      return this.textEvents(messageId, delta);
    }
    if (part.type === "text") {
      if (part.ignored || part.synthetic) return [];
      const previous = this.parts.get(partId)?.value ?? "";
      const current = text(part.text);
      if (!current.startsWith(previous))
        fail(
          "NON_APPEND_TEXT",
          "OpenCode rewrote streamed text; a delta cannot represent this safely.",
        );
      this.parts.set(partId, {
        message: messageId,
        kind: "text",
        value: current,
      });
      return this.textEvents(messageId, current.slice(previous.length));
    }
    if (part.type !== "tool") return []; // Deliberately do not expose provider reasoning.
    const state = object(part.state);
    if (state.status === "pending") return [];
    if (!["running", "completed", "error"].includes(text(state.status)))
      return [];
    const callId = text(part.callID);
    const toolName = text(part.tool);
    if (!callId || !toolName)
      fail(
        "INVALID_TOOL",
        "OpenCode tool event is missing its call ID or name.",
      );
    let call = this.calls.get(callId);
    const toolCallId = call?.id ?? `${this.runId}:${callId}`;
    if (!call) {
      call = { done: false, id: toolCallId };
      this.calls.set(callId, call);
      out.push(
        { type: "TOOL_CALL_START", toolCallId, toolCallName: toolName },
        {
          type: "TOOL_CALL_ARGS",
          toolCallId,
          delta: JSON.stringify(object(state.input)),
        },
        { type: "TOOL_CALL_END", toolCallId },
      );
    }
    if (
      !call.done &&
      (state.status === "completed" || state.status === "error")
    ) {
      call.done = true;
      out.push({
        type: "TOOL_CALL_RESULT",
        toolCallId,
        role: "tool",
        messageId: `${toolCallId}:result`,
        content:
          state.status === "error"
            ? JSON.stringify({ error: text(state.error) })
            : text(state.output),
      });
    }
    return out;
  }

  finish(): OpenCodeEvent[] {
    const out: OpenCodeEvent[] = [];
    for (const id of this.messages) {
      if (!this.endedMessages.has(id)) {
        this.endedMessages.add(id);
        out.push({ type: "TEXT_MESSAGE_END", messageId: this.messageId(id) });
      }
    }
    return out;
  }
  private messageId(id: string): string {
    return `${this.runId}:${id}`;
  }
  private textEvents(id: string, delta: string): OpenCodeEvent[] {
    if (!delta) return [];
    if (this.endedMessages.has(id))
      fail("LATE_TEXT", "OpenCode streamed text after closing its message.");
    const out: OpenCodeEvent[] = [];
    if (!this.messages.has(id)) {
      this.messages.add(id);
      out.push({
        type: "TEXT_MESSAGE_START",
        role: "assistant",
        messageId: this.messageId(id),
      });
    }
    out.push({
      type: "TEXT_MESSAGE_CONTENT",
      messageId: this.messageId(id),
      delta,
    });
    return out;
  }
}

export function createOpenCodeBridge(options: OpenCodeOptions) {
  const base = new URL(options.baseUrl);
  if (
    !["http:", "https:"].includes(base.protocol) ||
    base.username ||
    base.password ||
    base.search ||
    base.hash
  ) {
    throw new TypeError(
      "baseUrl must be an HTTP(S) URL without credentials, query or fragment",
    );
  }
  if (!options.scope?.trim())
    throw new TypeError("A trusted, non-empty scope is required");
  if (options.model && (!options.model.providerID || !options.model.modelID))
    throw new TypeError("model needs providerID and modelID");
  if (!base.pathname.endsWith("/")) base.pathname += "/";
  // Capture configuration so per-request callers cannot mutate another request's headers or routing.
  const config = {
    ...options,
    model: options.model ? { ...options.model } : undefined,
  };
  const headers = new Headers(options.headers);
  const fetcher = options.fetch ?? globalThis.fetch;
  const sessions = options.sessions ?? new OpenCodeSessionStore();
  const requestTimeout = positiveInteger(
    options.requestTimeoutMs ?? 15_000,
    "requestTimeoutMs",
  );
  const runTimeout = positiveInteger(
    options.runTimeoutMs ?? 120_000,
    "runTimeoutMs",
  );
  const maxEventBytes = positiveInteger(
    options.maxEventBytes ?? 1024 * 1024,
    "maxEventBytes",
  );
  const maxRunBytes = positiveInteger(
    options.maxRunBytes ?? 16 * 1024 * 1024,
    "maxRunBytes",
  );
  const maxInputBytes = positiveInteger(
    options.maxInputBytes ?? 256 * 1024,
    "maxInputBytes",
  );
  const keyFor = (threadId: string) =>
    JSON.stringify([base.href, config.directory ?? "", config.scope, threadId]);
  const endpoint = (path: string) => {
    const url = new URL(path, base);
    if (config.directory) url.searchParams.set("directory", config.directory);
    return url;
  };
  async function request(
    path: string,
    method: string,
    body?: unknown,
    signal?: AbortSignal,
  ): Promise<Response> {
    const requestHeaders = new Headers(headers);
    requestHeaders.set("accept", "application/json");
    if (body !== undefined)
      requestHeaders.set("content-type", "application/json");
    const response = await fetcher(endpoint(path), {
      method,
      headers: requestHeaders,
      body: body === undefined ? undefined : JSON.stringify(body),
      redirect: "error",
      signal: signal
        ? AbortSignal.any([signal, AbortSignal.timeout(requestTimeout)])
        : AbortSignal.timeout(requestTimeout),
    });
    if (!response.ok) {
      await response.body?.cancel().catch(() => {});
      // Do not reflect provider credentials, workspace contents or an upstream HTML body to the browser.
      fail(
        "UPSTREAM_HTTP",
        `OpenCode request failed (HTTP ${response.status}).`,
      );
    }
    return response;
  }
  async function json(response: Response): Promise<unknown> {
    if (!response.body)
      fail("UPSTREAM_JSON", "OpenCode returned an empty JSON response.");
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let data = "";
    let length = 0;
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        length += value.length;
        if (length > maxEventBytes)
          fail("EVENT_LIMIT", "OpenCode JSON response is too large.");
        data += decoder.decode(value, { stream: true });
      }
      data += decoder.decode();
      try {
        return JSON.parse(data);
      } catch {
        return fail("UPSTREAM_JSON", "OpenCode returned invalid JSON.");
      }
    } finally {
      await reader.cancel().catch(() => {});
      reader.releaseLock();
    }
  }
  async function abortSession(id: string): Promise<void> {
    const response = await request(
      `session/${encodeURIComponent(id)}/abort`,
      "POST",
    );
    if ((await json(response)) !== true) {
      fail("CANCEL_FAILED", "OpenCode did not acknowledge cancellation.");
    }
  }

  async function* stream({
    input,
    abortSignal,
  }: {
    input: OpenCodeInput;
    abortSignal: AbortSignal;
  }): AsyncGenerator<OpenCodeEvent> {
    if (!input.threadId || !input.runId)
      fail("INVALID_INPUT", "threadId and runId are required.");
    if (input.tools?.length)
      fail(
        "FRONTEND_TOOLS_UNSUPPORTED",
        "OpenCode frontend-tool bridging is not implemented; use a separate agent for frontend actions rather than silently losing them.",
      );
    if (!Array.isArray(input.messages))
      fail("INVALID_INPUT", "messages must be an array.");
    if (bytes(JSON.stringify(input)) > maxInputBytes)
      fail("INPUT_LIMIT", "Input exceeds the OpenCode adapter limit.");
    abortSignal.throwIfAborted();
    const key = keyFor(input.threadId);
    const session = sessions.acquire(key);
    const controller = new AbortController();
    const onAbort = () => controller.abort(abortSignal.reason);
    abortSignal.addEventListener("abort", onAbort, { once: true });
    const timeout = setTimeout(
      () =>
        controller.abort(
          new OpenCodeBridgeError("RUN_TIMEOUT", "OpenCode run timed out."),
        ),
      runTimeout,
    );
    let paused = Boolean(session.pending);
    let finished = false;
    let streamResponse: Response | undefined;
    let iterator: AsyncGenerator<unknown> | undefined;
    try {
      controller.signal.throwIfAborted();
      const latest = input.messages.at(-1);
      const pending = session.pending;
      if (!pending) {
        if (input.resume?.length)
          fail(
            "UNKNOWN_INTERRUPT",
            "No pending OpenCode interrupt matches this resume.",
          );
        if (!latest || latest.role !== "user" || !latest.id)
          fail(
            "INVALID_INPUT",
            "A new OpenCode turn must end with a user message with an ID.",
          );
        if (session.consumed.has(latest.id))
          fail(
            "DUPLICATE_TURN",
            "This user message was already submitted; it will not be executed again.",
          );
        if (session.consumed.size >= 1000)
          fail("TURN_LIMIT", "Start a new thread after 1,000 submitted turns.");
        messageText(latest.content); // Fail before creating a session for unsupported content.
      } else {
        const resume = input.resume?.find((r) => r.interruptId === pending.id);
        if (!resume) {
          if (input.resume?.length)
            fail(
              "UNKNOWN_INTERRUPT",
              "Resume ID does not match the pending OpenCode interrupt.",
            );
          throw new OpenCodeInterrupt(pending);
        }
        if ((input.resume?.length ?? 0) !== 1)
          fail(
            "INVALID_RESUME",
            "Resume exactly one pending OpenCode interrupt at a time.",
          );
      }
      const newSession = !session.id;
      if (newSession) {
        const history = input.messages
          .slice(0, -1)
          .map((m) => ({ role: m.role, content: messageText(m.content) }));
        const created = object(
          await json(
            await request(
              "session",
              "POST",
              { title: "CopilotKit" },
              controller.signal,
            ),
          ),
        );
        session.id = text(created.id);
        if (!session.id)
          fail("INVALID_SESSION", "OpenCode did not return a session ID.");
        // OpenCode has no general AG-UI history-import API. Seed old history once as clearly labelled context.
        if (history.length) {
          const response = await request(
            `session/${encodeURIComponent(session.id)}/message`,
            "POST",
            {
              noReply: true,
              parts: [
                {
                  type: "text",
                  text: `Previous conversation (context only):\n${JSON.stringify(history)}`,
                },
              ],
              ...(config.model ? { model: config.model } : {}),
            },
            controller.signal,
          );
          await response.body?.cancel();
        }
      }
      const id = session.id!;
      const sseHeaders = new Headers(headers);
      sseHeaders.set("accept", "text/event-stream");
      // This stream remains open for a run; a separate header deadline is removed after handshake.
      const headerTimer = setTimeout(
        () =>
          controller.abort(
            new OpenCodeBridgeError(
              "CONNECT_TIMEOUT",
              "OpenCode stream handshake timed out.",
            ),
          ),
        requestTimeout,
      );
      try {
        streamResponse = await fetcher(endpoint("event"), {
          headers: sseHeaders,
          redirect: "error",
          signal: controller.signal,
        });
        if (
          !streamResponse.ok ||
          !streamResponse.headers
            .get("content-type")
            ?.includes("text/event-stream") ||
          !streamResponse.body
        ) {
          fail(
            "UPSTREAM_STREAM",
            "OpenCode did not establish an SSE connection.",
          );
        }
        iterator = readOpenCodeSse(streamResponse.body, maxEventBytes);
        const hello = await iterator.next();
        if (hello.done || object(hello.value).type !== "server.connected")
          fail(
            "UPSTREAM_HANDSHAKE",
            "Expected OpenCode's server.connected handshake before submitting work.",
          );
      } finally {
        clearTimeout(headerTimer);
      }

      if (pending) {
        const resume = input.resume!.find((r) => r.interruptId === pending.id)!;
        const payload = object(resume.payload);
        let path: string;
        let body: unknown;
        if (pending.kind === "permission") {
          path = `permission/${encodeURIComponent(pending.id)}/reply`;
          // Only an explicit boolean can approve. Never grant a persistent/always permission.
          body = {
            reply:
              resume.status === "resolved" && payload.approved === true
                ? "once"
                : "reject",
          };
        } else if (resume.status === "cancelled") {
          path = `question/${encodeURIComponent(pending.id)}/reject`;
        } else {
          if (
            resume.status !== "resolved" ||
            !Array.isArray(payload.answers) ||
            !payload.answers.every(
              (a) => Array.isArray(a) && a.every((v) => typeof v === "string"),
            )
          ) {
            fail(
              "INVALID_RESUME",
              "Question resumes require payload.answers as string[][] or cancelled status.",
            );
          }
          path = `question/${encodeURIComponent(pending.id)}/reply`;
          body = { answers: payload.answers };
        }
        const response = await request(path, "POST", body, controller.signal);
        await response.body?.cancel();
        session.pending = undefined;
        paused = false;
      } else {
        // Mark before the network request: failures have ambiguous delivery and must not auto-repeat tools.
        session.consumed.add(latest!.id);
        session.promptId = `msg_${Date.now().toString(16)}${crypto.randomUUID().replaceAll("-", "").slice(0, 16)}`;
        const system = [
          config.systemPrompt ?? "",
          ...(input.context ?? []).map((c) => `${c.description}:\n${c.value}`),
        ];
        if (input.state !== undefined)
          system.push(
            `Application state (read-only context):\n${JSON.stringify(input.state)}`,
          );
        const response = await request(
          `session/${encodeURIComponent(id)}/prompt_async`,
          "POST",
          {
            messageID: session.promptId,
            parts: [{ type: "text", text: messageText(latest!.content) }],
            ...(config.model ? { model: config.model } : {}),
            ...(config.agent ? { agent: config.agent } : {}),
            ...(system.some(Boolean)
              ? { system: system.filter(Boolean).join("\n\n") }
              : {}),
          },
          controller.signal,
        );
        await response.body?.cancel();
      }
      const translator = new OpenCodeEventTranslator(
        id,
        session.promptId!,
        input.runId,
        pending ? session.checkpoint : undefined,
      );
      let runBytes = 0;
      while (true) {
        const next = await iterator.next();
        if (next.done)
          fail(
            "STREAM_DISCONNECTED",
            "OpenCode disconnected before the run finished; reconnect is not automatically replayed.",
          );
        runBytes += bytes(JSON.stringify(next.value));
        if (runBytes > maxRunBytes)
          fail(
            "RUN_LIMIT",
            "OpenCode event traffic exceeds the configured run limit.",
          );
        const event = object(next.value);
        const props = object(event.properties);
        if (
          event.type === "server.instance.disposed" &&
          (!config.directory || props.directory === config.directory)
        )
          fail(
            "UPSTREAM_DISPOSED",
            "OpenCode disposed its workspace instance.",
          );
        if (props.sessionID === id) {
          if (event.type === "session.error")
            fail(
              "UPSTREAM_AGENT_ERROR",
              "OpenCode reported an agent error; inspect protected server logs for details.",
            );
          if (
            event.type === "permission.asked" ||
            event.type === "question.asked"
          ) {
            const requestId = text(props.id);
            if (!requestId)
              fail("INVALID_INTERRUPT", "OpenCode interrupt has no ID.");
            session.pending = {
              kind:
                event.type === "permission.asked" ? "permission" : "question",
              id: requestId,
              payload: props,
            };
            yield* translator.finish();
            session.checkpoint = translator.checkpoint();
            paused = true;
            throw new OpenCodeInterrupt(session.pending);
          }
          if (
            (event.type === "session.status" &&
              object(props.status).type === "idle") ||
            event.type === "session.idle"
          ) {
            // A resume may finish without emitting a new assistant message (e.g. a rejected request).
            if (translator.seenAssistant || pending) {
              yield* translator.finish();
              finished = true;
              session.checkpoint = undefined;
              return;
            }
          }
        }
        yield* translator.translate(event);
      }
    } catch (error) {
      if (controller.signal.aborted) throw controller.signal.reason;
      throw error;
    } finally {
      clearTimeout(timeout);
      abortSignal.removeEventListener("abort", onAbort);
      // Closing our SSE response is not cancellation of OpenCode's separate agent process.
      controller.abort();
      await iterator?.return(undefined).catch(() => {});
      await streamResponse?.body?.cancel().catch(() => {});
      try {
        if (!finished && !paused && session.id) {
          session.pending = undefined;
          try {
            await abortSession(session.id);
          } catch {
            sessions.delete(key); // Do not reuse a session whose cancellation was not acknowledged.
            fail(
              "CANCEL_FAILED",
              "OpenCode cancellation failed; stop the upstream session before retrying this workload.",
            );
          }
        }
      } finally {
        // Validation failures must not consume the finite session capacity.
        if (!session.id) sessions.delete(key);
        sessions.release(key);
      }
    }
  }

  /** Server-side cleanup only. Do not expose without a thread-ownership check. */
  async function disposeThread(threadId: string): Promise<void> {
    const key = keyFor(threadId);
    const session = sessions.get(key);
    if (session?.busy)
      fail("THREAD_BUSY", "Cannot dispose an active OpenCode thread.");
    if (!session) return;
    session.busy = true;
    try {
      if (session.id) {
        await abortSession(session.id);
        const response = await request(
          `session/${encodeURIComponent(session.id)}`,
          "DELETE",
        );
        await response.body?.cancel();
      }
      sessions.delete(key);
    } finally {
      sessions.release(key);
    }
  }
  return { stream, disposeThread, sessions };
}
