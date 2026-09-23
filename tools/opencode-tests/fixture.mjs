import { createServer } from "node:http";

const info = (s, extras = {}) => ({
  type: "message.updated",
  properties: {
    info: {
      id: s.assistant,
      sessionID: s.id,
      role: "assistant",
      parentID: s.prompt.messageID,
      ...extras,
    },
  },
});

const part = (s, data) => ({
  type: "message.part.updated",
  properties: {
    part: {
      id: "text-" + s.id,
      sessionID: s.id,
      messageID: s.assistant,
      ...data,
    },
  },
});

/** HTTP/SSE contract fixture, NOT an OpenCode process or an LLM. */
export async function startFixture(options = {}) {
  const calls = [];
  const sessions = new Map();
  const streams = new Set();
  let sequence = 0;
  const emit = (event) => {
    const frame = `data: ${JSON.stringify(event)}\n\n`;
    for (const stream of streams) if (!stream.destroyed) stream.write(frame);
  };
  const idle = (s) =>
    emit({
      type: "session.status",
      properties: { sessionID: s.id, status: { type: "idle" } },
    });
  const finish = (s, answer = "Hello 🌍") => {
    emit(info(s));
    emit(part(s, { type: "text", text: "" }));
    emit(part(s, { type: "text", text: answer.slice(0, 3) }));
    emit(part(s, { type: "text", text: answer }));
    emit(info(s, { time: { completed: Date.now() } }));
    idle(s);
  };
  const server = createServer(async (req, res) => {
    try {
      let raw = "";
      for await (const chunk of req) raw += chunk;
      const body = raw ? JSON.parse(raw) : undefined;
      const url = new URL(req.url, "http://fixture");
      const path = url.pathname;
      calls.push({
        path,
        method: req.method,
        body,
        headers: req.headers,
        directory: url.searchParams.get("directory"),
      });
      if (
        options.authorization &&
        req.headers.authorization !== options.authorization
      ) {
        res.writeHead(401).end("secret-upstream-error-body");
        return;
      }
      if (path === "/event") {
        res.writeHead(200, {
          "content-type": "text/event-stream",
          "cache-control": "no-cache",
        });
        res.flushHeaders();
        streams.add(res);
        req.on("close", () => streams.delete(res));
        res.on("close", () => streams.delete(res));
        if (!options.noHandshake)
          res.write('data: {"type":"server.connected","properties":{}}\n\n');
        return;
      }
      const send = (value) => {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify(value));
      };
      if (path === "/session" && req.method === "POST") {
        const id = "ses_" + ++sequence;
        const s = { id, assistant: "assistant-" + id };
        sessions.set(id, s);
        send({ id });
        return;
      }
      const match = path.match(/^\/session\/([^/]+)(?:\/(.*))?$/);
      if (match) {
        const s = sessions.get(match[1]);
        if (!s) {
          res.writeHead(404).end();
          return;
        }
        if (req.method === "DELETE") {
          sessions.delete(s.id);
          send(true);
          return;
        }
        if (match[2] === "message") {
          send({ info: { role: "user", id: "history" }, parts: [] });
          return;
        }
        if (match[2] === "abort") {
          s.aborted = options.abortReply !== false;
          send(options.abortReply ?? true);
          return;
        }
        if (match[2] === "prompt_async") {
          s.prompt = body;
          res.writeHead(options.promptStatus ?? 204).end();
          if (!options.promptStatus) {
            setTimeout(() => {
              if (options.onPrompt)
                options.onPrompt({
                  s,
                  emit,
                  info,
                  part,
                  idle,
                  finish,
                  streams,
                });
              else finish(s);
            }, 5);
          }
          return;
        }
      }
      if (/^\/(permission|question)\//.test(path)) {
        send(true);
        setTimeout(
          () =>
            options.onReply?.({
              path,
              body,
              sessions,
              emit,
              info,
              part,
              idle,
              finish,
            }),
          5,
        );
        return;
      }
      res.writeHead(404).end();
    } catch (error) {
      res.writeHead(500).end(error.message);
    }
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    baseUrl: `http://127.0.0.1:${server.address().port}`,
    calls,
    sessions,
    streams,
    emit,
    async close() {
      for (const stream of streams) stream.end();
      // Stop accepting connections before destroying sockets, including any
      // connection that fetch may have opened while an earlier one was aborted.
      const closed = new Promise((resolve) => server.close(resolve));
      server.closeAllConnections();
      await closed;
    },
  };
}
