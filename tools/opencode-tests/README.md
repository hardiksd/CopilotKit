# Experimental OpenCode runtime adapter: developer review

**Status: candidate implementation, not verified end to end.** Protocol tests can
run independently, but that does not establish compatibility with the complete
CopilotKit runtime or an actual OpenCode release. Do not publish this as a
fully supported integration until the runtime and live gates pass.

## What changes

The candidate adds `OpenCodeAgent`, `OpenCodeSessionStore`, and
`OpenCodeBridgeError` to `@copilotkit/runtime/v2`. It does not change the default
agent, remove existing provider integrations, or require the Claude Agent SDK.
No new npm dependency is added to the runtime. OpenCode is a separately managed
server. The adapter uses native fetch and translates its HTTP/SSE protocol into
AG-UI. `BuiltInAgent` custom factory mode owns the outer AG-UI lifecycle.

The base source snapshot is `hardiksd/CopilotKit` commit
`bbf420baae0019f7ccf0bc406b3454475ce2888c` (runtime version 1.73.3).
The live gate targets **OpenCode 1.18.32**; other versions are not certified.

## Supported by the candidate protocol implementation

- Text streaming, native OpenCode tool calls and results, errors and cancellation.
- OpenCode owns the model/tool loop; the bridge never invokes a model directly.
- Session reuse across turns; separate process-local sessions keyed by endpoint,
  directory, authenticated scope and CopilotKit thread ID.
- A per-thread execution lock, bounded retained session metadata, input/event/run
  limits, timeout handling, explicit disposal, and rejection of ambiguous retries.
- Native OpenCode `permission.asked` and `question.asked` events become AG-UI
  interrupts. The upstream run stays paused; a matching resume replies to it
  rather than starting the prompt again. Approval is only `once`, never `always`.
- Read-only application context/state included in the OpenCode prompt.

## Not implemented (not a full drop-in replacement)

- Frontend-defined/browser-executed tools. A non-empty `input.tools` is rejected
  with `FRONTEND_TOOLS_UNSUPPORTED` before contacting OpenCode. This also limits
  generative UI workflows built on those tools. Do not disable the guard merely
  to make chat appear to work: the tool-result round trip is missing.
- Agent-authored application-state snapshots/deltas, A2UI generation parity or
  MCP Apps parity. State is context only, not synchronized writable agent state.
- Attachment handling. Non-text content fails explicitly.
- Durable session mapping, stream reconnect/replay or process-failure recovery.
- Editing/regenerating old turns or synchronizing arbitrary client history edits.
  New turns must end with a unique user message. Imported old text history is
  seeded once as a labelled transcript, not as a native role-perfect session.
- Rendering approval/question forms automatically. Your existing interrupt UI
  must recognize the documented reasons/payloads below.
- A multi-tenant sandbox or authentication system. A scope is a namespace, NOT
  proof of identity, workspace isolation, thread ownership or authorization.

## Wiring into an existing application

After building the fork, import the new class through the v2 runtime entry point.
Keep the existing frontend provider URL and agent ID, but do not register browser
execution tools on the OpenCode agent until the two-way bridge is implemented.

```ts
import { OpenCodeAgent, OpenCodeSessionStore } from "@copilotkit/runtime/v2";

// One shared store per runtime process, outside a request-specific factory.
const sessions = new OpenCodeSessionStore(256);

// Call only AFTER server-side authentication and thread-ownership validation.
function agentForAuthenticatedUser(user: { tenantId: string; id: string }) {
  const baseUrl = process.env.OPENCODE_SERVER_URL;
  const password = process.env.OPENCODE_SERVER_PASSWORD;
  if (!baseUrl || !password)
    throw new Error("OpenCode server is not configured");

  return new OpenCodeAgent({
    baseUrl,
    scope: JSON.stringify([user.tenantId, user.id]),
    sessions,
    headers: {
      authorization: `Basic ${Buffer.from(`opencode:${password}`).toString("base64")}`,
    },
    // Set a model and a workspace using trusted server-side provisioning.
    // Never use forwardedProps to select endpoint, directory or credentials.
    runTimeoutMs: 120_000,
  });
}
```

Register the returned agent under the existing runtime's `default` (or existing)
agent ID. A request-scoped factory MUST share the store, rather than creating a
new store on every request. Use distinct provisioned OpenCode endpoints/workspaces
for mutually untrusted users. Do not expose an OpenCode port directly to browsers.
The example above intentionally does not supply application authentication or
workspace provisioning; it is not a complete public server.

Model credentials belong to the OpenCode server. Adapter authentication headers
are server-to-server credentials and are never read from `forwardedProps`.
Changing model/directory/credentials for an existing namespace should create a
new scope or explicitly dispose its old threads before routing changes.

### Native interrupt contract

`reason` is `opencode.permission` or `opencode.question`; `id` is the upstream
request ID. `metadata` contains OpenCode's request metadata. Treat it as untrusted
renderable data; do not render it as raw HTML or automatically grant permission.

Permission resume:

```ts
{ interruptId, status: "resolved", payload: { approved: true } }
```

Only the exact boolean `true` approves once. Cancellation or any other value
rejects. Question resume uses OpenCode's array-of-answer-arrays:

```ts
{ interruptId, status: "resolved", payload: { answers: [["main"]] } }
```

Question cancellation uses `{ interruptId, status: "cancelled" }`. A mismatched
interrupt ID does not consume a pending approval. Paused sessions retain live
upstream work: explicitly reject/dispose abandoned requests during application
lifecycle management. `disposeThread(threadId)` is server-side only and must be
protected by ownership checks. Stopping the local SSE stream is not sufficient;
the implementation also calls OpenCode's native abort route.

### Deployment limits

The session store and locks are process-local. Independent request clones share
that store, but distinct application replicas do not. Do not deploy this initial
adapter behind arbitrary multi-replica routing expecting durable reconnect or
resumable approvals. OpenCode's filesystem/shell permissions are not a security
sandbox. Container/VM isolation, least-privilege mounts, user authorization, output
policy, retention and auditing remain the application's responsibility.

## Verification gates

Run from a complete checkout, with the repository's locked dependencies installed:

```sh
pnpm exec nx run opencode-protocol-tests:test
pnpm exec nx run @copilotkit/runtime:build
pnpm exec nx run @copilotkit/runtime:check-types
pnpm exec nx run opencode-protocol-tests:runtime
npm install --global opencode-ai@1.18.32
pnpm exec nx run opencode-protocol-tests:live
```

1. **Protocol gate:** Strict-compiles the dependency-free bridge, then runs Node
   tests against a real local HTTP/SSE fixture. It does NOT run OpenCode, an LLM,
   the CopilotKit wrapper or a browser. Without Nx, the development fallback is
   `node tools/opencode-tests/run-tests.mjs` with `tsc` on PATH.
2. **Runtime gate:** Loads the actual built runtime and tests lifecycle, cloning,
   shared sessions, advertised capabilities, permission resume and HTTP endpoints.
   It still uses an OpenCode fixture. Missing builds fail, not skip.
3. **Live gate:** Loads the actual built runtime, starts a pinned OpenCode process
   with isolated temporary configuration, and drives its native file-read/tool
   loop through the runtime. The only fake is the deterministic local model API;
   no model-provider secret or paid inference is required. It checks the native
   tool result is returned to OpenCode's next model call. Missing prerequisites
   fail. It does NOT test a browser or actual model quality.
4. **Remaining acceptance:** Exercise the real chat UI and interrupt UI, stop and
   parallel-user behavior in the intended deployment; verify real model behavior;
   complete missing browser-tool/state contracts before claiming parity.

The candidate GitHub Actions workflow is restricted to the feature branch / PR
paths, uses read-only repository permissions, and performs no publishing or
production deployment. It has not been run during offline preparation.

Before opening a PR, also run the repository formatting and generated public-API
manifest checks. This candidate adds exports and must not bypass those checks.

Protocol references:

- https://opencode.ai/docs/server/
- https://opencode.ai/docs/sdk/
- https://docs.copilotkit.ai/backend/custom-agent
- https://github.com/anomalyco/opencode/blob/dev/packages/sdk/js/src/v2/gen/types.gen.ts
