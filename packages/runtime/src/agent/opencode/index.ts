import type { AgentCapabilities } from "@ag-ui/core";
import type { BaseEvent } from "@ag-ui/client";
import { BuiltInAgent } from "../index";
import type { OpenCodeOptions } from "./bridge";
import {
  createOpenCodeBridge,
  OpenCodeInterrupt,
  OpenCodeSessionStore,
} from "./bridge";

export { OpenCodeBridgeError, OpenCodeSessionStore } from "./bridge";
export type { OpenCodeOptions } from "./bridge";

/**
 * An opt-in OpenCode execution backend for the existing CopilotKit v2 UI.
 *
 * OpenCode runs separately (`opencode serve`). No Claude Agent SDK or model
 * credentials are loaded here. Supply an authenticated, server-derived scope.
 *
 * Native OpenCode tools and permission/question interrupts are supported.
 * Frontend-defined tools, state mutations, attachments and durable reconnect
 * are intentionally not advertised as supported by this initial adapter.
 */
export class OpenCodeAgent extends BuiltInAgent {
  private readonly openCodeOptions: OpenCodeOptions;
  private readonly openCodeBridge: ReturnType<typeof createOpenCodeBridge>;

  constructor(options: OpenCodeOptions) {
    const captured: OpenCodeOptions = {
      ...options,
      headers: options.headers ? { ...options.headers } : undefined,
      model: options.model ? { ...options.model } : undefined,
      sessions: options.sessions ?? new OpenCodeSessionStore(),
    };
    const bridge = createOpenCodeBridge(captured);
    super({
      type: "custom",
      factory: async function* (context) {
        try {
          for await (const event of bridge.stream(context)) {
            // The dependency-free protocol module emits the AG-UI wire shape.
            // Lifecycle events are owned by BuiltInAgent, not this adapter.
            yield event as BaseEvent;
          }
        } catch (error) {
          if (!(error instanceof OpenCodeInterrupt)) throw error;
          await context.interrupt([
            {
              id: error.pending.id,
              reason: `opencode.${error.pending.kind}`,
              metadata: error.pending.payload,
            },
          ]);
        }
      },
    });
    this.openCodeOptions = captured;
    this.openCodeBridge = bridge;
  }

  override async getCapabilities(): Promise<AgentCapabilities> {
    return {
      transport: { streaming: true },
      tools: { supported: true, clientProvided: false },
      humanInTheLoop: { interrupts: true },
    };
  }

  override clone(): OpenCodeAgent {
    const clone = new OpenCodeAgent(this.openCodeOptions);
    clone.agentId = this.agentId;
    clone.description = this.description;
    // Match BuiltInAgent.clone(): AG-UI keeps the middleware list private.
    // Copies preserve configured middleware without sharing the mutable array.
    // @ts-expect-error accessing private AbstractAgent.middlewares
    clone.middlewares = [...this.middlewares];
    return clone;
  }

  /** Call from authenticated server-side lifecycle management only. */
  disposeThread(threadId: string): Promise<void> {
    return this.openCodeBridge.disposeThread(threadId);
  }
}
