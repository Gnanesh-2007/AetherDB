/**
 * Persistent Memory AI Agent backed entirely by AetherDB.
 * TypeScript implementation.
 */

import { AetherDB, AgentClient, RecallResult } from "../sdks/js";
import { embedText, DIMENSIONS } from "./embeddings";

export interface AgentSessionState {
  status: string;
  current_task: string | null;
  step: number;
  initialized_at?: string;
  [key: string]: any;
}

export class PersistentMemoryAgent {
  public readonly agentId: string;
  public readonly endpoint: string;
  public readonly db: AetherDB;
  public readonly agent: AgentClient;

  constructor(agentId: string = "demo-agent", endpoint: string = "http://127.0.0.1:8301") {
    this.agentId = agentId;
    this.endpoint = endpoint;
    this.db = new AetherDB(endpoint);
    this.agent = this.db.agent(agentId);
  }

  async initializeState(): Promise<AgentSessionState> {
    const initialState: AgentSessionState = {
      status: "ready",
      current_task: null,
      step: 0,
      initialized_at: new Date().toISOString(),
    };
    await this.agent.state.set("session", initialState);
    return initialState;
  }

  async getState(): Promise<AgentSessionState | null> {
    return (await this.agent.state.get("session")) as AgentSessionState | null;
  }

  async updateState(newState: AgentSessionState): Promise<AgentSessionState> {
    await this.agent.state.set("session", newState);
    return newState;
  }

  async remember(memoryId: string, text: string, metadata: Record<string, any> = {}): Promise<any> {
    const embedding = embedText(text);
    const res = await this.agent.memory.remember({
      id: memoryId,
      text,
      embedding,
      metadata: {
        ...metadata,
        timestamp: new Date().toISOString(),
      },
    });
    return { ...res, embedding };
  }

  async recall(query: string, topK: number = 5): Promise<RecallResult[]> {
    const queryEmbedding = embedText(query);
    return await this.agent.memory.recall({
      query,
      embedding: queryEmbedding,
      topK,
    });
  }

  async consumeTokens(amount: number = 1): Promise<number> {
    return await this.agent.state.incr("tokens", amount);
  }

  async getTokenCount(): Promise<number> {
    const count = await this.agent.state.get("tokens");
    return typeof count === "number" ? count : count ? Number(count) : 0;
  }

  async clearSession(): Promise<{ cleared: boolean }> {
    await this.agent.state.delete("session");
    await this.agent.state.delete("tokens");
    return { cleared: true };
  }

  static async simulateRestart(agentId: string = "demo-agent", endpoint: string = "http://127.0.0.1:8301"): Promise<{
    agent: PersistentMemoryAgent;
    restoredState: AgentSessionState | null;
    tokenCount: number;
  }> {
    const restartedAgent = new PersistentMemoryAgent(agentId, endpoint);
    const restoredState = await restartedAgent.getState();
    const tokenCount = await restartedAgent.getTokenCount();
    return {
      agent: restartedAgent,
      restoredState,
      tokenCount,
    };
  }
}
