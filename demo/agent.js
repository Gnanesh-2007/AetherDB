/**
 * Persistent Memory AI Agent backed entirely by AetherDB.
 *
 * Demonstrates:
 * 1. Persistent agent state (status, current task, execution step)
 * 2. Long-term semantic memory recall backed by AVX2 SIMD vector search
 * 3. Atomic token quota tracking via Raft/LSM atomic counter
 * 4. Agent isolation (strict tenant/agent namespace separation)
 * 5. Full crash & restart recovery from AetherDB persistent storage
 */

const { AetherDB } = require("../sdks/js");
const { embedText } = require("./embeddings");

class PersistentMemoryAgent {
  /**
   * @param {string} [agentId="demo-agent"]
   * @param {string} [endpoint="http://127.0.0.1:8301"]
   */
  constructor(agentId = "demo-agent", endpoint = "http://127.0.0.1:8301") {
    this.agentId = agentId;
    this.endpoint = endpoint;
    this.db = new AetherDB(endpoint);
    this.agent = this.db.agent(agentId);
  }

  /**
   * Step 1: Initialize default agent state
   */
  async initializeState() {
    const initialState = {
      status: "ready",
      current_task: null,
      step: 0,
      initialized_at: new Date().toISOString(),
    };
    await this.agent.state.set("session", initialState);
    return initialState;
  }

  /**
   * Retrieve active agent session state
   */
  async getState() {
    return await this.agent.state.get("session");
  }

  /**
   * Update active agent session state
   */
  async updateState(newState) {
    await this.agent.state.set("session", newState);
    return newState;
  }

  /**
   * Step 2: Remember a fact with semantic vector embedding
   * @param {string} memoryId
   * @param {string} text
   * @param {object} [metadata={}]
   */
  async remember(memoryId, text, metadata = {}) {
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

  /**
   * Step 3: Recall memories semantically matching a natural language query
   * @param {string} query
   * @param {number} [topK=5]
   */
  async recall(query, topK = 5) {
    const queryEmbedding = embedText(query);
    const results = await this.agent.memory.recall({
      query,
      embedding: queryEmbedding,
      topK,
    });
    return results;
  }

  /**
   * Step 5: Atomically increment agent token usage counter
   * @param {number} amount
   */
  async consumeTokens(amount = 1) {
    return await this.agent.state.incr("tokens", amount);
  }

  /**
   * Query current total tokens consumed
   */
  async getTokenCount() {
    const count = await this.agent.state.get("tokens");
    return typeof count === "number" ? count : (count ? Number(count) : 0);
  }

  /**
   * Reset / clear session state
   */
  async clearSession() {
    await this.agent.state.delete("session");
    await this.agent.state.delete("tokens");
    return { cleared: true };
  }

  /**
   * Simulate a complete application process restart.
   * Discards in-memory state and connects afresh to AetherDB storage.
   */
  static async simulateRestart(agentId = "demo-agent", endpoint = "http://127.0.0.1:8301") {
    // New un-cached instance
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

module.exports = {
  PersistentMemoryAgent,
};
