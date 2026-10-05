const { describe, it } = require("node:test");
const assert = require("node:assert");
const { AetherDB, AetherError } = require("../index.js");

const SERVER_URL = process.env.AETHER_URL || "http://127.0.0.1:8301";

describe("AetherDB Agent SDK Test Suite", () => {
  const db = new AetherDB(SERVER_URL);

  it("should connect and check server health", async () => {
    const health = await db.health();
    assert.strictEqual(health.status, "healthy");
    assert.strictEqual(health.engine, "aetherdb-rust");
  });

  describe("Agent State API", () => {
    const agent = db.agent("test-state-agent");

    it("should set, get, and delete agent state with subkeys", async () => {
      // 1. Initial get should return null
      const initial = await agent.state.get("session_state");
      assert.strictEqual(initial, null);

      // 2. Set subkey state
      const setRes = await agent.state.set("session_state", {
        task: "autonomous coding",
        step: 2,
        status: "in_progress",
      });
      assert.strictEqual(setRes.status, "ok");

      // 3. Get subkey state
      const state = await agent.state.get("session_state");
      assert.deepStrictEqual(state, {
        task: "autonomous coding",
        step: 2,
        status: "in_progress",
      });

      // 4. Delete subkey state
      const delRes = await agent.state.delete("session_state");
      assert.strictEqual(delRes.deleted, true);

      // 5. Get after delete should return null
      const afterDel = await agent.state.get("session_state");
      assert.strictEqual(afterDel, null);
    });

    it("should support root state without explicit subkey", async () => {
      await agent.state.set({ rootTask: "main_workflow", active: true });
      const rootState = await agent.state.get();
      assert.deepStrictEqual(rootState, { rootTask: "main_workflow", active: true });

      await agent.state.delete();
      const afterDel = await agent.state.get();
      assert.strictEqual(afterDel, null);
    });

    it("should atomically increment agent token counters", async () => {
      const key = `token_quota_${Date.now()}`;
      const c1 = await agent.state.incr(key, 100);
      assert.strictEqual(c1, 100);

      const c2 = await agent.state.incr(key, 50);
      assert.strictEqual(c2, 150);

      const c3 = await agent.state.incr(key, 1);
      assert.strictEqual(c3, 151);
    });
  });

  describe("Agent Memory API (Remember & Recall)", () => {
    const agent = db.agent("test-memory-agent");

    it("should remember and semantically recall memories", async () => {
      // Remember memories
      const rem1 = await agent.memory.remember({
        id: "mem_lang",
        text: "The user prefers Python for machine learning workflows.",
        embedding: [0.92, 0.08, 0.0, 0.0],
        metadata: { category: "developer_preferences" },
      });
      assert.strictEqual(rem1.status, "ok");
      assert.strictEqual(rem1.memoryId, "mem_lang");

      const rem2 = await agent.memory.remember({
        id: "mem_sys",
        text: "Distributed consensus in AetherDB is powered by Multi-Raft.",
        embedding: [0.0, 0.95, 0.05, 0.0],
        metadata: { category: "architecture" },
      });
      assert.strictEqual(rem2.status, "ok");

      // Recall query
      const memories = await agent.memory.recall({
        embedding: [0.95, 0.05, 0.0, 0.0],
        topK: 2,
      });

      assert.ok(Array.isArray(memories));
      assert.ok(memories.length >= 1);
      assert.strictEqual(memories[0].memoryId, "mem_lang");
      assert.strictEqual(memories[0].text, "The user prefers Python for machine learning workflows.");
      assert.deepStrictEqual(memories[0].metadata, { category: "developer_preferences" });
      assert.ok(memories[0].score > 0.98);
    });

    it("should isolate memories across distinct agent IDs", async () => {
      const runId = Date.now();
      const agentAlpha = db.agent(`agent-alpha-iso-${runId}`);
      const agentBeta = db.agent(`agent-beta-iso-${runId}`);

      await agentAlpha.memory.remember({
        id: "alpha_secret",
        text: "Alpha confidential mission briefing",
        embedding: [1.0, 0.0, 0.0, 0.0],
      });

      // Beta recalls with Alpha's embedding -> should return 0 results
      const betaRecalled = await agentBeta.memory.recall({
        embedding: [1.0, 0.0, 0.0, 0.0],
        topK: 5,
      });
      assert.strictEqual(betaRecalled.length, 0);

      // Alpha recalls -> returns 1 result
      const alphaRecalled = await agentAlpha.memory.recall({
        embedding: [1.0, 0.0, 0.0, 0.0],
        topK: 5,
      });
      assert.strictEqual(alphaRecalled.length, 1);
      assert.strictEqual(alphaRecalled[0].memoryId, "alpha_secret");
    });
  });

  describe("Error Handling & Validation", () => {
    it("should throw AetherError on invalid agent initialization", () => {
      assert.throws(() => db.agent(""), AetherError);
      assert.throws(() => db.agent("   "), AetherError);
    });

    it("should throw AetherError on missing memory parameters", async () => {
      const agent = db.agent("err-agent");
      await assert.rejects(
        async () => agent.memory.remember({ text: "no id", embedding: [1, 0] }),
        AetherError
      );
      await assert.rejects(
        async () => agent.memory.remember({ id: "id1", embedding: [1, 0] }), // missing text
        AetherError
      );
      await assert.rejects(
        async () => agent.memory.remember({ id: "id1", text: "ok", embedding: [] }), // empty embedding
        AetherError
      );
    });

    it("should throw AetherError on connection failures with bad endpoints", async () => {
      const badDb = new AetherDB("http://127.0.0.1:9999");
      await assert.rejects(async () => badDb.health(), AetherError);
    });
  });

  describe("Existing Primitives Compatibility (KV + Vector + Atomic)", () => {
    it("should support raw KV operations", async () => {
      await db.set("raw_kv_test", { user: "alice", role: "admin" });
      const val = await db.get("raw_kv_test");
      assert.deepStrictEqual(val, { user: "alice", role: "admin" });

      await db.del("raw_kv_test");
      const afterDel = await db.get("raw_kv_test");
      assert.strictEqual(afterDel, null);
    });

    it("should support direct atomic increment", async () => {
      const key = `global_reqs_${Date.now()}`;
      const v1 = await db.atomic.incr(key, 10);
      assert.strictEqual(v1, 10);
      const v2 = await db.incr(key, 5);
      assert.strictEqual(v2, 15);
    });

    it("should support direct vector upsert & search", async () => {
      await db.vector.upsert("vec_doc_01", [0.0, 0.0, 0.0, 1.0], { topic: "simd" });
      const results = await db.vector.search([0.0, 0.0, 0.0, 1.0], 1);
      assert.strictEqual(results[0].id, "vec_doc_01");
      assert.deepStrictEqual(results[0].metadata, { topic: "simd" });
    });
  });
});
