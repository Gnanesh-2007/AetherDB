/**
 * Automated Test Suite for AetherDB Phase 3: Persistent Memory Agent Demo
 *
 * Verifies all 8 required test scenarios against AetherDB:
 * 1. Agent remembers memory
 * 2. Agent recalls memory
 * 3. State survives restart simulation
 * 4. Memory survives restart simulation
 * 5. Agent A cannot retrieve Agent B's memory (strict isolation)
 * 6. Token counter increments atomically
 * 7. Multiple memories return ranked results
 * 8. Empty / no-match recall behaves safely
 */

const { test, describe, before, after } = require("node:test");
const assert = require("node:assert/strict");
const { PersistentMemoryAgent } = require("./agent");
const { AetherDB } = require("../sdks/js");

const DB_ENDPOINT = process.env.AETHER_ADDR || "http://127.0.0.1:8301";

describe("⚡ AetherDB Persistent Memory Agent Test Suite", () => {
  const db = new AetherDB(DB_ENDPOINT);
  const agentIdAlpha = `test-agent-alpha-${Date.now()}`;
  const agentIdBeta = `test-agent-beta-${Date.now()}`;

  let agentAlpha;
  let agentBeta;

  before(async () => {
    // Verify server connectivity
    const health = await db.health();
    assert.ok(health.status === "ok" || health.status === "healthy", "Database health must be ok or healthy");

    agentAlpha = new PersistentMemoryAgent(agentIdAlpha, DB_ENDPOINT);
    agentBeta = new PersistentMemoryAgent(agentIdBeta, DB_ENDPOINT);
  });

  after(async () => {
    // Clean up
    await agentAlpha.clearSession();
    await agentBeta.clearSession();
  });

  test("Test 1: Agent remembers memory with vector embeddings and metadata", async () => {
    const memoryId = "mem_pref_python";
    const text = "The user prefers Python for AI/ML.";
    const meta = { category: "preference", priority: "high" };

    const res = await agentAlpha.remember(memoryId, text, meta);
    assert.equal(res.status, "ok");
    assert.equal(res.agentId, agentIdAlpha);
    assert.equal(res.memoryId, memoryId);
    assert.equal(res.embedding.length, 16);
  });

  test("Test 2: Agent recalls memory matching natural language query", async () => {
    const query = "What programming language does the user prefer?";
    const results = await agentAlpha.recall(query, 5);

    assert.ok(Array.isArray(results), "Results must be an array");
    assert.ok(results.length > 0, "Should recall at least 1 memory");

    const match = results.find((r) => r.memoryId === "mem_pref_python" || r.id === "mem_pref_python");
    assert.ok(match, "Recalled results must contain 'mem_pref_python'");
    assert.match(match.text, /Python for AI\/ML/);
    assert.ok(match.score > 0, "Similarity score must be positive");
  });

  test("Test 3: State survives restart simulation", async () => {
    // 1. Set state in agentAlpha
    const initialSession = {
      status: "running",
      current_task: "distributed systems benchmark",
      step: 42,
    };
    await agentAlpha.updateState(initialSession);

    // 2. Simulate Cold Restart (instantiate a brand new agent instance with no cache)
    const { restoredState } = await PersistentMemoryAgent.simulateRestart(agentIdAlpha, DB_ENDPOINT);

    assert.ok(restoredState, "Restored state must not be null");
    assert.equal(restoredState.status, "running");
    assert.equal(restoredState.current_task, "distributed systems benchmark");
    assert.equal(restoredState.step, 42);
  });

  test("Test 4: Memory survives restart simulation", async () => {
    // 1. Store a unique memory
    const memoryId = "mem_rust_learning";
    const text = "The user is actively mastering Rust async concurrency.";
    await agentAlpha.remember(memoryId, text, { topic: "rust" });

    // 2. Simulate Cold Restart
    const { agent: restartedAgent } = await PersistentMemoryAgent.simulateRestart(agentIdAlpha, DB_ENDPOINT);

    // 3. Recall using the fresh un-cached instance
    const recalled = await restartedAgent.recall("Rust async concurrency", 3);
    const found = recalled.find((m) => m.memoryId === memoryId || m.id === memoryId);

    assert.ok(found, "Memory must be recallable from cold-restarted agent");
    assert.match(found.text, /Rust async concurrency/);
  });

  test("Test 5: Agent A cannot retrieve Agent B's memory (Strict Agent Isolation)", async () => {
    // Store secret memory exclusively in Agent Alpha
    const secretId = "mem_alpha_secret_token_99";
    const secretText = "Agent Alpha top-secret encryption key is ALPHA_KEY_42";
    await agentAlpha.remember(secretId, secretText);

    // Agent Beta queries for the exact same text
    const betaRecalled = await agentBeta.recall("ALPHA_KEY_42", 5);

    const leak = betaRecalled.find((m) => m.memoryId === secretId || m.id === secretId);
    assert.equal(leak, undefined, "Agent Beta MUST NOT recall memories belonging to Agent Alpha");

    // Also check state isolation
    await agentAlpha.updateState({ secretState: "alpha-only" });
    const betaState = await agentBeta.getState();
    assert.notEqual(betaState?.secretState, "alpha-only", "Agent Beta must not see Agent Alpha state");
  });

  test("Test 6: Token counter increments atomically", async () => {
    // Reset or start counter
    const c1 = await agentAlpha.consumeTokens(50);
    assert.ok(c1 >= 50, "Counter should be at least 50");

    const c2 = await agentAlpha.consumeTokens(25);
    assert.equal(c2, c1 + 25, "Counter should increment exactly by 25");

    // Concurrent increments
    const concurrentIncrements = await Promise.all([
      agentAlpha.consumeTokens(10),
      agentAlpha.consumeTokens(10),
      agentAlpha.consumeTokens(10),
    ]);

    const finalCount = await agentAlpha.getTokenCount();
    assert.equal(finalCount, c2 + 30, "All concurrent token increments must be accounted for atomically");
  });

  test("Test 7: Multiple memories return ranked results", async () => {
    // Ingest diverse set of facts
    await agentAlpha.remember("mem_db_aether", "AetherDB is a distributed Multi-Raft database in Rust.");
    await agentAlpha.remember("mem_food_pizza", "The user ordered a mushroom pepperoni pizza for dinner.");
    await agentAlpha.remember("mem_db_raft", "Raft consensus ensures consistent state replication across nodes.");

    // Query specifically for database consensus
    const results = await agentAlpha.recall("distributed consensus Raft replication", 3);

    assert.ok(results.length >= 2, "Should return multiple matches");
    // Verify descending order of similarity scores
    for (let i = 0; i < results.length - 1; i++) {
      assert.ok(
        results[i].score >= results[i + 1].score,
        `Result #${i} (score: ${results[i].score}) must have score >= Result #${i + 1} (score: ${results[i + 1].score})`
      );
    }

    // Top result should be database/Raft related, not pizza
    assert.ok(
      results[0].memoryId === "mem_db_raft" || results[0].memoryId === "mem_db_aether",
      "Top result should be Raft or AetherDB"
    );
  });

  test("Test 8: Empty / no-match recall behaves safely", async () => {
    const emptyAgentId = `empty-agent-${Date.now()}`;
    const emptyAgent = new PersistentMemoryAgent(emptyAgentId, DB_ENDPOINT);

    // Recall on agent with zero ingested memories
    const results = await emptyAgent.recall("quantum gravity supersymmetry string theory", 5);
    assert.ok(Array.isArray(results), "Must return an array");
    assert.equal(results.length, 0, "Must return empty array without throwing error");

    // Empty query string
    const emptyQueryResults = await emptyAgent.recall("", 5);
    assert.ok(Array.isArray(emptyQueryResults), "Must handle empty query string safely");
  });
});
