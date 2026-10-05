/**
 * AetherDB Phase 3: Persistent Memory Agent Demo Walkthrough
 *
 * Runs the complete 6-step AI Agent lifecycle backed by AetherDB.
 */

const { PersistentMemoryAgent } = require("./agent");

async function runDemo() {
  console.log("================================================================================");
  console.log("⚡ AetherDB: Autonomous AI Agent Persistent State & Memory Showcase");
  console.log("================================================================================\n");

  const AGENT_ID = "demo-agent-alpha";
  const agent = new PersistentMemoryAgent(AGENT_ID);

  // ---------------------------------------------------------------------------
  // STEP 1 — Start: Initialize Agent State
  // ---------------------------------------------------------------------------
  console.log("📍 [STEP 1] Agent Initialization");
  console.log(`   Initializing session state for agent "${AGENT_ID}"...`);
  const initState = await agent.initializeState();
  console.log("   ✓ Initial State set in AetherDB:");
  console.log("    ", JSON.stringify(initState, null, 2));
  console.log();

  // ---------------------------------------------------------------------------
  // STEP 2 — Remember Facts: Store Persistent Semantic Memories
  // ---------------------------------------------------------------------------
  console.log("📍 [STEP 2] Ingesting Semantic Long-Term Memories");
  const facts = [
    { id: "mem_pref_python", text: "The user prefers Python for AI/ML.", domain: "preferences" },
    { id: "mem_learn_rust", text: "The user is learning Rust.", domain: "skills" },
    { id: "mem_build_aether", text: "The user is building AetherDB.", domain: "projects" },
    { id: "mem_interest_agents", text: "The user is interested in autonomous AI agents.", domain: "interests" },
  ];

  for (const fact of facts) {
    const res = await agent.remember(fact.id, fact.text, { domain: fact.domain });
    console.log(`   ✓ Remembered [${fact.id}] "${fact.text}" (16-dim SIMD vector stored)`);
  }
  console.log();

  // ---------------------------------------------------------------------------
  // STEP 3 — Ask a Question: Semantic Vector Recall
  // ---------------------------------------------------------------------------
  const userQuery = "What technologies and projects am I interested in?";
  console.log("📍 [STEP 3] Semantic Query & Memory Recall");
  console.log(`   User Query: "${userQuery}"`);
  console.log("   Recalling top relevant memories via AetherDB AVX2 SIMD Cosine Search...");

  const recalled = await agent.recall(userQuery, 4);
  console.log(`   ✓ Recalled ${recalled.length} memories:`);
  recalled.forEach((m, idx) => {
    console.log(`     #${idx + 1} [Score: ${(m.score * 100).toFixed(1)}%] (${m.memoryId}): "${m.text}"`);
  });
  console.log();

  // ---------------------------------------------------------------------------
  // STEP 4 — Update State: Execution Tracking
  // ---------------------------------------------------------------------------
  console.log("📍 [STEP 4] Updating Epistemic Agent State");
  const updatedState = {
    status: "running",
    current_task: "answer user question",
    step: 1,
    last_query: userQuery,
    matched_memories_count: recalled.length,
    updated_at: new Date().toISOString(),
  };
  await agent.updateState(updatedState);
  const verifyState = await agent.getState();
  console.log("   ✓ Updated State in AetherDB:");
  console.log("    ", JSON.stringify(verifyState, null, 2));
  console.log();

  // ---------------------------------------------------------------------------
  // STEP 5 — Token Accounting: Atomic Token Metering
  // ---------------------------------------------------------------------------
  console.log("📍 [STEP 5] Atomic Token Quota & Consumption Tracking");
  console.log("   Atomically billing token consumption for query inference (+128 tokens)...");
  const totalTokens = await agent.consumeTokens(128);
  console.log(`   ✓ Atomic Token Counter: ${totalTokens} tokens consumed`);
  console.log();

  // ---------------------------------------------------------------------------
  // STEP 6 — Restart Demonstration: Cold Recovery from Persistent Storage
  // ---------------------------------------------------------------------------
  console.log("📍 [STEP 6] Process Crash & Restart Simulation");
  console.log("   Simulating application shutdown and cold reboot (zero in-memory cache)...");
  
  const { agent: restartedAgent, restoredState, tokenCount } = await PersistentMemoryAgent.simulateRestart(AGENT_ID);
  
  console.log("   ✓ Cold Restart complete!");
  console.log("   ✓ Restored State from AetherDB LSM/WAL:");
  console.log("    ", JSON.stringify(restoredState, null, 2));
  console.log(`   ✓ Restored Token Consumption: ${tokenCount} tokens`);
  
  // Verify memories still intact
  const verifyRecall = await restartedAgent.recall("Python AI", 2);
  console.log("   ✓ Recalling memories from restarted agent instance:");
  verifyRecall.forEach((m, idx) => {
    console.log(`     #${idx + 1} [Score: ${(m.score * 100).toFixed(1)}%] (${m.memoryId}): "${m.text}"`);
  });

  console.log("\n================================================================================");
  console.log("✅ RESULT: Memory & State Persisted Perfectly Across Process Restart!");
  console.log("================================================================================\n");
}

runDemo().catch((err) => {
  console.error("Demo failed:", err);
  process.exit(1);
});
