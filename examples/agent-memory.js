const { AetherDB } = require("../sdks/js");

async function main() {
  console.log("⚡ AetherDB Agent Memory & State SDK End-to-End Walkthrough");
  console.log("----------------------------------------------------------\n");

  const db = new AetherDB("http://localhost:8301");

  // Health check
  const health = await db.health();
  console.log(`Connected to cluster node: status=${health.status}, engine=${health.engine}, version=${health.version}\n`);

  // Initialize an agent handle
  const agent = db.agent("research-agent");
  console.log(`🤖 Initialized Agent Client: "${agent.agentId}"\n`);

  // 1. SET Agent State
  console.log("1. Setting Agent State...");
  await agent.state.set("session", {
    task: "database research",
    status: "running",
    step: 4,
    focusAreas: ["Multi-Raft", "MVCC", "HNSW"],
  });
  console.log("   ✓ State written successfully.\n");

  // 2. GET Agent State
  console.log("2. Retrieving Agent State...");
  const state = await agent.state.get("session");
  console.log("   ✓ Retrieved State:", JSON.stringify(state, null, 2), "\n");

  // 3. REMEMBER Semantic Memories
  console.log("3. Storing Semantic Memories...");
  await agent.memory.remember({
    id: "mem_001",
    text: "The user prefers Python for AI development.",
    embedding: [0.92, 0.08, 0.0, 0.0],
    metadata: { domain: "programming_preferences", confidence: 0.98 },
  });

  await agent.memory.remember({
    id: "mem_002",
    text: "AetherDB achieves sub-millisecond vector retrieval using AVX2 SIMD HNSW graph indexing.",
    embedding: [0.05, 0.95, 0.0, 0.0],
    metadata: { domain: "systems_architecture", confidence: 1.0 },
  });
  console.log("   ✓ Memories stored with structured metadata and vector embeddings.\n");

  // 4. RECALL Relevant Memories
  console.log("4. Recalling Relevant Memories for query: 'What language does the user prefer for AI?'...");
  const memories = await agent.memory.recall({
    query: "What language does the user prefer for AI?",
    embedding: [0.95, 0.05, 0.0, 0.0],
    topK: 5,
  });

  console.log(`   ✓ Found ${memories.length} relevant memories:`);
  memories.forEach((m, idx) => {
    console.log(`     [Rank #${idx + 1}] ID: ${m.memoryId} (Similarity: ${(m.score * 100).toFixed(2)}%)`);
    console.log(`       Text: "${m.text}"`);
    console.log(`       Metadata:`, m.metadata);
  });
  console.log();

  // 5. INCR Token Quota / Sequence Counter
  console.log("5. Atomically Stepping Token Quota Counter...");
  const tokens1 = await agent.state.incr("tokens", 100);
  console.log(`   ✓ Tokens after +100: ${tokens1}`);
  const tokens2 = await agent.state.incr("tokens", 50);
  console.log(`   ✓ Tokens after +50:  ${tokens2}\n`);

  // 6. DELETE Agent State
  console.log("6. Cleaning up Session State...");
  const delRes = await agent.state.delete("session");
  console.log(`   ✓ State deleted: deleted=${delRes.deleted}`);
  const finalState = await agent.state.get("session");
  console.log(`   ✓ Verification: finalState=${finalState}\n`);

  console.log("🎉 Complete Agent Memory & State Workflow Executed Successfully!");
}

main().catch((err) => {
  console.error("Workflow failed with error:", err);
  process.exit(1);
});
