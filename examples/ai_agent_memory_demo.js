const { AetherDB } = require("../sdks/js");

async function main() {
  console.log("╔════════════════════════════════════════════════════════════════════════╗");
  console.log("║         🤖 Phase 4: Autonomous AI Agent Memory & State Showcase        ║");
  console.log("╚════════════════════════════════════════════════════════════════════════╝\n");

  const db = new AetherDB({ endpoint: "http://127.0.0.1:8301" });

  // 1. Health Check
  const health = await db.health();
  console.log("1. Connected to AetherDB Gateway:", health);

  // 2. Working Session State (KV Layer)
  console.log("\n2. Storing Active AI Agent Context...");
  await db.set("agent:session:9901", {
    userId: "user_42",
    agentName: "ResearchSentinel",
    currentTask: "Analyzing distributed database whitepapers",
    callStackDepth: 3,
    status: "EXECUTING_STEP_4",
  });

  const session = await db.get("agent:session:9901");
  console.log("   ✔ Retrieved Session State:", session);

  // 3. Atomic Token / Quota Tracking
  console.log("\n3. Incrementing User Token Usage (Atomic CAS Counter)...");
  const tokensUsed1 = await db.atomic.incr("quota:user_42:tokens", 450);
  const tokensUsed2 = await db.atomic.incr("quota:user_42:tokens", 350);
  console.log("   ✔ Accumulated Token Count:", tokensUsed2, "tokens (Rate limit safe)");

  // 4. Ingesting Semantic Long-Term Memories (Vector Layer)
  console.log("\n4. Ingesting Knowledge Embeddings into Vector Space...");
  
  // Dummy 8-dimensional normalized embeddings for demo
  await db.vector.upsert(
    "doc:raft_paper",
    [0.91, 0.12, 0.05, 0.02, -0.15, 0.33, 0.04, 0.11],
    { title: "In Search of an Understandable Consensus Algorithm (Raft)", tags: ["distributed", "consensus"] }
  );

  await db.vector.upsert(
    "doc:lsm_paper",
    [0.88, 0.25, -0.10, 0.08, 0.05, 0.28, -0.02, 0.14],
    { title: "The Log-Structured Merge-Tree (LSM-Tree)", tags: ["storage", "persistence"] }
  );

  await db.vector.upsert(
    "doc:cooking_recipe",
    [-0.10, -0.85, 0.44, 0.32, 0.11, -0.22, 0.90, -0.05],
    { title: "Italian Wood-Fired Pizza Recipe", tags: ["food", "culinary"] }
  );

  console.log("   ✔ Ingested 3 Knowledge Documents");

  // 5. Querying Semantic Memory (Vector Search)
  console.log("\n5. Executing Semantic Similarity Query for: 'Distributed Consensus & Replication'");
  const queryVector = [0.95, 0.10, 0.02, 0.01, -0.10, 0.30, 0.01, 0.10]; // High similarity to Raft
  const memories = await db.vector.search(queryVector, 2);

  console.log("   ✔ Top-2 Nearest Memories Retrieved via SIMD Cosine Search:");
  memories.forEach((m, idx) => {
    console.log(`      [${idx + 1}] ID: ${m.id} (Score: ${(m.score * 100).toFixed(2)}%)`);
    console.log(`          Title: "${m.metadata.title}" | Tags: [${m.metadata.tags.join(", ")}]`);
  });

  console.log("\n==========================================================================");
  console.log("🎉 Autonomous AI Agent Memory Lifecycle Completed Successfully!");
  console.log("==========================================================================\n");
}

main().catch(console.error);
