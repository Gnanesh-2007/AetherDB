"""
AetherDB Python SDK End-to-End Agent Memory & State Walkthrough
"""

import json
import os
import sys

# Set standard streams to utf-8 if supported
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# Ensure sdks/python is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "sdks", "python")))

from aetherdb import AetherDB


def main():
    print("[*] AetherDB Python SDK: Agent State & Semantic Memory Walkthrough")
    print("----------------------------------------------------------------\n")

    db = AetherDB("http://127.0.0.1:8301")

    # Health check
    health = db.health()
    print(f"Connected to cluster node: status={health.get('status')}, engine={health.get('engine')}, version={health.get('version')}\n")

    # Initialize an agent handle
    agent = db.agent("research-agent")
    print(f"[*] Initialized Agent Client: \"{agent.agent_id}\"\n")

    # 1. SET Agent State
    print("1. Setting Agent State...")
    agent.state.set("session", {
        "task": "Distributed storage engine research",
        "status": "running",
        "step": 4,
        "focusAreas": ["Multi-Raft", "MVCC", "HNSW", "SIMD"],
    })
    print("   [+] State written successfully.\n")

    # 2. GET Agent State
    print("2. Retrieving Agent State...")
    state = agent.state.get("session")
    print("   [+] Retrieved State:\n", json.dumps(state, indent=2), "\n")

    # 3. REMEMBER Semantic Memories
    print("3. Storing Semantic Memories...")
    agent.memory.remember(
        id="mem_001",
        text="The user prefers Python and Rust for AI systems development.",
        embedding=[0.92, 0.08, 0.0, 0.0],
        metadata={"domain": "programming_preferences", "confidence": 0.98},
    )

    agent.memory.remember(
        id="mem_002",
        text="AetherDB achieves sub-millisecond vector retrieval using AVX2 SIMD HNSW graph indexing.",
        embedding=[0.05, 0.95, 0.0, 0.0],
        metadata={"domain": "systems_architecture", "confidence": 1.0},
    )
    print("   [+] Memories stored with structured metadata and vector embeddings.\n")

    # 4. RECALL Relevant Memories
    print("4. Recalling Relevant Memories for query: 'What languages does the user prefer?'...")
    memories = agent.memory.recall(
        query="What languages does the user prefer?",
        embedding=[0.95, 0.05, 0.0, 0.0],
        top_k=5,
    )

    print(f"   [+] Found {len(memories)} relevant memories:")
    for idx, m in enumerate(memories):
        score_pct = m["score"] * 100.0
        print(f"     [Rank #{idx + 1}] ID: {m['id']} (Similarity: {score_pct:.2f}%)")
        print(f"       Text: \"{m['text']}\"")
        print(f"       Metadata: {m['metadata']}")
    print()

    # 5. INCR Token Quota / Sequence Counter
    print("5. Atomically Stepping Token Quota Counter...")
    tokens1 = agent.state.incr("tokens", 100)
    print(f"   [+] Tokens after +100: {tokens1}")
    tokens2 = agent.state.incr("tokens", 50)
    print(f"   [+] Tokens after +50:  {tokens2}\n")

    # 6. DELETE Agent State
    print("6. Cleaning up Session State...")
    del_res = agent.state.delete("session")
    print(f"   [+] State deleted: {del_res.get('deleted')}")
    final_state = agent.state.get("session")
    print(f"   [+] Verification: final_state={final_state}\n")

    print("[SUCCESS] Complete Python Agent Memory & State Workflow Executed Successfully!")


if __name__ == "__main__":
    main()
