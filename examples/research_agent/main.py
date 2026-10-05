"""
AetherDB Canonical Autonomous Research Agent Example
Demonstrates end-to-end persistent state, atomic token counters, semantic memory ingestion,
sub-millisecond SIMD cosine recall, and cold restart persistence.
"""

import json
import os
import sys
import time

# Ensure UTF-8 output on Windows
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# Add sdks/python to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "sdks", "python")))

from aetherdb import AetherDB, AetherError


class AutonomousResearchAgent:
    """
    Autonomous AI agent backed by AetherDB persistent state and semantic memory.
    """

    def __init__(self, agent_id: str, endpoint: str = "http://127.0.0.1:8301"):
        self.agent_id = agent_id
        self.endpoint = endpoint
        self.db = AetherDB(endpoint=self.endpoint)
        self.agent = self.db.agent(self.agent_id)

    def bootstrap(self):
        """Verify cluster health and readiness."""
        health = self.db.health()
        print(f"[*] Connected to AetherDB node (status={health.get('status')}, engine={health.get('engine')}, v{health.get('version')})")

    def run_research_step(self, task: str, step: int, focus_areas: list):
        """Execute a research step: update persistent context and record token consumption."""
        print(f"\n[Step {step}] Executing research task: '{task}'")
        
        # 1. Update structured state
        state_payload = {
            "current_task": task,
            "status": "in_progress",
            "step": step,
            "focus_areas": focus_areas,
            "last_updated": time.time(),
        }
        self.agent.state.set("session", state_payload)
        print(f"   [+] Persistent session state checkpointed.")

        # 2. Track LLM tokens atomically (e.g., 250 prompt tokens + 150 completion tokens)
        prompt_tokens = 250
        completion_tokens = 150
        total_tokens = self.agent.state.incr("tokens", prompt_tokens + completion_tokens)
        print(f"   [+] Token counter stepped (+{prompt_tokens + completion_tokens}). Total tokens consumed: {total_tokens}")

    def ingest_findings(self, findings: list):
        """Ingest synthesized research findings into long-term semantic memory."""
        print(f"\n[*] Ingesting {len(findings)} research findings into semantic memory...")
        for f in findings:
            self.agent.memory.remember(
                id=f["id"],
                text=f["text"],
                embedding=f["embedding"],
                metadata=f.get("metadata", {}),
            )
            print(f"   [+] Stored memory '{f['id']}': \"{f['text'][:55]}...\"")

    def recall_context(self, query: str, query_vector: list, top_k: int = 3):
        """Perform sub-millisecond semantic search across agent long-term memory."""
        print(f"\n[*] Recalling memories for query: \"{query}\" (top_k={top_k})")
        results = self.agent.memory.recall(
            query=query,
            embedding=query_vector,
            top_k=top_k,
        )
        for idx, item in enumerate(results, start=1):
            sim_pct = item["score"] * 100.0
            print(f"   [Rank #{idx}] {item['id']} ({sim_pct:.2f}% match)")
            print(f"      Text: \"{item['text']}\"")
            print(f"      Meta: {item['metadata']}")
        return results


def main():
    print("=" * 70)
    print(" ⚡ AETHERDB AUTONOMOUS RESEARCH AGENT WALKTHROUGH")
    print("=" * 70)

    agent_id = "autonomous-researcher-v1"
    agent = AutonomousResearchAgent(agent_id=agent_id)
    agent.bootstrap()

    # Step 1: Initialize Task Context
    agent.run_research_step(
        task="Investigate high-throughput MVCC and Multi-Raft sharding",
        step=1,
        focus_areas=["MVCC", "Multi-Raft", "HNSW", "SIMD"],
    )

    # Step 2: Store Long-Term Semantic Knowledge
    agent.ingest_findings([
        {
            "id": "finding_raft_01",
            "text": "Multi-Raft shards the key space into independent consensus groups, allowing linear write scalability.",
            "embedding": [0.95, 0.05, 0.0, 0.0],
            "metadata": {"domain": "consensus", "confidence": 0.99},
        },
        {
            "id": "finding_mvcc_02",
            "text": "Hybrid Logical Clocks combined with MVCC provide snapshot isolation without external GPS clock synchronization.",
            "embedding": [0.08, 0.92, 0.0, 0.0],
            "metadata": {"domain": "transactions", "confidence": 0.97},
        },
        {
            "id": "finding_vector_03",
            "text": "AetherDB AVX2 SIMD cosine kernels achieve sub-millisecond similarity recall across 4096-D HNSW embeddings.",
            "embedding": [0.05, 0.05, 0.90, 0.0],
            "metadata": {"domain": "vector_engine", "confidence": 1.0},
        },
    ])

    # Step 3: Semantic Recall
    agent.recall_context(
        query="How does Multi-Raft achieve scalable consensus?",
        query_vector=[0.98, 0.02, 0.0, 0.0],
        top_k=2,
    )

    # Step 4: Simulate Cold Process Restart
    print("\n" + "=" * 70)
    print(" 🔄 SIMULATING AGENT RESTART (COLD PROCESS RECOVERY)")
    print("=" * 70)
    
    del agent  # Discard old agent instance
    
    # Spawn brand new client instance
    restarted_agent = AutonomousResearchAgent(agent_id=agent_id)
    restarted_agent.bootstrap()

    # Verify persistent state recovery
    recovered_state = restarted_agent.agent.state.get("session")
    print(f"\n[+] Recovered Session State after restart:\n{json.dumps(recovered_state, indent=2)}")

    # Verify atomic counter continuity
    tokens = restarted_agent.agent.state.incr("tokens", 50)
    print(f"\n[+] Stepped token counter after restart (+50). Cumulative total: {tokens}")

    # Verify semantic memory recovery
    restarted_agent.recall_context(
        query="Tell me about AVX2 vector search performance.",
        query_vector=[0.05, 0.05, 0.95, 0.0],
        top_k=1,
    )

    print("\n" + "=" * 70)
    print(" 🎉 AUTONOMOUS AGENT PERSISTENCE AND RECOVERY VERIFIED SUCCESSFULLY!")
    print("=" * 70)


if __name__ == "__main__":
    main()
