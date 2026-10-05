"""
AetherDB LangChain Integration Example
Demonstrates persistent conversational chat history and semantic vector store memory for LangChain agents.
"""

import os
import sys

# Ensure utf-8 output encoding on Windows
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# Add paths
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "sdks", "python")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "integrations", "langchain")))

from langchain_core.messages import HumanMessage, AIMessage
from aetherdb import AetherDB
from aetherdb_langchain import AetherDBChatMessageHistory, AetherDBVectorStore


def main():
    print("[*] AetherDB LangChain Integration Walkthrough")
    print("-----------------------------------------------\n")

    db = AetherDB("http://127.0.0.1:8301")
    agent_id = "langchain-research-agent"

    # 1. Conversational Chat History Persistence
    print("1. Persisting LangChain Chat Message History...")
    history = AetherDBChatMessageHistory(agent_id=agent_id, db=db)

    history.add_message(HumanMessage(content="What are the main subsystems of AetherDB?"))
    history.add_message(AIMessage(content="AetherDB combines an LSM storage engine, HNSW vector indexing, MVCC 2PC transactions, and Multi-Raft replication."))
    print("   [+] Messages saved to agent persistent state.\n")

    # Re-retrieve from fresh history instance
    fresh_history = AetherDBChatMessageHistory(agent_id=agent_id, db=db)
    print("2. Re-retrieving Chat History across client instances:")
    for msg in fresh_history.messages:
        role = "Human" if isinstance(msg, HumanMessage) else "AI"
        print(f"   [{role}]: {msg.content}")
    print()

    # 2. Semantic Vector Store Memory
    print("3. Ingesting Documents into AetherDBVectorStore...")
    vectorstore = AetherDBVectorStore(db=db, agent_id=agent_id)

    vectorstore.add_texts(
        texts=[
            "AetherDB achieves sub-millisecond vector similarity search using AVX2 SIMD kernels.",
            "Hybrid Logical Clocks provide causally consistent distributed timestamps without atomic clocks.",
            "Multi-Raft manages parallel consensus groups across sharded partitions.",
        ],
        metadatas=[
            {"category": "vector_engine", "simd": "avx2"},
            {"category": "consensus_clock", "algorithm": "HLC"},
            {"category": "consensus_replication", "algorithm": "Multi-Raft"},
        ],
        ids=["doc_simd_01", "doc_hlc_02", "doc_multiraft_03"],
        embeddings=[
            [0.92, 0.08, 0.0, 0.0],
            [0.05, 0.95, 0.0, 0.0],
            [0.02, 0.08, 0.90, 0.0],
        ],
    )
    print("   [+] Documents indexed in AetherDB HNSW engine.\n")

    # 3. Vector Similarity Search
    print("4. Performing Vector Similarity Recall for query vector [0.95, 0.05, 0.0, 0.0]...")
    results = vectorstore.similarity_search_by_vector_with_score(
        embedding=[0.95, 0.05, 0.0, 0.0],
        k=2,
    )

    print(f"   [+] Recalled {len(results)} relevant documents:")
    for rank, (doc, score) in enumerate(results, start=1):
        print(f"     [Rank #{rank}] ID: {doc.id} (Similarity: {score * 100:.2f}%)")
        print(f"       Text: \"{doc.page_content}\"")
        print(f"       Metadata: {doc.metadata}")
    print()

    print("[SUCCESS] LangChain persistent chat history and vector store workflow completed successfully!")


if __name__ == "__main__":
    main()
