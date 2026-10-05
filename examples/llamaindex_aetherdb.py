"""
AetherDB LlamaIndex Integration Example
Demonstrates persistent vector indexing, semantic node querying, and KV storage with LlamaIndex.
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
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "integrations", "llamaindex")))

from llama_index.core.schema import TextNode
from llama_index.core.vector_stores.types import VectorStoreQuery
from aetherdb import AetherDB
from aetherdb_llamaindex import AetherDBVectorStore, AetherDBKVStore


def main():
    print("[*] AetherDB LlamaIndex Integration Walkthrough")
    print("------------------------------------------------\n")

    db = AetherDB("http://127.0.0.1:8301")
    agent_id = "llamaindex-research-agent"

    # 1. LlamaIndex VectorStore Ingestion
    print("1. Ingesting TextNodes into AetherDBVectorStore...")
    vectorstore = AetherDBVectorStore(db=db, agent_id=agent_id)

    nodes = [
        TextNode(
            text="SkipList MemTables provide lock-free concurrent writes and ordered range scans.",
            id_="node_skiplist_01",
            embedding=[0.92, 0.08, 0.0, 0.0],
            metadata={"chapter": "storage_engine", "level": "internals"},
        ),
        TextNode(
            text="Distributed 2PC transaction coordinator enforces Snapshot Isolation across Multi-Raft shards.",
            id_="node_2pc_02",
            embedding=[0.05, 0.95, 0.0, 0.0],
            metadata={"chapter": "transactions", "level": "distributed"},
        ),
    ]

    added_ids = vectorstore.add(nodes)
    print(f"   [+] Indexed {len(added_ids)} nodes in AetherDB HNSW vector index: {added_ids}\n")

    # 2. VectorStore Query
    print("2. Querying VectorStore for query vector [0.95, 0.05, 0.0, 0.0]...")
    query = VectorStoreQuery(
        query_embedding=[0.95, 0.05, 0.0, 0.0],
        similarity_top_k=2,
    )
    query_result = vectorstore.query(query)

    print(f"   [+] Retrieved {len(query_result.nodes)} ranked nodes:")
    for rank, (node, sim) in enumerate(zip(query_result.nodes, query_result.similarities), start=1):
        print(f"     [Rank #{rank}] ID: {node.node_id} (Similarity: {sim * 100:.2f}%)")
        print(f"       Text: \"{node.get_content()}\"")
        print(f"       Metadata: {node.metadata}")
    print()

    # 3. LlamaIndex KVStore
    print("3. Persisting Structured State into AetherDBKVStore...")
    kvstore = AetherDBKVStore(db=db, agent_id=agent_id)

    kvstore.put("pipeline_config", {
        "chunk_size": 512,
        "chunk_overlap": 64,
        "active_index": "aetherdb-hnsw",
    }, collection="configs")
    print("   [+] Config saved to KV store.")

    retrieved_cfg = kvstore.get("pipeline_config", collection="configs")
    print("   [+] Retrieved Config from KV store:", retrieved_cfg, "\n")

    print("[SUCCESS] LlamaIndex vector store and KV store workflow completed successfully!")


if __name__ == "__main__":
    main()
