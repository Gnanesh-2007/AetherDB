"""
Integration and unit tests for AetherDB LlamaIndex integration.
Verifies vector store querying, node persistence, KVStore operations, and cross-agent isolation.
"""

import os
import sys
import unittest

# Add paths for aetherdb and aetherdb_llamaindex
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "sdks", "python")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from llama_index.core.schema import TextNode
from llama_index.core.vector_stores.types import VectorStoreQuery
from aetherdb import AetherDB
from aetherdb_llamaindex import AetherDBVectorStore, AetherDBKVStore


class TestLlamaIndexVectorStore(unittest.TestCase):
    """Test LlamaIndex VectorStore adapter backed by AetherDB."""

    @classmethod
    def setUpClass(cls):
        cls.endpoint = os.environ.get("AETHER_TEST_URL", "http://127.0.0.1:8301")
        cls.api_key = os.environ.get("AETHERDB_API_KEY") or os.environ.get("AETHERDB_TEST_KEY_A")
        cls.db = AetherDB(endpoint=cls.endpoint, api_key=cls.api_key)

    def test_vectorstore_add_and_query(self):
        agent_id = f"llamaindex-vec-{os.getpid()}"
        store = AetherDBVectorStore(db=self.db, agent_id=agent_id)

        node1 = TextNode(
            text="Multi-Raft shards consensus logs into independent Raft groups.",
            id_="node_raft_01",
            embedding=[1.0, 0.0, 0.0, 0.0],
            metadata={"subsystem": "consensus", "importance": "high"},
        )
        node2 = TextNode(
            text="MVCC timestamps guarantee snapshot isolation without read locks.",
            id_="node_mvcc_02",
            embedding=[0.0, 1.0, 0.0, 0.0],
            metadata={"subsystem": "transactions", "importance": "high"},
        )

        added_ids = store.add([node1, node2])
        self.assertEqual(added_ids, ["node_raft_01", "node_mvcc_02"])

        # Query nearest to node 1
        query = VectorStoreQuery(
            query_embedding=[0.98, 0.05, 0.0, 0.0],
            similarity_top_k=2,
        )
        res = store.query(query)

        self.assertGreater(len(res.nodes), 0)
        top_node = res.nodes[0]
        self.assertEqual(top_node.node_id, "node_raft_01")
        self.assertIn("Multi-Raft shards consensus", top_node.get_content())
        self.assertEqual(top_node.metadata["subsystem"], "consensus")
        self.assertGreater(res.similarities[0], 0.95)

    def test_vectorstore_cross_agent_isolation(self):
        """Verify LlamaIndex nodes stored by Agent A cannot be queried by Agent B."""
        agent_a = f"llama-agent-a-{os.getpid()}"
        agent_b = f"llama-agent-b-{os.getpid()}"

        store_a = AetherDBVectorStore(db=self.db, agent_id=agent_a)
        store_b = AetherDBVectorStore(db=self.db, agent_id=agent_b)

        node_a = TextNode(
            text="AGENT_A_PROPRIETARY_RESEARCH_DATA",
            id_="llama_node_secret_a",
            embedding=[0.707, 0.707, 0.0, 0.0],
            metadata={"confidential": True},
        )
        store_a.add([node_a])

        # Store B queries with same embedding
        query_b = VectorStoreQuery(
            query_embedding=[0.707, 0.707, 0.0, 0.0],
            similarity_top_k=5,
        )
        res_b = store_b.query(query_b)
        self.assertNotIn("llama_node_secret_a", res_b.ids)

        # Store A queries and finds it
        res_a = store_a.query(query_b)
        self.assertIn("llama_node_secret_a", res_a.ids)


class TestLlamaIndexKVStore(unittest.TestCase):
    """Test LlamaIndex Key-Value Store adapter backed by AetherDB."""

    @classmethod
    def setUpClass(cls):
        cls.endpoint = os.environ.get("AETHER_TEST_URL", "http://127.0.0.1:8301")
        cls.api_key = os.environ.get("AETHERDB_API_KEY") or os.environ.get("AETHERDB_TEST_KEY_A")
        cls.db = AetherDB(endpoint=cls.endpoint, api_key=cls.api_key)

    def test_kvstore_put_get_delete(self):
        agent_id = f"llamaindex-kv-{os.getpid()}"
        kv = AetherDBKVStore(db=self.db, agent_id=agent_id)

        # Initial get
        self.assertIsNone(kv.get("doc_1", collection="docs"))

        # Put
        kv.put("doc_1", {"title": "LSM Architectures", "pages": 15}, collection="docs")

        # Get
        retrieved = kv.get("doc_1", collection="docs")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved["title"], "LSM Architectures")
        self.assertEqual(retrieved["pages"], 15)

        # Delete
        del_res = kv.delete("doc_1", collection="docs")
        self.assertTrue(del_res)
        self.assertIsNone(kv.get("doc_1", collection="docs"))


if __name__ == "__main__":
    unittest.main()
