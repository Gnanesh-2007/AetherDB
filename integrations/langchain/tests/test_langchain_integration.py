"""
Integration and unit tests for AetherDB LangChain integration.
Verifies chat history persistence, vector store operations, metadata roundtrips, and cross-agent isolation.
"""

import os
import sys
import unittest

# Add paths for aetherdb and aetherdb_langchain
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "sdks", "python")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from aetherdb import AetherDB
from aetherdb_langchain import AetherDBChatMessageHistory, AetherDBVectorStore


class TestLangChainChatHistory(unittest.TestCase):
    """Test LangChain Chat Message History integration with AetherDB."""

    @classmethod
    def setUpClass(cls):
        cls.endpoint = os.environ.get("AETHER_TEST_URL", "http://127.0.0.1:8301")
        cls.api_key = os.environ.get("AETHERDB_API_KEY") or os.environ.get("AETHERDB_TEST_KEY_A")
        cls.db = AetherDB(endpoint=cls.endpoint, api_key=cls.api_key)

    def test_chat_history_lifecycle(self):
        agent_id = f"langchain-chat-{os.getpid()}"
        history = AetherDBChatMessageHistory(agent_id=agent_id, db=self.db)

        # Initial messages should be empty
        self.assertEqual(len(history.messages), 0)

        # Add messages
        history.add_message(HumanMessage(content="Hello AetherDB, please store this session."))
        history.add_message(AIMessage(content="Acknowledged. State is now persisted in AetherDB LSM engine."))

        # Verify messages list
        msgs = history.messages
        self.assertEqual(len(msgs), 2)
        self.assertIsInstance(msgs[0], HumanMessage)
        self.assertEqual(msgs[0].content, "Hello AetherDB, please store this session.")
        self.assertIsInstance(msgs[1], AIMessage)
        self.assertEqual(msgs[1].content, "Acknowledged. State is now persisted in AetherDB LSM engine.")

        # Reconnect with fresh history instance (persistence check)
        fresh_history = AetherDBChatMessageHistory(agent_id=agent_id, db=self.db)
        fresh_msgs = fresh_history.messages
        self.assertEqual(len(fresh_msgs), 2)
        self.assertEqual(fresh_msgs[0].content, "Hello AetherDB, please store this session.")

        # Clear history
        fresh_history.clear()
        self.assertEqual(len(fresh_history.messages), 0)

    def test_chat_history_cross_agent_isolation(self):
        """Verify Agent A chat history is strictly isolated from Agent B."""
        agent_a = f"lc-agent-a-{os.getpid()}"
        agent_b = f"lc-agent-b-{os.getpid()}"

        history_a = AetherDBChatMessageHistory(agent_id=agent_a, db=self.db)
        history_b = AetherDBChatMessageHistory(agent_id=agent_b, db=self.db)

        history_a.add_message(HumanMessage(content="SECRET_MESSAGE_ALPHA"))
        history_b.add_message(HumanMessage(content="SECRET_MESSAGE_BETA"))

        msgs_a = history_a.messages
        msgs_b = history_b.messages

        self.assertEqual(len(msgs_a), 1)
        self.assertEqual(msgs_a[0].content, "SECRET_MESSAGE_ALPHA")

        self.assertEqual(len(msgs_b), 1)
        self.assertEqual(msgs_b[0].content, "SECRET_MESSAGE_BETA")


class TestLangChainVectorStore(unittest.TestCase):
    """Test LangChain VectorStore adapter backed by AetherDB."""

    @classmethod
    def setUpClass(cls):
        cls.endpoint = os.environ.get("AETHER_TEST_URL", "http://127.0.0.1:8301")
        cls.api_key = os.environ.get("AETHERDB_API_KEY") or os.environ.get("AETHERDB_TEST_KEY_A")
        cls.db = AetherDB(endpoint=cls.endpoint, api_key=cls.api_key)

    def test_vectorstore_add_and_search_by_vector(self):
        agent_id = f"langchain-vec-{os.getpid()}"
        store = AetherDBVectorStore(db=self.db, agent_id=agent_id)

        texts = [
            "Raft provides replicated state machine consensus across distributed nodes.",
            "SkipList MemTable offers lock-free concurrent writes with deterministic sorting.",
        ]
        metadatas = [
            {"domain": "consensus", "importance": "high"},
            {"domain": "storage", "importance": "medium"},
        ]
        embeddings = [
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
        ]
        ids = ["doc_raft_01", "doc_skiplist_02"]

        added_ids = store.add_texts(texts=texts, metadatas=metadatas, ids=ids, embeddings=embeddings)
        self.assertEqual(added_ids, ["doc_raft_01", "doc_skiplist_02"])

        # Search by vector closest to Raft doc
        results = store.similarity_search_by_vector_with_score(
            embedding=[0.99, 0.02, 0.0, 0.0],
            k=2,
        )
        self.assertGreater(len(results), 0)
        top_doc, score = results[0]

        self.assertEqual(top_doc.id, "doc_raft_01")
        self.assertIn("Raft provides replicated state machine", top_doc.page_content)
        self.assertEqual(top_doc.metadata["domain"], "consensus")
        self.assertGreater(score, 0.95)

    def test_vectorstore_cross_agent_isolation(self):
        """Verify Agent A memories cannot be searched or recalled by Agent B."""
        agent_a = f"lc-vec-agent-a-{os.getpid()}"
        agent_b = f"lc-vec-agent-b-{os.getpid()}"

        store_a = AetherDBVectorStore(db=self.db, agent_id=agent_a)
        store_b = AetherDBVectorStore(db=self.db, agent_id=agent_b)

        store_a.add_texts(
            texts=["AGENT_A_TOP_SECRET_MISSION"],
            metadatas=[{"agent": "a"}],
            ids=["doc_a_secret"],
            embeddings=[[0.8, 0.6, 0.0, 0.0]],
        )

        # Store B queries with Agent A's exact embedding
        results_b = store_b.similarity_search_by_vector(
            embedding=[0.8, 0.6, 0.0, 0.0],
            k=5,
        )
        recalled_ids_b = [d.id for d in results_b]
        self.assertNotIn("doc_a_secret", recalled_ids_b)

        # Store A queries and retrieves it
        results_a = store_a.similarity_search_by_vector(
            embedding=[0.8, 0.6, 0.0, 0.0],
            k=5,
        )
        recalled_ids_a = [d.id for d in results_a]
        self.assertIn("doc_a_secret", recalled_ids_a)


if __name__ == "__main__":
    unittest.main()
