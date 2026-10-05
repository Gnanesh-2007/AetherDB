"""
Unit and integration tests for AetherDB Python SDK.
Verifies Agent hierarchy, state operations, memory recall, isolation, atomic counters, and error handling.
"""

import math
import os
import sys
import unittest

# Ensure the local aetherdb package is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from aetherdb import (
    AetherDB,
    AetherClient,
    AetherError,
    AetherTransport,
    AgentClient,
    AgentStateClient,
    AgentMemoryClient,
)


class TestAetherDBClientBasics(unittest.TestCase):
    """Test client initialization, configuration, and header propagation."""

    def test_client_init_defaults(self):
        db = AetherDB()
        self.assertEqual(db.endpoint, "http://127.0.0.1:8301")
        self.assertIn("Content-Type", db.headers)
        self.assertEqual(db.headers["Content-Type"], "application/json")
        self.assertNotIn("Authorization", db.headers)
        self.assertNotIn("X-Aether-Tenant", db.headers)

    def test_client_init_with_options(self):
        db = AetherDB(
            endpoint="http://127.0.0.1:8301/",
            api_key="secret-key-123",
            tenant_id="tenant-alpha",
            timeout=5.0,
            headers={"X-Custom": "custom-val"},
        )
        self.assertEqual(db.endpoint, "http://127.0.0.1:8301")
        self.assertEqual(db.headers["Authorization"], "Bearer secret-key-123")
        self.assertEqual(db.headers["X-Aether-Tenant"], "tenant-alpha")
        self.assertEqual(db.headers["X-Custom"], "custom-val")
        self.assertEqual(db.transport.timeout, 5.0)

    def test_backwards_compatible_alias(self):
        client = AetherClient("http://127.0.0.1:8301")
        self.assertIsInstance(client, AetherDB)

    def test_agent_client_creation_and_immutability(self):
        db = AetherDB("http://127.0.0.1:8301")
        agent = db.agent("research-agent")
        self.assertIsInstance(agent, AgentClient)
        self.assertEqual(agent.agent_id, "research-agent")
        self.assertIsInstance(agent.state, AgentStateClient)
        self.assertIsInstance(agent.memory, AgentMemoryClient)

        # Immutability check
        with self.assertRaises(AttributeError):
            agent.agent_id = "other-agent"

    def test_agent_id_validation(self):
        db = AetherDB("http://127.0.0.1:8301")
        with self.assertRaises(AetherError):
            db.agent("")
        with self.assertRaises(AetherError):
            db.agent("   ")
        with self.assertRaises(AetherError):
            db.agent("a" * 257)


class TestLiveAetherDBAgentOperations(unittest.TestCase):
    """Live end-to-end tests against the running AetherDB instance."""

    @classmethod
    def setUpClass(cls):
        cls.endpoint = os.environ.get("AETHER_TEST_URL", "http://127.0.0.1:8301")
        cls.api_key = os.environ.get("AETHERDB_API_KEY") or os.environ.get("AETHERDB_TEST_KEY_A")
        cls.db = AetherDB(endpoint=cls.endpoint, api_key=cls.api_key)

        # Verify backend connectivity
        try:
            health = cls.db.health()
            assert health.get("status") == "healthy"
        except Exception as e:
            raise unittest.SkipTest(f"Live AetherDB not reachable at {cls.endpoint}: {e}")

    def test_1_health_check(self):
        health = self.db.health()
        self.assertEqual(health.get("status"), "healthy")
        self.assertEqual(health.get("engine"), "aetherdb-rust")

    def test_2_agent_state_crud(self):
        agent_id = f"test-agent-state-{os.getpid()}"
        agent = self.db.agent(agent_id)

        # Initial state should be None
        init_state = agent.state.get("session")
        self.assertIsNone(init_state)

        # Set structured state
        session_data = {
            "task": "Distributed consensus benchmarking",
            "status": "in_progress",
            "step": 3,
            "params": {"nodes": 3, "quorum": 2},
        }
        res_set = agent.state.set("session", session_data)
        self.assertEqual(res_set.get("status"), "ok")

        # Get structured state
        fetched = agent.state.get("session")
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched["task"], "Distributed consensus benchmarking")
        self.assertEqual(fetched["step"], 3)
        self.assertEqual(fetched["params"]["nodes"], 3)

        # Delete state
        res_del = agent.state.delete("session")
        self.assertEqual(res_del.get("status"), "ok")

        # Verify deletion
        self.assertIsNone(agent.state.get("session"))

    def test_3_agent_atomic_increment(self):
        agent_id = f"test-agent-tokens-{os.getpid()}"
        agent = self.db.agent(agent_id)

        # First increment
        t1 = agent.state.incr("tokens", 150)
        self.assertEqual(t1, 150)

        # Second increment
        t2 = agent.state.incr("tokens", 75)
        self.assertEqual(t2, 225)

        # Third increment with default delta=1
        t3 = agent.state.incr("tokens")
        self.assertEqual(t3, 226)

    def test_4_agent_memory_remember_and_recall(self):
        agent_id = f"test-agent-mem-{os.getpid()}"
        agent = self.db.agent(agent_id)

        # Generate normalized 4-D embedding vectors
        v1 = [1.0, 0.0, 0.0, 0.0]
        v2 = [0.0, 1.0, 0.0, 0.0]
        v_query = [0.99, 0.05, 0.0, 0.0]

        # Remember memories
        rem1 = agent.memory.remember(
            id="raft-consensus",
            text="Raft algorithm guarantees state machine replication across distributed nodes.",
            embedding=v1,
            metadata={"topic": "consensus", "difficulty": "advanced"},
        )
        self.assertEqual(rem1.get("status"), "ok")
        self.assertEqual(rem1.get("memory_id"), "raft-consensus")

        rem2 = agent.memory.remember(
            id="bloom-filters",
            text="Bloom filters provide probabilistic set membership tests with zero false negatives.",
            embedding=v2,
            metadata={"topic": "storage", "structure": "probabilistic"},
        )
        self.assertEqual(rem2.get("status"), "ok")

        # Recall using query vector closest to v1
        results = agent.memory.recall(embedding=v_query, top_k=2)
        self.assertGreater(len(results), 0)

        top_hit = results[0]
        self.assertEqual(top_hit["id"], "raft-consensus")
        self.assertIn("Raft algorithm guarantees", top_hit["text"])
        self.assertGreater(top_hit["score"], 0.95)
        self.assertEqual(top_hit["metadata"]["topic"], "consensus")

    def test_5_agent_isolation(self):
        """Verify strict isolation: Agent B cannot see Agent A's state or recall Agent A's memory."""
        agent_a = self.db.agent("agent-alpha-iso")
        agent_b = self.db.agent("agent-beta-iso")

        # Clean slate setup
        agent_a.state.set("secret", {"plan": "classified-alpha"})
        agent_a.memory.remember(
            id="alpha-memory-1",
            text="Top secret alpha operations log.",
            embedding=[0.8, 0.6, 0.0, 0.0],
            metadata={"classified": True},
        )

        # Agent B queries state of secret
        b_state = agent_b.state.get("secret")
        self.assertIsNone(b_state)

        # Agent B recalls with Agent A's exact embedding
        b_recall = agent_b.memory.recall(embedding=[0.8, 0.6, 0.0, 0.0], top_k=5)
        # Should not contain alpha-memory-1
        recalled_ids = [r["id"] for r in b_recall]
        self.assertNotIn("alpha-memory-1", recalled_ids)

    def test_6_persistence_across_client_instances(self):
        """Verify that state and memories persist when creating fresh client instances."""
        agent_id = f"persist-agent-{os.getpid()}"

        # Instance 1: write
        client_1 = AetherDB(self.endpoint, api_key=self.api_key)
        agent_1 = client_1.agent(agent_id)
        agent_1.state.set("checkpoint", {"stage": "stage_1", "progress": 100})
        agent_1.state.incr("cost_microcents", 5000)
        agent_1.memory.remember(
            id="persist-fact-1",
            text="WAL prevents data loss during sudden crashes.",
            embedding=[0.0, 0.0, 1.0, 0.0],
        )

        # Instance 2: read from independent client instance
        client_2 = AetherDB(self.endpoint, api_key=self.api_key)
        agent_2 = client_2.agent(agent_id)

        state = agent_2.state.get("checkpoint")
        self.assertIsNotNone(state)
        self.assertEqual(state["stage"], "stage_1")
        self.assertEqual(state["progress"], 100)

        # Check counter
        counter = agent_2.state.incr("cost_microcents", 1)
        self.assertEqual(counter, 5001)

        # Recall memory
        recalled = agent_2.memory.recall(embedding=[0.0, 0.0, 0.99, 0.05], top_k=1)
        self.assertEqual(len(recalled), 1)
        self.assertEqual(recalled[0]["id"], "persist-fact-1")
        self.assertIn("WAL prevents data loss", recalled[0]["text"])

    def test_7_error_preservation(self):
        """Verify backend error message and HTTP status preservation."""
        agent = self.db.agent("error-test-agent")

        # Invalid empty text for remember
        with self.assertRaises(AetherError) as ctx:
            agent.memory.remember(id="invalid-mem", text="", embedding=[1.0, 0.0])
        self.assertIn("Text content must be a non-empty string", str(ctx.exception))

        # Test server-side validation error propagation (e.g. empty embedding list)
        with self.assertRaises(AetherError) as ctx:
            agent.memory.recall(embedding=[])
        self.assertIn("Query embedding must be a non-empty list", str(ctx.exception))


class TestBackwardsCompatibility(unittest.TestCase):
    """Verify raw KV, atomic, and raw vector clients remain 100% functional."""

    @classmethod
    def setUpClass(cls):
        cls.endpoint = os.environ.get("AETHER_TEST_URL", "http://127.0.0.1:8301")
        cls.api_key = os.environ.get("AETHERDB_API_KEY") or os.environ.get("AETHERDB_TEST_KEY_A")
        cls.db = AetherDB(endpoint=cls.endpoint, api_key=cls.api_key)

    def test_raw_kv(self):
        k = f"raw_key_{os.getpid()}"
        self.db.set(k, {"type": "raw_kv_test", "val": 42})
        data = self.db.get(k)
        self.assertEqual(data["val"], 42)

        self.db.delete(k)
        self.assertIsNone(self.db.get(k))

    def test_raw_atomic(self):
        k = f"raw_counter_{os.getpid()}"
        val = self.db.atomic.incr(k, delta=10)
        self.assertEqual(val, 10)
        val2 = self.db.atomic.incr(k, delta=5)
        self.assertEqual(val2, 15)

    def test_raw_vector(self):
        vid = f"raw_vec_{os.getpid()}"
        self.db.vector.upsert(vid, [0.5, 0.5, 0.5, 0.5], metadata={"source": "raw"})
        res = self.db.vector.search([0.5, 0.5, 0.5, 0.5], top_k=5)
        found = [r for r in res if r["id"] == vid]
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0]["metadata"]["source"], "raw")


if __name__ == "__main__":
    unittest.main()
