"""
Security, Multi-Tenant Partitioning, and Multi-Agent Isolation Audit Tests for AetherDB.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from aetherdb import AetherDB, AetherError


class TestSecurityAndNamespaceIsolation(unittest.TestCase):
    """Rigorous verification of tenant boundaries, agent isolation, and input validation."""

    @classmethod
    def setUpClass(cls):
        cls.endpoint = os.environ.get("AETHER_TEST_URL", "http://127.0.0.1:8301")
        cls.key_a = os.environ.get("AETHERDB_TEST_KEY_A") or os.environ.get("AETHERDB_API_KEY")
        cls.key_b = os.environ.get("AETHERDB_TEST_KEY_B") or os.environ.get("AETHERDB_API_KEY")
        cls.db = AetherDB(endpoint=cls.endpoint, api_key=cls.key_a)

    def test_multi_tenant_and_multi_agent_isolation_matrix(self):
        """
        Verify the 4-quadrant isolation matrix:
        - Tenant-A / Agent-A
        - Tenant-A / Agent-B
        - Tenant-B / Agent-A
        - Tenant-B / Agent-B
        """
        tenant_a = self.key_a.split("_")[2] if self.key_a and len(self.key_a.split("_")) >= 3 else "tenant-alpha"
        tenant_b = self.key_b.split("_")[2] if self.key_b and len(self.key_b.split("_")) >= 3 else "tenant-beta"
        db_tenant_a = AetherDB(endpoint=self.endpoint, tenant_id=tenant_a, api_key=self.key_a)
        db_tenant_b = AetherDB(endpoint=self.endpoint, tenant_id=tenant_b, api_key=self.key_b)

        agent_a1 = db_tenant_a.agent("agent-one")
        agent_a2 = db_tenant_a.agent("agent-two")
        agent_b1 = db_tenant_b.agent("agent-one")
        agent_b2 = db_tenant_b.agent("agent-two")

        # 1. State Isolation
        agent_a1.state.set("secret", {"val": "TENANT_A_AGENT_1_SECRET"})
        agent_a2.state.set("secret", {"val": "TENANT_A_AGENT_2_SECRET"})
        agent_b1.state.set("secret", {"val": "TENANT_B_AGENT_1_SECRET"})
        agent_b2.state.set("secret", {"val": "TENANT_B_AGENT_2_SECRET"})

        self.assertEqual(agent_a1.state.get("secret")["val"], "TENANT_A_AGENT_1_SECRET")
        self.assertEqual(agent_a2.state.get("secret")["val"], "TENANT_A_AGENT_2_SECRET")
        self.assertEqual(agent_b1.state.get("secret")["val"], "TENANT_B_AGENT_1_SECRET")
        self.assertEqual(agent_b2.state.get("secret")["val"], "TENANT_B_AGENT_2_SECRET")

        # 2. Semantic Memory Isolation
        emb = [1.0, 0.0, 0.0, 0.0]
        agent_a1.memory.remember(id="mem_1", text="Alpha One Knowledge", embedding=emb)
        agent_b1.memory.remember(id="mem_1", text="Beta One Knowledge", embedding=emb)

        recall_a1 = agent_a1.memory.recall(embedding=emb, top_k=5)
        recall_b1 = agent_b1.memory.recall(embedding=emb, top_k=5)
        recall_a2 = agent_a2.memory.recall(embedding=emb, top_k=5)

        self.assertEqual(len(recall_a1), 1)
        self.assertEqual(recall_a1[0]["text"], "Alpha One Knowledge")

        self.assertEqual(len(recall_b1), 1)
        self.assertEqual(recall_b1[0]["text"], "Beta One Knowledge")

        # Agent A2 in Tenant A should NOT see Agent A1's memory
        self.assertEqual(len(recall_a2), 0)

    def test_input_validation_and_boundary_guards(self):
        """Test rejection of malformed agent IDs, oversized payloads, and invalid vectors."""
        # Empty agent ID
        with self.assertRaises(AetherError):
            self.db.agent("")

        # Oversized agent ID (> 256 chars)
        with self.assertRaises(AetherError):
            self.db.agent("x" * 257)

        agent = self.db.agent("valid-agent")

        # Empty text
        with self.assertRaises(AetherError):
            agent.memory.remember(id="m1", text="", embedding=[1.0, 0.0])

        # Empty vector
        with self.assertRaises(AetherError):
            agent.memory.remember(id="m1", text="hello", embedding=[])

        # Empty query vector
        with self.assertRaises(AetherError):
            agent.memory.recall(embedding=[])

    def test_readiness_and_prometheus_metrics_endpoints(self):
        """Verify /readiness and /metrics return valid responses without leaking sensitive data."""
        import urllib.request

        # 1. Readiness
        req_ready = urllib.request.Request(f"{self.endpoint}/readiness")
        with urllib.request.urlopen(req_ready) as resp:
            self.assertEqual(resp.status, 200)
            data = resp.read().decode("utf-8")
            self.assertIn('"status":"ready"', data)
            self.assertIn('"ready":true', data)

        # 2. Prometheus Metrics
        headers = {}
        if self.key_a:
            headers["Authorization"] = f"Bearer {self.key_a}"
        req_metrics = urllib.request.Request(f"{self.endpoint}/metrics", headers=headers)
        with urllib.request.urlopen(req_metrics) as resp:
            self.assertEqual(resp.status, 200)
            text = resp.read().decode("utf-8")
            self.assertIn("# TYPE aetherdb_requests_total counter", text)
            self.assertIn("aetherdb_requests_total{node_id=\"1\"}", text)
            self.assertIn("aetherdb_storage_bytes{node_id=\"1\"", text)
            self.assertNotIn("Bearer", text)
            self.assertNotIn("secret", text.lower())


if __name__ == "__main__":
    unittest.main()
