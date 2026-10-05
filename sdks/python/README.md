# AetherDB Python SDK

> **Persistent memory and state layer for autonomous AI applications.**

The official Python client SDK for **AetherDB** provides an agent-native interface to manage structured persistent state, atomic execution counters, and long-term semantic memory (HNSW vector indexing + AVX2 SIMD cosine recall).

---

## Installation

```bash
pip install .
```

Or add `aetherdb` to your `requirements.txt` / `pyproject.toml`.

---

## Quickstart

```python
from aetherdb import AetherDB

# Connect to AetherDB server
db = AetherDB("http://127.0.0.1:8301")

# Check cluster node health
health = db.health()
print("Cluster health:", health["status"])

# Initialize an agent client handle
agent = db.agent("research-agent")
```

---

## 1. Structured Persistent State (`agent.state`)

Store, retrieve, and delete arbitrary JSON-serializable context for an agent:

```python
# Set structured state
agent.state.set("session", {
    "task": "Distributed systems research",
    "status": "running",
    "step": 4,
    "focus": ["Multi-Raft", "MVCC", "HNSW"],
})

# Retrieve state
state = agent.state.get("session")
print("Agent session state:", state)

# Delete state key
agent.state.delete("session")
```

---

## 2. Hardware-Atomic Counters (`agent.state.incr`)

Track token usage, cost microcents, rate limits, or loop steps with lock-free atomic increments:

```python
# Increment token count by 150
tokens = agent.state.incr("tokens", 150)
print(f"Total tokens consumed: {tokens}")

# Step turn counter by default delta (+1)
turns = agent.state.incr("step")
```

---

## 3. Semantic Memory & Vector Recall (`agent.memory`)

Ingest long-term knowledge with vector embeddings, and recall context using sub-millisecond SIMD similarity search:

```python
# 1. Ingest knowledge into persistent memory
agent.memory.remember(
    id="raft-001",
    text="Raft algorithm guarantees state machine replication across distributed nodes.",
    embedding=[0.92, 0.08, 0.0, 0.0],
    metadata={"domain": "consensus", "difficulty": "advanced"}
)

# 2. Recall relevant memories with query embedding
results = agent.memory.recall(
    query="How does Raft ensure consensus?",
    embedding=[0.95, 0.05, 0.0, 0.0],
    top_k=5
)

for memory in results:
    print(f"[{memory['score'] * 100:.1f}% Match] {memory['id']}: {memory['text']}")
    print(f"Metadata: {memory['metadata']}")
```

---

## 4. Multi-Tenant & API Key Configuration

Pass tenant identifiers and API keys during client initialization:

```python
db = AetherDB(
    endpoint="http://127.0.0.1:8301",
    api_key="your_api_key_here",
    tenant_id="tenant-production-alpha",
    timeout=10.0  # seconds
)
```

Requests will automatically propagate `Authorization: Bearer <api_key>` and `X-Aether-Tenant: <tenant_id>` headers.

---

## 5. Error Handling

AetherDB raises structured `AetherError` exceptions preserving HTTP status codes and backend error diagnostics:

```python
from aetherdb import AetherDB, AetherError

db = AetherDB("http://127.0.0.1:8301")
agent = db.agent("researcher")

try:
    agent.memory.remember(id="mem_1", text="", embedding=[1.0, 0.0])
except AetherError as e:
    print(f"Status Code: {e.status_code}")
    print(f"Error Message: {e.message}")
```

---

## Running Tests

```bash
pytest sdks/python/tests/test_agent_sdk.py -v
```
