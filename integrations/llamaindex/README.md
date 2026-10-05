# AetherDB LlamaIndex Integration

> **Persistent state and vector indexing for LlamaIndex retrieval pipelines.**

`aetherdb-llamaindex` connects **LlamaIndex** directly to **AetherDB** for persistent vector storage and structured key-value state.

---

## Installation

```bash
pip install aetherdb
pip install llama-index-core
```

---

## 1. VectorStore Indexing & Querying

```python
from aetherdb import AetherDB
from aetherdb_llamaindex import AetherDBVectorStore
from llama_index.core.schema import TextNode
from llama_index.core.vector_stores.types import VectorStoreQuery

db = AetherDB("http://127.0.0.1:8301")
vectorstore = AetherDBVectorStore(db=db, agent_id="research-agent")

# Add nodes
node = TextNode(
    text="SkipList MemTable offers lock-free concurrent writes and ordered iteration.",
    id_="node_001",
    embedding=[0.92, 0.08, 0.0, 0.0],
    metadata={"subsystem": "storage"},
)
vectorstore.add([node])

# Query nearest neighbors
query = VectorStoreQuery(
    query_embedding=[0.95, 0.05, 0.0, 0.0],
    similarity_top_k=3,
)
result = vectorstore.query(query)

for node, similarity in zip(result.nodes, result.similarities):
    print(f"[{similarity * 100:.1f}% Match] {node.node_id}: {node.get_content()}")
```

---

## 2. Key-Value Storage

```python
from aetherdb import AetherDB
from aetherdb_llamaindex import AetherDBKVStore

db = AetherDB("http://127.0.0.1:8301")
kvstore = AetherDBKVStore(db=db, agent_id="research-agent")

# Store structured pipeline state
kvstore.put("pipeline_config", {"chunk_size": 512}, collection="configs")

# Retrieve state
config = kvstore.get("pipeline_config", collection="configs")
print(config)
```
