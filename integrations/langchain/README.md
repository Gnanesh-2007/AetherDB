# AetherDB LangChain Integration

> **Persistent state and semantic memory for LangChain AI applications.**

`aetherdb-langchain` provides thin, idiomatic adapters connecting **LangChain** directly to **AetherDB**'s high-performance LSM storage engine, hardware-atomic counters, and AVX2 SIMD HNSW vector recall.

---

## Installation

```bash
pip install aetherdb
pip install langchain-core
```

---

## 1. Conversational Memory & Chat History

Store conversation history persistently under an agent's isolated namespace:

```python
from aetherdb import AetherDB
from aetherdb_langchain import AetherDBChatMessageHistory
from langchain_core.messages import HumanMessage, AIMessage

db = AetherDB("http://127.0.0.1:8301")
history = AetherDBChatMessageHistory(agent_id="research-agent", db=db)

# Add messages
history.add_message(HumanMessage(content="Explain Raft consensus."))
history.add_message(AIMessage(content="Raft is a leader-based consensus algorithm..."))

# Retrieve messages across sessions
for msg in history.messages:
    print(f"{msg.type}: {msg.content}")
```

---

## 2. VectorStore & Semantic Retrieval

Index and recall documents using AetherDB's AVX2 SIMD vector search:

```python
from aetherdb import AetherDB
from aetherdb_langchain import AetherDBVectorStore

db = AetherDB("http://127.0.0.1:8301")
vectorstore = AetherDBVectorStore(db=db, agent_id="research-agent")

# Add documents with embeddings
vectorstore.add_texts(
    texts=["Raft provides fault-tolerant state machine replication."],
    metadatas=[{"category": "consensus"}],
    ids=["doc_001"],
    embeddings=[[0.92, 0.08, 0.0, 0.0]],
)

# Similarity search
docs_with_scores = vectorstore.similarity_search_by_vector_with_score(
    embedding=[0.95, 0.05, 0.0, 0.0],
    k=3,
)

for doc, score in docs_with_scores:
    print(f"[{score * 100:.1f}% Match] {doc.id}: {doc.page_content}")
```

---

## Multi-Agent Isolation Invariant

Memories and chat histories stored by `agent_a` cannot be accessed or searched by `agent_b`. Isolation is enforced directly by AetherDB at the storage engine level.
