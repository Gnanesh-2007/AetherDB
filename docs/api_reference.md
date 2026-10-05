# AetherDB API Reference Specification

> **Version:** `0.1.0`  
> **Protocols:** HTTP REST (Port 8301) / Custom Binary TCP (Port 8300)

---

## 1. Authentication & Tenant Headers

| Header | Type | Description |
| :--- | :--- | :--- |
| `Authorization` | `Bearer aether_sk_<tenant>_<token>` | API key authentication. |
| `X-Aether-Tenant` | `<tenant_id>` | Tenant partition identifier for multi-tenant isolation. |
| `Content-Type` | `application/json` | Required for all JSON POST payloads. |

---

## 2. Health & Observability Endpoints

### `GET /health`
Liveness check.
```json
{
  "status": "healthy",
  "engine": "aetherdb-rust",
  "version": "0.1.0"
}
```

### `GET /readiness`
Readiness probe verifying the node can accept read/write traffic.
```json
{
  "status": "ready",
  "node_id": 1,
  "ready": true,
  "engine": "aetherdb-rust",
  "version": "0.1.0"
}
```

### `GET /metrics`
Prometheus standard text metrics output.
```text
# HELP aetherdb_requests_total Total number of HTTP requests processed by AetherDB.
# TYPE aetherdb_requests_total counter
aetherdb_requests_total{node_id="1"} 142
...
```

### `GET /v1/telemetry`
JSON metrics telemetry snapshot for dashboards and devtools.

---

## 3. Agent-Native State Endpoints

### `POST /v1/agent/state/set`
Persists structured context under the agent's namespace.
- **Request Body:**
  ```json
  {
    "agent_id": "research-agent",
    "key": "session",
    "state": { "task": "distributed-systems", "step": 4 }
  }
  ```
- **Response:**
  ```json
  { "status": "ok", "agent_id": "research-agent" }
  ```

### `POST /v1/agent/state/get`
Retrieves agent state by subkey.
- **Request Body:**
  ```json
  { "agent_id": "research-agent", "key": "session" }
  ```
- **Response:**
  ```json
  {
    "found": true,
    "agent_id": "research-agent",
    "state": { "task": "distributed-systems", "step": 4 }
  }
  ```

### `POST /v1/agent/state/delete`
Deletes an agent state subkey.
- **Request Body:**
  ```json
  { "agent_id": "research-agent", "key": "session" }
  ```
- **Response:**
  ```json
  { "status": "ok", "deleted": true, "agent_id": "research-agent" }
  ```

### `POST /v1/agent/state/incr`
Atomically increments an agent-scoped integer counter (e.g. LLM token metering).
- **Request Body:**
  ```json
  { "agent_id": "research-agent", "key": "tokens", "amount": 150 }
  ```
- **Response:**
  ```json
  {
    "status": "ok",
    "agent_id": "research-agent",
    "key": "tokens",
    "value": 150,
    "new_value": 150
  }
  ```

---

## 4. Agent Semantic Memory Endpoints

### `POST /v1/agent/memory/remember`
Stores an episodic or semantic memory record with vector embeddings.
- **Request Body:**
  ```json
  {
    "agent_id": "research-agent",
    "memory_id": "raft-001",
    "text": "Raft provides replicated state machine consensus.",
    "embedding": [0.92, 0.08, 0.0, 0.0],
    "metadata": { "domain": "consensus", "confidence": 0.99 }
  }
  ```
- **Response:**
  ```json
  { "status": "ok", "agent_id": "research-agent", "memory_id": "raft-001" }
  ```

### `POST /v1/agent/memory/recall`
Performs sub-millisecond AVX2 SIMD cosine similarity search strictly scoped to the target agent.
- **Request Body:**
  ```json
  {
    "agent_id": "research-agent",
    "embedding": [0.95, 0.05, 0.0, 0.0],
    "top_k": 5,
    "query": "How does Raft maintain consistency?"
  }
  ```
- **Response:**
  ```json
  {
    "agent_id": "research-agent",
    "results": [
      {
        "memory_id": "raft-001",
        "score": 0.9994,
        "text": "Raft provides replicated state machine consensus.",
        "metadata": { "domain": "consensus", "confidence": 0.99 }
      }
    ]
  }
  ```

---

## 5. Raw Key-Value & SIMD Vector Endpoints

| Endpoint | Method | Payload | Description |
| :--- | :---: | :--- | :--- |
| `/v1/get` | `POST` | `{"key": "string"}` | Raw point lookup |
| `/v1/set` | `POST` | `{"key": "string", "value": "any"}` | Raw KV set |
| `/v1/del` | `POST` | `{"key": "string"}` | Raw KV delete |
| `/v1/incr` | `POST` | `{"key": "string", "delta": 1}` | Global atomic counter increment |
| `/v1/vector/upsert` | `POST` | `{"id": "string", "vector": [...], "metadata": ...}` | Global HNSW vector insertion |
| `/v1/vector/search` | `POST` | `{"vector": [...], "top_k": 5}` | Global AVX2 SIMD cosine search |
