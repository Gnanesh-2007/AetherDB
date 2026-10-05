# @aetherdb/sdk

> **Official JavaScript & TypeScript SDK for AetherDB**  
> *A persistent distributed state + semantic memory layer for AI agents.*

---

## 📦 Installation

```bash
npm install @aetherdb/sdk
```

---

## ⚡ Quickstart: Agent State & Semantic Memory

```typescript
import { AetherDB } from "@aetherdb/sdk";

// Initialize client with endpoint (or pass { endpoint, apiKey, timeoutMs })
const db = new AetherDB("http://localhost:8301");

// Obtain a scoped agent client
const agent = db.agent("research-agent");

// 1. Manage Agent State
await agent.state.set("session", {
  task: "database research",
  status: "running",
  step: 4,
});

const state = await agent.state.get("session");
console.log("Agent State:", state);

// 2. Store Long-Term Semantic Memory
await agent.memory.remember({
  id: "mem_001",
  text: "The user prefers Python for AI development.",
  embedding: [0.92, 0.08, 0.0, 0.0],
  metadata: { category: "preferences", confidence: 0.98 },
});

// 3. Recall Semantically Relevant Memories
const memories = await agent.memory.recall({
  query: "What language does the user prefer for AI?",
  embedding: [0.95, 0.05, 0.0, 0.0],
  topK: 5,
});

console.log("Recalled Memories:", memories);

// 4. Atomic Token Metering & Step Counters
const tokens = await agent.state.incr("tokens", 100);
console.log("Current tokens:", tokens);

// 5. Delete State
await agent.state.delete("session");
```

---

## 🛠️ API Reference

### `AetherDB`
- `new AetherDB(config?: string | AetherConfig)`
- `db.agent(agentId: string): AgentClient`
- `db.get<T>(key: string): Promise<T | null>`
- `db.set(key: string, value: any): Promise<{ status: string }>`
- `db.del(key: string): Promise<{ status: string }>`
- `db.incr(key: string, amount?: number): Promise<number>`
- `db.health(): Promise<{ status: string; engine: string; version: string }>`

### `AgentClient`
- `agent.agentId: string`
- `agent.state: AgentStateClient`
- `agent.memory: AgentMemoryClient`

### `agent.state`
- `agent.state.set<T>(key: string, state: T): Promise<{ status: string; agent_id: string }>`
- `agent.state.set<T>(state: T): Promise<{ status: string; agent_id: string }>` (Root state)
- `agent.state.get<T>(key?: string): Promise<T | null>`
- `agent.state.delete(key?: string): Promise<{ status: string; deleted: boolean; agent_id: string }>`
- `agent.state.incr(key: string, amount?: number): Promise<number>`

### `agent.memory`
- `agent.memory.remember(params: RememberParams): Promise<RememberResult>`
- `agent.memory.recall(params: RecallParams): Promise<RecallResult[]>`

---

## 🧪 Testing

```bash
npm test
npm run typecheck
```

---

## 📄 License
MIT © AetherDB Contributors
