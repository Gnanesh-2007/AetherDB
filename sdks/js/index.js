/**
 * AetherDB Unified JavaScript / TypeScript Client SDK
 * "A persistent distributed state + semantic memory layer for AI agents."
 */

class AetherError extends Error {
  constructor(message, statusCode = null, responseBody = null) {
    super(message);
    this.name = "AetherError";
    this.statusCode = statusCode;
    this.responseBody = responseBody;
  }
}

class AetherTransport {
  constructor(config = {}, options = {}) {
    if (typeof config === "string") {
      config = { endpoint: config, ...options };
    }
    this.endpoint = (config.endpoint || "http://127.0.0.1:8301").replace(/\/$/, "");
    this.timeoutMs = config.timeoutMs || config.timeout || 10000;
    this.headers = {
      "Content-Type": "application/json",
      ...(config.headers || {}),
    };
    const apiKey =
      config.apiKey ||
      (typeof process !== "undefined" && process.env
        ? process.env.AETHERDB_API_KEY || process.env.AETHER_API_KEY
        : undefined);
    const tenantId =
      config.tenantId ||
      config.tenant_id ||
      (typeof process !== "undefined" && process.env
        ? process.env.AETHERDB_TENANT || process.env.AETHER_TENANT_ID
        : undefined);

    if (apiKey) {
      this.headers["Authorization"] = `Bearer ${apiKey}`;
    }
    if (tenantId) {
      this.headers["X-Aether-Tenant"] = tenantId;
    }
  }

  async request(path, options = {}) {
    const url = `${this.endpoint}${path.startsWith("/") ? path : `/${path}`}`;
    const controller = new AbortController();
    const timeoutMs = options.timeoutMs || this.timeoutMs;
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    const fetchOptions = {
      method: options.method || "GET",
      headers: {
        ...this.headers,
        ...(options.headers || {}),
      },
      signal: controller.signal,
    };

    if (options.body !== undefined) {
      fetchOptions.body = typeof options.body === "string" ? options.body : JSON.stringify(options.body);
    }

    try {
      const res = await fetch(url, fetchOptions);
      clearTimeout(timer);

      let data;
      const text = await res.text();
      try {
        data = text ? JSON.parse(text) : {};
      } catch {
        data = text;
      }

      if (!res.ok) {
        const errMsg =
          data && typeof data === "object" && data.error
            ? data.error
            : `HTTP ${res.status}: ${res.statusText}`;
        throw new AetherError(errMsg, res.status, data);
      }

      return data;
    } catch (err) {
      clearTimeout(timer);
      if (err.name === "AbortError") {
        throw new AetherError(`Request timeout after ${timeoutMs}ms to ${url}`, 408);
      }
      if (err instanceof AetherError) {
        throw err;
      }
      throw new AetherError(`Network failure connecting to AetherDB (${url}): ${err.message}`);
    }
  }
}

class AetherVectorClient {
  constructor(transport) {
    this.transport = transport;
  }

  async upsert(id, vector, metadata = null) {
    if (!id) throw new AetherError("Vector ID is required");
    if (!Array.isArray(vector) || vector.length === 0) {
      throw new AetherError("Non-empty float vector array is required");
    }
    return this.transport.request("/v1/vector/upsert", {
      method: "POST",
      body: {
        id,
        vector,
        metadata: metadata ? (typeof metadata === "string" ? metadata : JSON.stringify(metadata)) : null,
      },
    });
  }

  async search(vector, topK = 5) {
    if (!Array.isArray(vector) || vector.length === 0) {
      throw new AetherError("Query vector array is required");
    }
    const data = await this.transport.request("/v1/vector/search", {
      method: "POST",
      body: { vector, top_k: topK },
    });
    return (data.results || []).map((r) => ({
      id: r.id,
      score: r.score,
      metadata: r.metadata ? (typeof r.metadata === "string" ? JSON.parse(r.metadata) : r.metadata) : null,
    }));
  }
}

class AetherAtomicClient {
  constructor(transport) {
    this.transport = transport;
  }

  async incr(key, amount = 1) {
    if (!key) throw new AetherError("Key is required for atomic incr");
    const delta = typeof amount === "number" ? amount : 1;
    const data = await this.transport.request("/v1/incr", {
      method: "POST",
      body: { key, amount: delta },
    });
    return data.value !== undefined ? data.value : data.new_value;
  }
}

class AgentStateClient {
  constructor(transport, agentId) {
    this.transport = transport;
    this.agentId = agentId;
  }

  async set(keyOrState, stateOpt) {
    let key, state;
    if (stateOpt !== undefined) {
      key = keyOrState;
      state = stateOpt;
    } else {
      key = undefined;
      state = keyOrState;
    }

    if (state === undefined) {
      throw new AetherError("State value must be provided to agent.state.set()");
    }

    const payload = {
      agent_id: this.agentId,
      state: state,
    };
    if (key !== undefined) {
      payload.key = key;
    }

    return this.transport.request("/v1/agent/state/set", {
      method: "POST",
      body: payload,
    });
  }

  async get(key) {
    const payload = {
      agent_id: this.agentId,
    };
    if (key !== undefined) {
      payload.key = key;
    }

    const data = await this.transport.request("/v1/agent/state/get", {
      method: "POST",
      body: payload,
    });

    if (!data.found || data.state === null || data.state === undefined) {
      return null;
    }
    return data.state;
  }

  async delete(key) {
    const payload = {
      agent_id: this.agentId,
    };
    if (key !== undefined) {
      payload.key = key;
    }

    return this.transport.request("/v1/agent/state/delete", {
      method: "POST",
      body: payload,
    });
  }

  async del(key) {
    return this.delete(key);
  }

  async incr(key, amount = 1) {
    if (!key) {
      throw new AetherError("Key is required for agent.state.incr()");
    }
    const delta = typeof amount === "number" ? amount : 1;
    const payload = {
      agent_id: this.agentId,
      key: key,
      amount: delta,
    };

    const data = await this.transport.request("/v1/agent/state/incr", {
      method: "POST",
      body: payload,
    });

    return data.value !== undefined ? data.value : data.new_value;
  }
}

class AgentMemoryClient {
  constructor(transport, agentId) {
    this.transport = transport;
    this.agentId = agentId;
  }

  async remember(params) {
    if (!params || typeof params !== "object") {
      throw new AetherError("Params object required for agent.memory.remember()");
    }
    const memoryId = params.id || params.memory_id || params.memoryId;
    if (!memoryId) {
      throw new AetherError("Memory ID ('id' or 'memory_id') is required for agent.memory.remember()");
    }
    if (!params.text) {
      throw new AetherError("Text content is required for agent.memory.remember()");
    }
    if (!Array.isArray(params.embedding) || params.embedding.length === 0) {
      throw new AetherError("Non-empty float embedding array is required for agent.memory.remember()");
    }

    const payload = {
      agent_id: this.agentId,
      memory_id: memoryId,
      text: params.text,
      embedding: params.embedding,
    };
    if (params.metadata !== undefined) {
      payload.metadata = params.metadata;
    }

    const data = await this.transport.request("/v1/agent/memory/remember", {
      method: "POST",
      body: payload,
    });

    return {
      status: data.status || "ok",
      agentId: this.agentId,
      memoryId: memoryId,
      id: memoryId,
    };
  }

  async recall(params) {
    if (!params || typeof params !== "object") {
      throw new AetherError("Params object required for agent.memory.recall()");
    }
    if (!Array.isArray(params.embedding) || params.embedding.length === 0) {
      throw new AetherError("Query embedding array is required for agent.memory.recall()");
    }

    const payload = {
      agent_id: this.agentId,
      embedding: params.embedding,
      top_k: params.topK || params.top_k || 5,
    };
    if (params.query) {
      payload.query = params.query;
    }

    const data = await this.transport.request("/v1/agent/memory/recall", {
      method: "POST",
      body: payload,
    });

    const results = data.results || [];
    return results.map((r) => ({
      id: r.memory_id,
      memoryId: r.memory_id,
      score: r.score,
      text: r.text,
      metadata: r.metadata || null,
    }));
  }
}

class AgentClient {
  constructor(transport, agentId) {
    if (!agentId || typeof agentId !== "string" || !agentId.trim()) {
      throw new AetherError("Invalid agentId. Must be a non-empty string.");
    }
    this.agentId = agentId.trim();
    this.transport = transport;
    this.state = new AgentStateClient(transport, this.agentId);
    this.memory = new AgentMemoryClient(transport, this.agentId);
  }
}

class AetherDB {
  constructor(config = {}, options = {}) {
    this.transport = new AetherTransport(config, options);
    this.endpoint = this.transport.endpoint;
    this.headers = this.transport.headers;

    this.vector = new AetherVectorClient(this.transport);
    this.atomic = new AetherAtomicClient(this.transport);
  }

  agent(agentId) {
    return new AgentClient(this.transport, agentId);
  }

  async get(key) {
    const data = await this.transport.request("/v1/get", {
      method: "POST",
      body: { key },
    });
    if (!data.found || data.value === null) return null;
    try {
      return JSON.parse(data.value);
    } catch {
      return data.value;
    }
  }

  async set(key, value) {
    const valStr = typeof value === "object" ? JSON.stringify(value) : String(value);
    return this.transport.request("/v1/set", {
      method: "POST",
      body: { key, value: valStr },
    });
  }

  async del(key) {
    return this.transport.request("/v1/del", {
      method: "POST",
      body: { key },
    });
  }

  async incr(key, amount = 1) {
    return this.atomic.incr(key, amount);
  }

  async health() {
    return this.transport.request("/health", {
      method: "GET",
    });
  }
}

module.exports = {
  AetherDB,
  AetherError,
  AetherTransport,
  AetherVectorClient,
  AetherAtomicClient,
  AgentClient,
  AgentStateClient,
  AgentMemoryClient,
};
