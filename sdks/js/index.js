/**
 * AetherDB Unified JavaScript / TypeScript Client SDK
 */

class AetherVectorClient {
  constructor(endpoint, headers) {
    this.endpoint = endpoint;
    this.headers = headers;
  }

  async upsert(id, vector, metadata = null) {
    const res = await fetch(`${this.endpoint}/v1/vector/upsert`, {
      method: "POST",
      headers: this.headers,
      body: JSON.stringify({
        id,
        vector,
        metadata: metadata ? (typeof metadata === "string" ? metadata : JSON.stringify(metadata)) : null,
      }),
    });
    return res.json();
  }

  async search(vector, topK = 5) {
    const res = await fetch(`${this.endpoint}/v1/vector/search`, {
      method: "POST",
      headers: this.headers,
      body: JSON.stringify({ vector, top_k: topK }),
    });
    const data = await res.json();
    return (data.results || []).map((r) => ({
      id: r.id,
      score: r.score,
      metadata: r.metadata ? JSON.parse(r.metadata) : null,
    }));
  }
}

class AetherAtomicClient {
  constructor(endpoint, headers) {
    this.endpoint = endpoint;
    this.headers = headers;
  }

  async incr(key, delta = 1) {
    const res = await fetch(`${this.endpoint}/v1/incr`, {
      method: "POST",
      headers: this.headers,
      body: JSON.stringify({ key, delta }),
    });
    const data = await res.json();
    return data.value;
  }
}

class AetherDB {
  constructor(config = {}) {
    this.endpoint = (config.endpoint || "http://127.0.0.1:8301").replace(/\/$/, "");
    this.headers = {
      "Content-Type": "application/json",
    };
    if (config.apiKey) {
      this.headers["Authorization"] = `Bearer ${config.apiKey}`;
    }

    this.vector = new AetherVectorClient(this.endpoint, this.headers);
    this.atomic = new AetherAtomicClient(this.endpoint, this.headers);
  }

  async get(key) {
    const res = await fetch(`${this.endpoint}/v1/get`, {
      method: "POST",
      headers: this.headers,
      body: JSON.stringify({ key }),
    });
    const data = await res.json();
    if (!data.found || data.value === null) return null;
    try {
      return JSON.parse(data.value);
    } catch {
      return data.value;
    }
  }

  async set(key, value) {
    const valStr = typeof value === "object" ? JSON.stringify(value) : String(value);
    const res = await fetch(`${this.endpoint}/v1/set`, {
      method: "POST",
      headers: this.headers,
      body: JSON.stringify({ key, value: valStr }),
    });
    return res.json();
  }

  async del(key) {
    const res = await fetch(`${this.endpoint}/v1/del`, {
      method: "POST",
      headers: this.headers,
      body: JSON.stringify({ key }),
    });
    return res.json();
  }

  async health() {
    const res = await fetch(`${this.endpoint}/health`);
    return res.json();
  }
}

module.exports = { AetherDB };
