#!/usr/bin/env python3
"""
AetherDB v0.1.0 - Public Deployment Validation Suite.

Runs against a deployed HTTPS endpoint. Credentials are read from the environment
and are never printed:

    AETHERDB_TEST_KEY_A   API key for tenant A (aether_sk_<tenantA>_<secret>)
    AETHERDB_TEST_KEY_B   API key for tenant B (aether_sk_<tenantB>_<secret>)

Usage:
    python scripts/validate_public_deployment.py https://<host> [--phase write|verify|all]

`--phase write` stores restart-persistence fixtures, `--phase verify` reads them back
(run it after restarting the server). Default `all` runs the functional suite + write.
"""

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "sdks", "python"))
sys.path.insert(0, os.path.join(REPO_ROOT, "integrations", "langchain"))
sys.path.insert(0, os.path.join(REPO_ROOT, "integrations", "llamaindex"))

from aetherdb import AetherDB  # noqa: E402

KEY_A = os.environ.get("AETHERDB_TEST_KEY_A", "")
KEY_B = os.environ.get("AETHERDB_TEST_KEY_B", "")
RESULTS = []
LATENCIES = []


def tenant_of(key):
    return key.split("_")[2]


def record(name, ok, detail=""):
    RESULTS.append((name, ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {name:<58} {detail}", flush=True)


def http(url, method="GET", headers=None, data=None, timeout=20):
    headers = dict(headers or {})
    body = None
    if data is not None:
        body = json.dumps(data).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            out = (r.status, dict(r.headers), r.read().decode())
    except urllib.error.HTTPError as e:
        out = (e.code, dict(e.headers), e.read().decode())
    except Exception as e:  # network errors
        out = (0, {}, str(e))
    LATENCIES.append((time.perf_counter() - t0) * 1000)
    return out


def auth(key):
    return {"Authorization": f"Bearer {key}"}


def functional(url):
    # Health / readiness
    c, _, b = http(f"{url}/health")
    record("GET /health", c == 200 and "healthy" in b, f"HTTP {c}")
    c, _, b = http(f"{url}/readiness")
    record("GET /readiness", c == 200 and '"ready":true' in b, f"HTTP {c}")

    # Authentication
    probe = {"agent_id": "probe", "key": "k"}
    c, _, _ = http(f"{url}/v1/agent/state/get", "POST", data=probe)
    record("Auth: no credentials -> 401", c == 401, f"HTTP {c}")
    c, _, _ = http(f"{url}/v1/agent/state/get", "POST",
                   {"Authorization": "Bearer aether_sk_tenantA_forged"}, probe)
    record("Auth: forged well-formed key -> 401", c == 401, f"HTTP {c}")
    c, _, _ = http(f"{url}/v1/agent/state/get", "POST", {"X-Aether-Tenant": tenant_of(KEY_A)}, probe)
    record("Auth: X-Aether-Tenant header alone -> 401", c == 401, f"HTTP {c}")
    c, _, _ = http(f"{url}/v1/agent/state/get", "POST",
                   {**auth(KEY_A), "X-Aether-Tenant": tenant_of(KEY_B)}, probe)
    record("Auth: key + mismatched tenant header -> 401", c == 401, f"HTTP {c}")
    c, _, _ = http(f"{url}/v1/agent/state/get", "POST",
                   {**auth(KEY_A), "X-Aether-Tenant": tenant_of(KEY_A)}, probe)
    record("Auth: valid key (+ matching tenant header) -> 200", c == 200, f"HTTP {c}")

    # Tenant isolation: same agent id in both tenants
    for key, goal in ((KEY_A, "tenant-A-only"), (KEY_B, "tenant-B-only")):
        http(f"{url}/v1/agent/state/set", "POST", auth(key),
             {"agent_id": "research-agent", "key": "plan", "state": {"goal": goal}})
    _, _, ba = http(f"{url}/v1/agent/state/get", "POST", auth(KEY_A), {"agent_id": "research-agent", "key": "plan"})
    _, _, bb = http(f"{url}/v1/agent/state/get", "POST", auth(KEY_B), {"agent_id": "research-agent", "key": "plan"})
    record("Tenant isolation: state", "tenant-A-only" in ba and "tenant-B" not in ba
           and "tenant-B-only" in bb and "tenant-A" not in bb)

    va, vb = [1.0] + [0.0] * 7, [0.0, 1.0] + [0.0] * 6
    http(f"{url}/v1/agent/memory/remember", "POST", auth(KEY_A),
         {"agent_id": "research-agent", "memory_id": "mem-A", "text": "alpha memo", "embedding": va})
    http(f"{url}/v1/agent/memory/remember", "POST", auth(KEY_B),
         {"agent_id": "research-agent", "memory_id": "mem-B", "text": "beta memo", "embedding": vb})
    _, _, ra = http(f"{url}/v1/agent/memory/recall", "POST", auth(KEY_A),
                    {"agent_id": "research-agent", "embedding": vb, "top_k": 10})
    record("Tenant isolation: memory recall", "mem-B" not in ra and "beta memo" not in ra)

    http(f"{url}/v1/vector/upsert", "POST", auth(KEY_B), {"id": "vec-B", "vector": vb})
    _, _, rv = http(f"{url}/v1/vector/search", "POST", auth(KEY_A), {"vector": vb, "top_k": 10})
    record("Tenant isolation: raw vector search", "vec-B" not in rv)

    # Agent isolation within one tenant
    http(f"{url}/v1/agent/memory/remember", "POST", auth(KEY_A),
         {"agent_id": "agent-x", "memory_id": "mem-X", "text": "x only", "embedding": va})
    _, _, ry = http(f"{url}/v1/agent/memory/recall", "POST", auth(KEY_A),
                    {"agent_id": "agent-y", "embedding": va, "top_k": 10})
    record("Agent isolation: agent-y cannot recall agent-x", "mem-X" not in ry)

    # Python SDK full lifecycle
    try:
        db = AetherDB(endpoint=url, api_key=KEY_A, timeout=20)
        ag = db.agent("sdk-agent")
        ag.state.set("session", {"task": "distributed systems research", "status": "running"})
        assert ag.state.get("session")["status"] == "running"
        t1 = ag.state.incr("tokens", 150)
        t2 = ag.state.incr("tokens", 150)
        assert t2 == t1 + 150
        emb = [0.12] * 16
        ag.memory.remember(id="deployment-test-001",
                           text="AetherDB provides persistent semantic memory for autonomous agents.",
                           embedding=emb, metadata={"source": "public-deployment-test"})
        res = ag.memory.recall(query="What does AetherDB provide?", embedding=emb, top_k=5)
        assert res and res[0].get("memory_id") == "deployment-test-001"
        ag.state.delete("session")
        assert ag.state.get("session") is None
        record("Python SDK: SET/GET/INCR/DELETE/remember/recall", True)
    except Exception as e:
        record("Python SDK: SET/GET/INCR/DELETE/remember/recall", False, repr(e)[:120])

    # LangChain
    try:
        from aetherdb_langchain import AetherDBChatMessageHistory
        h = AetherDBChatMessageHistory(agent_id="lc-agent", session_key="s1", endpoint=url, api_key=KEY_A)
        h.clear()
        h.add_user_message("hello")
        h.add_ai_message("hi")
        h2 = AetherDBChatMessageHistory(agent_id="lc-agent", session_key="s1", endpoint=url, api_key=KEY_A)
        other = AetherDBChatMessageHistory(agent_id="lc-other", session_key="s1", endpoint=url, api_key=KEY_A)
        ok = [m.content for m in h2.messages] == ["hello", "hi"] and len(other.messages) == 0
        record("LangChain: chat history persistence + agent isolation", ok)
    except Exception as e:
        record("LangChain: chat history persistence + agent isolation", False, repr(e)[:120])

    # LlamaIndex
    try:
        from aetherdb_llamaindex import AetherDBKVStore
        kv = AetherDBKVStore(agent_id="li-agent", endpoint=url, api_key=KEY_A)
        kv.put("idx-001", {"type": "summary"})
        kv2 = AetherDBKVStore(agent_id="li-other", endpoint=url, api_key=KEY_A)
        ok = (kv.get("idx-001") or {}).get("type") == "summary" and kv2.get("idx-001") is None
        record("LlamaIndex: KV store persistence + agent isolation", ok)
    except Exception as e:
        record("LlamaIndex: KV store persistence + agent isolation", False, repr(e)[:120])

    # JavaScript SDK (key passed via env, not embedded in script source)
    js_path = os.path.join(REPO_ROOT, "sdks", "js", "index.js").replace("\\", "/")
    js = f"""
    const {{ AetherDB }} = require('{js_path}');
    (async () => {{
      const db = new AetherDB(process.env.TARGET_URL, {{ apiKey: process.env.AETHERDB_TEST_KEY_A, timeoutMs: 20000 }});
      const a = db.agent('js-agent');
      await a.state.set('task', {{ goal: 'js' }});
      if ((await a.state.get('task')).goal !== 'js') throw new Error('state');
      await a.memory.remember({{ id: 'js-mem', text: 'js memory', embedding: [0.3,0.1,0.2,0.4] }});
      const r = await a.memory.recall({{ embedding: [0.3,0.1,0.2,0.4], topK: 3 }});
      if (!r.length || r[0].id !== 'js-mem') throw new Error('recall');
      console.log('JS_OK');
    }})().catch(e => {{ console.error(String(e)); process.exit(1); }});
    """
    p = subprocess.run(["node", "-e", js], capture_output=True, text=True, timeout=90,
                       env={**os.environ, "TARGET_URL": url})
    record("JavaScript SDK: state + memory", "JS_OK" in p.stdout, (p.stderr or "")[:120])

    # Metrics must be private under strict auth
    c, _, b = http(f"{url}/metrics")
    record("Metrics: not publicly exposed (strict mode)", c == 401, f"HTTP {c}")

    # CORS: no wildcard
    _, h, _ = http(f"{url}/health")
    acao = {k.lower(): v for k, v in h.items()}.get("access-control-allow-origin")
    record("CORS: no wildcard Access-Control-Allow-Origin", acao != "*", f"ACAO={acao}")


def write_fixtures(url):
    db = AetherDB(endpoint=url, api_key=KEY_A, timeout=20)
    ag = db.agent("restart-agent")
    ag.state.set("checkpoint", {"step": 42})
    tokens = ag.state.incr("tokens", 777)
    ag.memory.remember(id="restart-mem", text="survives restarts", embedding=[0.9, 0.1, 0.0, 0.2])
    http(f"{url}/v1/vector/upsert", "POST", auth(KEY_A), {"id": "restart-vec", "vector": [0.2, 0.8, 0.1, 0.0]})
    with open(os.path.join(os.path.dirname(__file__), ".restart_expect.json"), "w") as f:
        json.dump({"tokens": tokens}, f)
    print(f"[INFO] restart fixtures written (tokens={tokens})")


def verify_fixtures(url):
    exp = json.load(open(os.path.join(os.path.dirname(__file__), ".restart_expect.json")))
    db = AetherDB(endpoint=url, api_key=KEY_A, timeout=20)
    ag = db.agent("restart-agent")
    record("Restart: state survives", (ag.state.get("checkpoint") or {}).get("step") == 42)
    cur = ag.state.incr("tokens", 0)
    record("Restart: counter survives", cur == exp["tokens"], f"{cur} vs {exp['tokens']}")
    res = ag.memory.recall(embedding=[0.9, 0.1, 0.0, 0.2], top_k=3)
    record("Restart: memory survives", bool(res) and res[0].get("memory_id") == "restart-mem")
    _, _, b = http(f"{url}/v1/vector/search", "POST", auth(KEY_A), {"vector": [0.2, 0.8, 0.1, 0.0], "top_k": 3})
    record("Restart: vector survives", "restart-vec" in b)


def main():
    if len(sys.argv) < 2 or not KEY_A or not KEY_B:
        print(__doc__)
        sys.exit(2)
    url = sys.argv[1].rstrip("/")
    phase = sys.argv[sys.argv.index("--phase") + 1] if "--phase" in sys.argv else "all"
    print(f"Target: {url}  phase={phase}")
    if phase in ("all",):
        functional(url)
    if phase in ("all", "write"):
        write_fixtures(url)
    if phase == "verify":
        verify_fixtures(url)
    if LATENCIES:
        s = sorted(LATENCIES)
        print(f"[INFO] {len(s)} raw HTTP calls  p50={s[len(s)//2]:.1f}ms  p95={s[int(len(s)*0.95)-1]:.1f}ms  max={s[-1]:.1f}ms")
    passed = sum(1 for _, ok in RESULTS if ok)
    print(f"SUMMARY: {passed}/{len(RESULTS)} passed")
    sys.exit(0 if passed == len(RESULTS) else 1)


if __name__ == "__main__":
    main()
