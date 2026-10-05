/**
 * AetherDB Phase 3: Persistent Memory Agent Demo Web Server
 *
 * Serves the Dark-Mode Developer Console and exposes clean RPC endpoints
 * powered by the AetherDB SDK.
 */

const http = require("http");
const fs = require("fs");
const path = require("path");
const { PersistentMemoryAgent } = require("./agent");

const PORT = process.env.PORT || 3000;
const DB_ADDR = process.env.AETHER_ADDR || "http://127.0.0.1:8301";

// Cache of active agent instances
const agents = new Map();

function getAgent(agentId = "demo-agent-alpha") {
  if (!agents.has(agentId)) {
    agents.set(agentId, new PersistentMemoryAgent(agentId, DB_ADDR));
  }
  return agents.get(agentId);
}

const server = http.createServer(async (req, res) => {
  // CORS Headers
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    res.end();
    return;
  }

  const url = new URL(req.url, `http://${req.headers.host}`);

  // Serve static UI
  if (req.method === "GET" && (url.pathname === "/" || url.pathname === "/index.html")) {
    const htmlPath = path.join(__dirname, "index.html");
    fs.readFile(htmlPath, (err, data) => {
      if (err) {
        res.writeHead(500, { "Content-Type": "text/plain" });
        res.end("Error loading index.html");
      } else {
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
        res.end(data);
      }
    });
    return;
  }

  // Health check endpoint
  if (req.method === "GET" && url.pathname === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", demoServer: true, port: PORT }));
    return;
  }

  // Handle JSON API endpoints
  if (req.method === "POST" && url.pathname.startsWith("/api/agent/")) {
    let bodyText = "";
    req.on("data", (chunk) => {
      bodyText += chunk;
    });

    req.on("end", async () => {
      let body = {};
      try {
        if (bodyText) body = JSON.parse(bodyText);
      } catch (e) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Invalid JSON body" }));
        return;
      }

      const agentId = body.agentId || "demo-agent-alpha";
      const agent = getAgent(agentId);

      try {
        switch (url.pathname) {
          case "/api/agent/init": {
            const state = await agent.initializeState();
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ status: "ok", state }));
            break;
          }

          case "/api/agent/state": {
            const state = await agent.getState();
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ status: "ok", state }));
            break;
          }

          case "/api/agent/update-state": {
            const state = await agent.updateState(body.state || {});
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ status: "ok", state }));
            break;
          }

          case "/api/agent/remember": {
            const { id, text, metadata } = body;
            const result = await agent.remember(id, text, metadata || {});
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify(result));
            break;
          }

          case "/api/agent/remember-batch": {
            const facts = body.facts || [];
            const results = [];
            for (const f of facts) {
              const r = await agent.remember(f.id, f.text, { domain: f.domain });
              results.push(r);
            }
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ status: "ok", count: results.length, results }));
            break;
          }

          case "/api/agent/recall": {
            const { query, topK } = body;
            const results = await agent.recall(query || "", topK || 5);
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ status: "ok", query, results }));
            break;
          }

          case "/api/agent/incr-tokens": {
            const amount = typeof body.amount === "number" ? body.amount : 1;
            const tokens = await agent.consumeTokens(amount);
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ status: "ok", tokens }));
            break;
          }

          case "/api/agent/restart": {
            // Discard in-memory instance from map to simulate crash/restart
            agents.delete(agentId);
            const { agent: restartedAgent, restoredState, tokenCount } = await PersistentMemoryAgent.simulateRestart(agentId, DB_ADDR);
            agents.set(agentId, restartedAgent);

            // Fetch memories via broad recall to re-populate UI
            const memories = await restartedAgent.recall("general information preferences skills projects", 10);

            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(
              JSON.stringify({
                status: "ok",
                restarted: true,
                state: restoredState,
                tokens: tokenCount,
                memories: memories.map((m) => ({
                  id: m.memoryId || m.id,
                  text: m.text,
                  domain: m.metadata?.domain || "persisted",
                })),
              })
            );
            break;
          }

          case "/api/agent/reset": {
            await agent.clearSession();
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ status: "ok", reset: true }));
            break;
          }

          default:
            res.writeHead(404, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ error: `Not found: ${url.pathname}` }));
        }
      } catch (err) {
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: err.message, statusCode: err.statusCode }));
      }
    });
    return;
  }

  res.writeHead(404, { "Content-Type": "text/plain" });
  res.end("Not Found");
});

if (require.main === module) {
  server.listen(PORT, () => {
    console.log(`⚡ AetherDB AI Agent Demo Console running at http://localhost:${PORT}`);
    console.log(`🔗 Backed by AetherDB Node at ${DB_ADDR}`);
  });
}

module.exports = { server, getAgent };
