"""
AetherDB Unified Python Client SDK
"The persistent memory and state layer for autonomous AI applications."
"""

__version__ = "0.1.0"

import json
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Union


class AetherError(Exception):
    """Base exception for all AetherDB client and server errors."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_body: Optional[Any] = None,
    ):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.response_body = response_body

    def __str__(self) -> str:
        if self.status_code is not None:
            return f"AetherError(status={self.status_code}): {self.message}"
        return f"AetherError: {self.message}"

    def __repr__(self) -> str:
        return f"AetherError(message={self.message!r}, status_code={self.status_code!r})"


class AetherTransport:
    """Centralized HTTP transport handler for AetherDB requests."""

    def __init__(
        self,
        endpoint: str = "http://127.0.0.1:8301",
        api_key: Optional[str] = None,
        tenant_id: Optional[str] = None,
        timeout: float = 10.0,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.endpoint = endpoint.rstrip("/")
        self.timeout = float(timeout)
        self.headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if headers:
            self.headers.update(headers)
        if api_key:
            self.headers["Authorization"] = f"Bearer {api_key}"
        if tenant_id:
            self.headers["X-Aether-Tenant"] = tenant_id

    def request(
        self,
        path: str,
        method: str = "GET",
        body: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        url = f"{self.endpoint}{path if path.startswith('/') else '/' + path}"
        req_headers = dict(self.headers)
        if headers:
            req_headers.update(headers)

        data_bytes = None
        if body is not None:
            if isinstance(body, (bytes, bytearray)):
                data_bytes = bytes(body)
            elif isinstance(body, str):
                data_bytes = body.encode("utf-8")
            else:
                data_bytes = json.dumps(body).encode("utf-8")

        req = urllib.request.Request(
            url,
            data=data_bytes,
            headers=req_headers,
            method=method.upper(),
        )

        req_timeout = timeout if timeout is not None else self.timeout

        try:
            with urllib.request.urlopen(req, timeout=req_timeout) as resp:
                raw_resp = resp.read().decode("utf-8")
                if not raw_resp:
                    return {}
                try:
                    return json.loads(raw_resp)
                except Exception:
                    return raw_resp
        except urllib.error.HTTPError as e:
            raw_err = ""
            parsed_err: Any = None
            try:
                raw_err = e.read().decode("utf-8")
                parsed_err = json.loads(raw_err)
            except Exception:
                pass

            err_msg = (
                parsed_err.get("error")
                if isinstance(parsed_err, dict) and "error" in parsed_err
                else raw_err or f"HTTP {e.code}: {e.reason}"
            )
            raise AetherError(
                message=err_msg,
                status_code=e.code,
                response_body=parsed_err or raw_err,
            ) from e
        except urllib.error.URLError as e:
            reason_str = str(e.reason)
            if "timed out" in reason_str.lower():
                raise AetherError(
                    f"Request timeout after {req_timeout}s to {url}",
                    status_code=408,
                ) from e
            raise AetherError(
                f"Network failure connecting to AetherDB ({url}): {e.reason}",
            ) from e
        except TimeoutError as e:
            raise AetherError(
                f"Request timeout after {req_timeout}s to {url}",
                status_code=408,
            ) from e
        except AetherError:
            raise
        except Exception as e:
            raise AetherError(f"Unexpected transport error ({url}): {e}") from e


class AetherVectorClient:
    """Vector database subsystem client for embedding indexing and similarity search."""

    def __init__(self, transport_or_endpoint: Union[AetherTransport, str], headers: Optional[Dict[str, str]] = None):
        if isinstance(transport_or_endpoint, AetherTransport):
            self.transport = transport_or_endpoint
        else:
            self.transport = AetherTransport(endpoint=transport_or_endpoint, headers=headers)
        self.endpoint = self.transport.endpoint
        self.headers = self.transport.headers

    def upsert(
        self,
        id: str,
        vector: List[float],
        metadata: Optional[Union[Dict[str, Any], str]] = None,
    ) -> Dict[str, Any]:
        if not id or not isinstance(id, str):
            raise AetherError("Vector ID must be a non-empty string")
        if not isinstance(vector, list) or len(vector) == 0:
            raise AetherError("Vector must be a non-empty list of floats")

        meta_str = json.dumps(metadata) if isinstance(metadata, (dict, list)) else metadata
        payload = {
            "id": id,
            "vector": vector,
            "metadata": meta_str,
        }
        return self.transport.request("/v1/vector/upsert", method="POST", body=payload)

    def search(
        self,
        vector: List[float],
        top_k: int = 5,
    ) -> List[Dict[str, Any]]:
        if not isinstance(vector, list) or len(vector) == 0:
            raise AetherError("Query vector must be a non-empty list of floats")

        payload = {"vector": vector, "top_k": int(top_k)}
        data = self.transport.request("/v1/vector/search", method="POST", body=payload)
        results = []
        for r in data.get("results", []):
            meta = r.get("metadata")
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except Exception:
                    pass
            results.append({
                "id": r.get("id"),
                "score": r.get("score"),
                "metadata": meta,
            })
        return results


class AetherAtomicClient:
    """Atomic hardware-native counter operations client."""

    def __init__(self, transport_or_endpoint: Union[AetherTransport, str], headers: Optional[Dict[str, str]] = None):
        if isinstance(transport_or_endpoint, AetherTransport):
            self.transport = transport_or_endpoint
        else:
            self.transport = AetherTransport(endpoint=transport_or_endpoint, headers=headers)
        self.endpoint = self.transport.endpoint
        self.headers = self.transport.headers

    def incr(self, key: str, delta: int = 1, amount: Optional[int] = None) -> int:
        if not key or not isinstance(key, str):
            raise AetherError("Key is required for atomic incr")
        amt = amount if amount is not None else delta
        payload = {"key": key, "delta": int(amt)}
        data = self.transport.request("/v1/incr", method="POST", body=payload)
        if isinstance(data, dict):
            return int(data.get("value", data.get("new_value", 0)))
        return int(data)


class AgentStateClient:
    """Agent-scoped structured persistent state and atomic counter operations."""

    def __init__(self, transport: AetherTransport, agent_id: str):
        self._transport = transport
        self._agent_id = agent_id

    @property
    def agent_id(self) -> str:
        return self._agent_id

    def set(self, key_or_state: Any, state_opt: Any = None, **kwargs: Any) -> Dict[str, Any]:
        """
        Persist structured state under the agent namespace.

        Usage:
            agent.state.set("session", {"status": "running", "step": 4})
            agent.state.set({"root_state": True})
        """
        if state_opt is not None:
            key = key_or_state
            state = state_opt
        elif "key" in kwargs and "state" in kwargs:
            key = kwargs["key"]
            state = kwargs["state"]
        elif "state" in kwargs:
            key = key_or_state if isinstance(key_or_state, str) else None
            state = kwargs["state"]
        else:
            key = None
            state = key_or_state

        if state is None:
            raise AetherError("State value must be provided to agent.state.set()")

        payload: Dict[str, Any] = {
            "agent_id": self._agent_id,
            "state": state,
        }
        if key is not None and str(key).strip():
            payload["key"] = str(key).strip()

        return self._transport.request("/v1/agent/state/set", method="POST", body=payload)

    def get(self, key: Optional[str] = None) -> Optional[Any]:
        """
        Retrieve structured state for this agent. Returns None if key not found.
        """
        payload: Dict[str, Any] = {"agent_id": self._agent_id}
        if key is not None and str(key).strip():
            payload["key"] = str(key).strip()

        data = self._transport.request("/v1/agent/state/get", method="POST", body=payload)
        if not data.get("found") or data.get("state") is None:
            return None
        return data.get("state")

    def delete(self, key: Optional[str] = None) -> Dict[str, Any]:
        """
        Delete structured state key or root state for this agent.
        """
        payload: Dict[str, Any] = {"agent_id": self._agent_id}
        if key is not None and str(key).strip():
            payload["key"] = str(key).strip()

        return self._transport.request("/v1/agent/state/delete", method="POST", body=payload)

    def del_(self, key: Optional[str] = None) -> Dict[str, Any]:
        """Alias for delete()."""
        return self.delete(key)

    def incr(self, key: str, amount: int = 1, delta: Optional[int] = None) -> int:
        """
        Atomically increment an agent-scoped integer counter (e.g. token accounting).
        """
        if not key or not isinstance(key, str) or not key.strip():
            raise AetherError("Key must be a non-empty string for agent.state.incr()")

        amt = amount if delta is None else delta
        payload = {
            "agent_id": self._agent_id,
            "key": key.strip(),
            "amount": int(amt),
        }

        data = self._transport.request("/v1/agent/state/incr", method="POST", body=payload)
        return int(data.get("value", data.get("new_value", 0)))


class AgentMemoryClient:
    """Agent-scoped long-term semantic memory operations (HNSW indexing + SIMD recall)."""

    def __init__(self, transport: AetherTransport, agent_id: str):
        self._transport = transport
        self._agent_id = agent_id

    @property
    def agent_id(self) -> str:
        return self._agent_id

    def remember(
        self,
        id: Optional[str] = None,
        text: str = "",
        embedding: Optional[List[float]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        memory_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Store a semantic memory record with embedding for this agent.

        Usage:
            agent.memory.remember(
                id="raft-001",
                text="Raft provides replicated consensus.",
                embedding=[0.12, -0.45, ...],
                metadata={"domain": "distributed-systems"}
            )
        """
        mem_id = id or memory_id or kwargs.get("memoryId")
        if not mem_id or not isinstance(mem_id, str) or not mem_id.strip():
            raise AetherError("Memory ID ('id' or 'memory_id') must be a non-empty string")
        if not text or not isinstance(text, str) or not text.strip():
            raise AetherError("Text content must be a non-empty string for agent.memory.remember()")
        if not isinstance(embedding, list) or len(embedding) == 0:
            raise AetherError("Embedding must be a non-empty list of floats for agent.memory.remember()")

        payload: Dict[str, Any] = {
            "agent_id": self._agent_id,
            "memory_id": mem_id.strip(),
            "text": text,
            "embedding": embedding,
        }
        if metadata is not None:
            payload["metadata"] = metadata

        data = self._transport.request("/v1/agent/memory/remember", method="POST", body=payload)
        return {
            "status": data.get("status", "ok"),
            "agent_id": self._agent_id,
            "memory_id": mem_id.strip(),
            "id": mem_id.strip(),
        }

    def recall(
        self,
        embedding: List[float],
        top_k: int = 5,
        query: Optional[str] = None,
        topK: Optional[int] = None,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """
        Perform sub-millisecond SIMD cosine similarity search strictly scoped to this agent.

        Usage:
            results = agent.memory.recall(
                embedding=[0.11, -0.44, ...],
                top_k=5,
            )
        """
        if not isinstance(embedding, list) or len(embedding) == 0:
            raise AetherError("Query embedding must be a non-empty list of floats for agent.memory.recall()")

        k = topK if topK is not None else top_k
        payload: Dict[str, Any] = {
            "agent_id": self._agent_id,
            "embedding": embedding,
            "top_k": int(k),
        }
        if query:
            payload["query"] = str(query)

        data = self._transport.request("/v1/agent/memory/recall", method="POST", body=payload)
        raw_results = data.get("results", [])
        formatted = []
        for r in raw_results:
            mid = r.get("memory_id", "")
            formatted.append({
                "id": mid,
                "memory_id": mid,
                "score": float(r.get("score", 0.0)),
                "text": r.get("text", ""),
                "metadata": r.get("metadata"),
            })
        return formatted


class AgentClient:
    """Unified client scoped to a single autonomous AI agent."""

    def __init__(self, transport: AetherTransport, agent_id: str):
        if not agent_id or not isinstance(agent_id, str) or not agent_id.strip():
            raise AetherError("Invalid agent_id. Must be a non-empty string <= 256 chars.")
        if len(agent_id.strip()) > 256:
            raise AetherError("Invalid agent_id. Length must not exceed 256 chars.")

        self._agent_id = agent_id.strip()
        self._transport = transport
        self.state = AgentStateClient(transport, self._agent_id)
        self.memory = AgentMemoryClient(transport, self._agent_id)

    @property
    def agent_id(self) -> str:
        """The immutable agent ID for this client instance."""
        return self._agent_id

    def __repr__(self) -> str:
        return f"AgentClient(agent_id={self._agent_id!r})"


class AetherDB:
    """
    AetherDB Primary Client.
    Provides agent-native state & semantic memory, raw KV, vector index, and atomic counter APIs.
    """

    def __init__(
        self,
        endpoint: str = "http://127.0.0.1:8301",
        api_key: Optional[str] = None,
        tenant_id: Optional[str] = None,
        timeout: float = 10.0,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.transport = AetherTransport(
            endpoint=endpoint,
            api_key=api_key,
            tenant_id=tenant_id,
            timeout=timeout,
            headers=headers,
        )
        self.endpoint = self.transport.endpoint
        self.headers = self.transport.headers

        self.vector = AetherVectorClient(self.transport)
        self.atomic = AetherAtomicClient(self.transport)

    def agent(self, agent_id: str) -> AgentClient:
        """Create an AgentClient scoped to the given agent ID."""
        return AgentClient(self.transport, agent_id)

    def get(self, key: str) -> Optional[Any]:
        """Raw KV point lookup. Returns None if key not found."""
        if not key:
            raise AetherError("Key must be a non-empty string")
        data = self.transport.request("/v1/get", method="POST", body={"key": key})
        if not data.get("found"):
            return None
        val = data.get("value")
        if val is None:
            return None
        if isinstance(val, str):
            try:
                return json.loads(val)
            except Exception:
                return val
        return val

    def set(self, key: str, value: Any) -> Dict[str, Any]:
        """Raw KV set."""
        if not key:
            raise AetherError("Key must be a non-empty string")
        val_str = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        return self.transport.request("/v1/set", method="POST", body={"key": key, "value": val_str})

    def delete(self, key: str) -> Dict[str, Any]:
        """Raw KV delete."""
        if not key:
            raise AetherError("Key must be a non-empty string")
        return self.transport.request("/v1/del", method="POST", body={"key": key})

    def del_(self, key: str) -> Dict[str, Any]:
        """Alias for delete()."""
        return self.delete(key)

    def incr(self, key: str, delta: int = 1) -> int:
        """Global atomic counter increment."""
        return self.atomic.incr(key, delta=delta)

    def health(self) -> Dict[str, Any]:
        """Check cluster and storage engine health status."""
        return self.transport.request("/health", method="GET")

    def __repr__(self) -> str:
        return f"AetherDB(endpoint={self.endpoint!r})"


# Backwards compatibility alias
AetherClient = AetherDB

__all__ = [
    "AetherDB",
    "AetherClient",
    "AetherError",
    "AetherTransport",
    "AetherVectorClient",
    "AetherAtomicClient",
    "AgentClient",
    "AgentStateClient",
    "AgentMemoryClient",
]
