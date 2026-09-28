import json
import urllib.request
from typing import Any, Dict, List, Optional

class AetherVectorClient:
    def __init__(self, endpoint: str, headers: Dict[str, str]):
        self.endpoint = endpoint
        self.headers = headers

    def upsert(self, id: str, vector: List[float], metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.endpoint}/v1/vector/upsert"
        payload = {
            "id": id,
            "vector": vector,
            "metadata": json.dumps(metadata) if metadata else None
        }
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers=self.headers,
            method="POST"
        )
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def search(self, vector: List[float], top_k: int = 5) -> List[Dict[str, Any]]:
        url = f"{self.endpoint}/v1/vector/search"
        payload = {"vector": vector, "top_k": top_k}
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers=self.headers,
            method="POST"
        )
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            results = []
            for r in data.get("results", []):
                meta = json.loads(r["metadata"]) if r.get("metadata") else None
                results.append({"id": r["id"], "score": r["score"], "metadata": meta})
            return results

class AetherAtomicClient:
    def __init__(self, endpoint: str, headers: Dict[str, str]):
        self.endpoint = endpoint
        self.headers = headers

    def incr(self, key: str, delta: int = 1) -> int:
        url = f"{self.endpoint}/v1/incr"
        payload = {"key": key, "delta": delta}
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers=self.headers,
            method="POST"
        )
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("value", 0)

class AetherDB:
    def __init__(self, endpoint: str = "http://127.0.0.1:8301", api_key: Optional[str] = None):
        self.endpoint = endpoint.rstrip("/")
        self.headers = {"Content-Type": "application/json"}
        if api_key:
            self.headers["Authorization"] = f"Bearer {api_key}"

        self.vector = AetherVectorClient(self.endpoint, self.headers)
        self.atomic = AetherAtomicClient(self.endpoint, self.headers)

    def get(self, key: str) -> Optional[Any]:
        url = f"{self.endpoint}/v1/get"
        req = urllib.request.Request(
            url,
            data=json.dumps({"key": key}).encode("utf-8"),
            headers=self.headers,
            method="POST"
        )
        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if not data.get("found"):
                    return None
                val = data.get("value")
                try:
                    return json.loads(val)
                except Exception:
                    return val
        except Exception:
            return None

    def set(self, key: str, value: Any) -> Dict[str, Any]:
        url = f"{self.endpoint}/v1/set"
        val_str = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        req = urllib.request.Request(
            url,
            data=json.dumps({"key": key, "value": val_str}).encode("utf-8"),
            headers=self.headers,
            method="POST"
        )
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def delete(self, key: str) -> Dict[str, Any]:
        url = f"{self.endpoint}/v1/del"
        req = urllib.request.Request(
            url,
            data=json.dumps({"key": key}).encode("utf-8"),
            headers=self.headers,
            method="POST"
        )
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def health(self) -> Dict[str, Any]:
        url = f"{self.endpoint}/health"
        req = urllib.request.Request(url, headers=self.headers, method="GET")
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8"))
