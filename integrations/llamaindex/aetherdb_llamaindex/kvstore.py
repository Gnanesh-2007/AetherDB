"""
AetherDB Key-Value Store adapter for LlamaIndex.
Provides persistent structured state storage backed by AetherDB's LSM storage engine.
"""

from typing import Any, Dict, List, Optional, Tuple, Union
import json

from llama_index.core.storage.kvstore.types import BaseKVStore

# Import AetherDB from core Python SDK
try:
    from aetherdb import AetherDB
except ImportError:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "sdks", "python")))
    from aetherdb import AetherDB


class AetherDBKVStore(BaseKVStore):
    """
    LlamaIndex BaseKVStore implementation backed by AetherDB.
    """

    def __init__(
        self,
        db: Optional[Union[AetherDB, str]] = None,
        agent_id: str = "llamaindex-store",
        endpoint: str = "http://127.0.0.1:8301",
        api_key: Optional[str] = None,
        tenant_id: Optional[str] = None,
    ):
        self.agent_id = agent_id.strip() if agent_id else "llamaindex-store"

        if isinstance(db, AetherDB):
            self.db = db
        elif isinstance(db, str):
            self.db = AetherDB(endpoint=db, api_key=api_key, tenant_id=tenant_id)
        else:
            self.db = AetherDB(endpoint=endpoint, api_key=api_key, tenant_id=tenant_id)

        self._agent = self.db.agent(self.agent_id)

    def _format_key(self, collection: str, key: str) -> str:
        return f"{collection}:{key}"

    def put(self, key: str, val: dict, collection: str = "data") -> None:
        """Store key-value pair in AetherDB."""
        formatted_key = self._format_key(collection, key)
        self._agent.state.set(formatted_key, val)

    def put_all(
        self,
        kv_pairs: List[Tuple[str, dict]],
        collection: str = "data",
        batch_size: int = 1,
    ) -> None:
        """Store batch of key-value pairs."""
        for key, val in kv_pairs:
            self.put(key, val, collection=collection)

    def get(self, key: str, collection: str = "data") -> Optional[dict]:
        """Retrieve key value from AetherDB."""
        formatted_key = self._format_key(collection, key)
        res = self._agent.state.get(formatted_key)
        if res is None:
            return None
        if isinstance(res, dict):
            return res
        if isinstance(res, str):
            try:
                return json.loads(res)
            except Exception:
                return {"value": res}
        return {"value": res}

    def get_all(self, collection: str = "data") -> Dict[str, dict]:
        """Retrieve all keys for a collection (not fully enumerable, returns empty map if unindexed)."""
        return {}

    def delete(self, key: str, collection: str = "data") -> bool:
        """Delete key from AetherDB."""
        formatted_key = self._format_key(collection, key)
        res = self._agent.state.delete(formatted_key)
        return bool(res.get("status") == "ok")

    async def aput(self, key: str, val: dict, collection: str = "data") -> None:
        self.put(key, val, collection=collection)

    async def aput_all(
        self,
        kv_pairs: List[Tuple[str, dict]],
        collection: str = "data",
        batch_size: int = 1,
    ) -> None:
        self.put_all(kv_pairs, collection=collection, batch_size=batch_size)

    async def aget(self, key: str, collection: str = "data") -> Optional[dict]:
        return self.get(key, collection=collection)

    async def aget_all(self, collection: str = "data") -> Dict[str, dict]:
        return self.get_all(collection=collection)

    async def adelete(self, key: str, collection: str = "data") -> bool:
        return self.delete(key, collection=collection)
