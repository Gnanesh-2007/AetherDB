"""
AetherDB VectorStore adapter for LlamaIndex.
Enables LlamaIndex indices and retrievers to route through AetherDB's HNSW vector indexing and AVX2 SIMD similarity engine.
"""

from typing import Any, List, Optional
from pydantic import PrivateAttr

from llama_index.core.schema import BaseNode, TextNode
from llama_index.core.vector_stores.types import (
    BasePydanticVectorStore,
    VectorStoreQuery,
    VectorStoreQueryResult,
)

# Import AetherDB from core Python SDK
try:
    from aetherdb import AetherDB
except ImportError:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "sdks", "python")))
    from aetherdb import AetherDB


class AetherDBVectorStore(BasePydanticVectorStore):
    """
    LlamaIndex vector store implementation backed by AetherDB.

    If `agent_id` is supplied, all operations are isolated to that specific agent's namespace.
    """

    endpoint: str = "http://127.0.0.1:8301"
    agent_id: Optional[str] = None
    api_key: Optional[str] = None
    tenant_id: Optional[str] = None
    stores_text: bool = True
    is_embedding_query: bool = True

    _client: Optional[AetherDB] = PrivateAttr(default=None)

    def __init__(
        self,
        endpoint: str = "http://127.0.0.1:8301",
        agent_id: Optional[str] = None,
        api_key: Optional[str] = None,
        tenant_id: Optional[str] = None,
        db: Optional[AetherDB] = None,
        **kwargs: Any,
    ):
        super().__init__(
            endpoint=endpoint,
            agent_id=agent_id.strip() if agent_id else None,
            api_key=api_key,
            tenant_id=tenant_id,
            **kwargs,
        )
        if db is not None:
            self._client = db
        else:
            self._client = AetherDB(
                endpoint=self.endpoint,
                api_key=self.api_key,
                tenant_id=self.tenant_id,
            )

    @property
    def client(self) -> AetherDB:
        """Return the underlying AetherDB client."""
        if self._client is None:
            self._client = AetherDB(
                endpoint=self.endpoint,
                api_key=self.api_key,
                tenant_id=self.tenant_id,
            )
        return self._client

    def get_nodes(
        self,
        node_ids: Optional[List[str]] = None,
        filters: Optional[Any] = None,
    ) -> List[BaseNode]:
        """Fetch nodes by node_ids if supported."""
        if not node_ids:
            return []
        nodes = []
        for nid in node_ids:
            if self.agent_id:
                state = self.client.agent(self.agent_id).state.get(f"node:{nid}")
                if state and isinstance(state, dict):
                    nodes.append(TextNode(
                        text=state.get("text", ""),
                        id_=nid,
                        metadata=state.get("metadata") or {},
                    ))
        return nodes

    def add(
        self,
        nodes: List[BaseNode],
        **add_kwargs: Any,
    ) -> List[str]:
        """
        Add nodes and embeddings to AetherDB.
        """
        node_ids = []
        for node in nodes:
            nid = node.node_id
            text = node.get_content()
            emb = node.embedding
            if emb is None:
                raise ValueError(f"Node '{nid}' does not have an embedding.")

            meta = dict(node.metadata) if node.metadata else {}

            if self.agent_id:
                # Store semantic memory record
                self.client.agent(self.agent_id).memory.remember(
                    id=nid,
                    text=text,
                    embedding=emb,
                    metadata=meta,
                )
                # Store node state for direct lookups
                self.client.agent(self.agent_id).state.set(f"node:{nid}", {
                    "text": text,
                    "metadata": meta,
                })
            else:
                meta["text"] = text
                self.client.vector.upsert(
                    id=nid,
                    vector=emb,
                    metadata=meta,
                )
            node_ids.append(nid)

        return node_ids

    def query(
        self,
        query: VectorStoreQuery,
        **kwargs: Any,
    ) -> VectorStoreQueryResult:
        """
        Query nearest neighbors using AetherDB HNSW vector index + AVX2 SIMD recall.
        """
        if query.query_embedding is None:
            raise ValueError("Query embedding is required for AetherDBVectorStore.query()")

        top_k = query.similarity_top_k or 5

        if self.agent_id:
            results = self.client.agent(self.agent_id).memory.recall(
                embedding=query.query_embedding,
                top_k=top_k,
            )
            nodes = []
            similarities = []
            ids = []
            for r in results:
                nodes.append(TextNode(
                    text=r.get("text", ""),
                    id_=r.get("id"),
                    metadata=r.get("metadata") or {},
                ))
                similarities.append(float(r.get("score", 0.0)))
                ids.append(r.get("id"))

            return VectorStoreQueryResult(nodes=nodes, similarities=similarities, ids=ids)
        else:
            results = self.client.vector.search(vector=query.query_embedding, top_k=top_k)
            nodes = []
            similarities = []
            ids = []
            for r in results:
                meta = r.get("metadata") or {}
                text = meta.get("text", "") if isinstance(meta, dict) else ""
                nodes.append(TextNode(
                    text=text,
                    id_=r.get("id"),
                    metadata=meta if isinstance(meta, dict) else {},
                ))
                similarities.append(float(r.get("score", 0.0)))
                ids.append(r.get("id"))

            return VectorStoreQueryResult(nodes=nodes, similarities=similarities, ids=ids)

    def delete(self, ref_doc_id: str, **delete_kwargs: Any) -> None:
        """Delete document/node from store if supported."""
        if self.agent_id:
            self.client.agent(self.agent_id).state.delete(f"node:{ref_doc_id}")
        else:
            self.client.delete(f"vec:{ref_doc_id}")
