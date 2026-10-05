"""
AetherDB VectorStore adapter for LangChain.
Enables LangChain retrieval pipelines to leverage AetherDB's HNSW vector indexing and AVX2 SIMD similarity search.
"""

from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Type, Union
import uuid

from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore

# Import AetherDB from core Python SDK
try:
    from aetherdb import AetherDB
except ImportError:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "sdks", "python")))
    from aetherdb import AetherDB


class AetherDBVectorStore(VectorStore):
    """
    LangChain VectorStore implementation backed by AetherDB.

    When `agent_id` is supplied, all memories are strictly scoped and isolated
    under that agent's namespace. Otherwise, operations route to global vector storage.
    """

    def __init__(
        self,
        embedding_function: Optional[Union[Embeddings, Callable[[str], List[float]]]] = None,
        db: Optional[Union[AetherDB, str]] = None,
        agent_id: Optional[str] = None,
        endpoint: str = "http://127.0.0.1:8301",
        api_key: Optional[str] = None,
        tenant_id: Optional[str] = None,
    ):
        self._embedding_function = embedding_function
        self.agent_id = agent_id.strip() if agent_id else None

        if isinstance(db, AetherDB):
            self.db = db
        elif isinstance(db, str):
            self.db = AetherDB(endpoint=db, api_key=api_key, tenant_id=tenant_id)
        else:
            self.db = AetherDB(endpoint=endpoint, api_key=api_key, tenant_id=tenant_id)

    @property
    def embeddings(self) -> Optional[Union[Embeddings, Callable[[str], List[float]]]]:
        return self._embedding_function

    def _embed_text(self, text: str) -> List[float]:
        if self._embedding_function is None:
            raise ValueError(
                "Embedding function is required to embed text. "
                "Provide an Embeddings instance or pass pre-computed vectors to similarity_search_by_vector."
            )
        if hasattr(self._embedding_function, "embed_query"):
            return self._embedding_function.embed_query(text)
        elif callable(self._embedding_function):
            return self._embedding_function(text)
        else:
            raise ValueError("Unsupported embedding_function type")

    def _embed_texts(self, texts: List[str]) -> List[List[float]]:
        if self._embedding_function is None:
            raise ValueError("Embedding function is required to embed texts.")
        if hasattr(self._embedding_function, "embed_documents"):
            return self._embedding_function.embed_documents(texts)
        elif callable(self._embedding_function):
            return [self._embedding_function(t) for t in texts]
        else:
            raise ValueError("Unsupported embedding_function type")

    def add_texts(
        self,
        texts: Iterable[str],
        metadatas: Optional[List[dict]] = None,
        ids: Optional[List[str]] = None,
        embeddings: Optional[List[List[float]]] = None,
        **kwargs: Any,
    ) -> List[str]:
        """
        Add texts and their embeddings to AetherDB.
        """
        text_list = list(texts)
        if not text_list:
            return []

        if embeddings is None:
            embeddings = self._embed_texts(text_list)

        if len(embeddings) != len(text_list):
            raise ValueError("Number of embeddings must match number of texts")

        doc_ids = ids if ids is not None else [str(uuid.uuid4()) for _ in text_list]
        meta_list = metadatas if metadatas is not None else [{} for _ in text_list]

        for doc_id, text, emb, meta in zip(doc_ids, text_list, embeddings, meta_list):
            if self.agent_id:
                self.db.agent(self.agent_id).memory.remember(
                    id=str(doc_id),
                    text=text,
                    embedding=emb,
                    metadata=meta,
                )
            else:
                meta_dict = dict(meta) if isinstance(meta, dict) else {}
                meta_dict["text"] = text
                self.db.vector.upsert(
                    id=str(doc_id),
                    vector=emb,
                    metadata=meta_dict,
                )

        return doc_ids

    def similarity_search_by_vector_with_score(
        self,
        embedding: List[float],
        k: int = 4,
        **kwargs: Any,
    ) -> List[Tuple[Document, float]]:
        """
        Perform sub-millisecond similarity search using AetherDB's AVX2 SIMD HNSW vector engine.
        """
        if self.agent_id:
            results = self.db.agent(self.agent_id).memory.recall(
                embedding=embedding,
                top_k=k,
                query=kwargs.get("query"),
            )
            docs_with_scores = []
            for r in results:
                doc = Document(
                    page_content=r.get("text", ""),
                    metadata=r.get("metadata") or {},
                    id=r.get("id"),
                )
                score = float(r.get("score", 0.0))
                docs_with_scores.append((doc, score))
            return docs_with_scores
        else:
            results = self.db.vector.search(vector=embedding, top_k=k)
            docs_with_scores = []
            for r in results:
                meta = r.get("metadata") or {}
                text = meta.get("text", "") if isinstance(meta, dict) else ""
                doc = Document(
                    page_content=text,
                    metadata=meta if isinstance(meta, dict) else {},
                    id=r.get("id"),
                )
                score = float(r.get("score", 0.0))
                docs_with_scores.append((doc, score))
            return docs_with_scores

    def similarity_search_by_vector(
        self,
        embedding: List[float],
        k: int = 4,
        **kwargs: Any,
    ) -> List[Document]:
        """Return docs most similar to embedding vector."""
        docs_and_scores = self.similarity_search_by_vector_with_score(embedding, k=k, **kwargs)
        return [doc for doc, _ in docs_and_scores]

    def similarity_search(
        self,
        query: str,
        k: int = 4,
        **kwargs: Any,
    ) -> List[Document]:
        """Return docs most similar to text query."""
        emb = self._embed_text(query)
        return self.similarity_search_by_vector(emb, k=k, query=query, **kwargs)

    def similarity_search_with_score(
        self,
        query: str,
        k: int = 4,
        **kwargs: Any,
    ) -> List[Tuple[Document, float]]:
        """Return docs and similarity scores for query."""
        emb = self._embed_text(query)
        return self.similarity_search_by_vector_with_score(emb, k=k, query=query, **kwargs)

    @classmethod
    def from_texts(
        cls: Type["AetherDBVectorStore"],
        texts: List[str],
        embedding: Embeddings,
        metadatas: Optional[List[dict]] = None,
        ids: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> "AetherDBVectorStore":
        """Construct AetherDBVectorStore from texts."""
        store = cls(embedding_function=embedding, **kwargs)
        store.add_texts(texts=texts, metadatas=metadatas, ids=ids)
        return store

    @classmethod
    def from_documents(
        cls: Type["AetherDBVectorStore"],
        documents: List[Document],
        embedding: Embeddings,
        ids: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> "AetherDBVectorStore":
        """Construct AetherDBVectorStore from Documents."""
        texts = [d.page_content for d in documents]
        metadatas = [d.metadata for d in documents]
        doc_ids = ids if ids is not None else [d.id for d in documents if d.id is not None]
        if len(doc_ids) != len(documents):
            doc_ids = ids
        return cls.from_texts(
            texts=texts,
            embedding=embedding,
            metadatas=metadatas,
            ids=doc_ids,
            **kwargs,
        )
