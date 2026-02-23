from typing import List, Dict
from qdrant_client import QdrantClient, models
from sentence_transformers import SentenceTransformer


QDRANT_URL = "http://localhost:6333"
COLLECTION_NAME = "doc_chunks"
EMBEDDING_MODEL = "sentence-transformers/all-distilroberta-v1"


class VectorStore:
    def __init__(
        self,
        qdrant_url: str = QDRANT_URL,
        collection_name: str = COLLECTION_NAME,
        embedding_model: str = EMBEDDING_MODEL,
    ):
        self.collection_name = collection_name
        self.client = QdrantClient(url=qdrant_url)
        self.model = SentenceTransformer(embedding_model)

    def ensure_collection(self) -> None:
        if self.client.collection_exists(self.collection_name):
            return

        self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config=models.VectorParams(
                size=self.model.get_sentence_embedding_dimension(),
                distance=models.Distance.COSINE,
            ),
        )

    def upsert_chunks(self, chunks: List[Dict]) -> None:
        if not chunks:
            return

        self.ensure_collection()

        texts = [c["text"] for c in chunks]
        vectors = self.model.encode(
            texts,
            normalize_embeddings=True,
            convert_to_numpy=False,
        )

        points = []
        for chunk, vector in zip(chunks, vectors):
            point_id = hash(
                f"{chunk['metadata']['doc_id']}:{chunk['metadata']['chunk_index']}"
            ) & 0x7FFFFFFFFFFFFFFF

            payload = {
                "doc_id": chunk["metadata"]["doc_id"],
                "source_path": chunk["metadata"]["source_path"],
                "chunk_index": chunk["metadata"]["chunk_index"],
                "text": chunk["text"],
            }

            points.append(
                models.PointStruct(
                    id=point_id,
                    vector=vector,
                    payload=payload
                )
            )

        self.client.upsert(
            collection_name=self.collection_name,
            points=points,
        )

    def search(self, query: str, top_k: int = 5) -> List[Dict]:
        query_vector = self.model.encode(
            query,
            normalize_embeddings=True,
            convert_to_numpy=True,
        )

        results = self.client.query_points(
            collection_name=self.collection_name,
            query=query_vector,
            limit=top_k,
            with_payload=True,
        )

        hits = []
        for p in results.points:
            payload = p.payload or {}
            hits.append(
                {

                    "text": payload.get("text", ""),
                    "doc_id": payload.get("doc_id"),
                    "source_path": payload.get("source_path"),
                    "chunk_index": payload.get("chunk_index"),
                    "score": float(p.score),

                }
            )

        return hits
