from collections.abc import Iterable
from itertools import batched
from typing import Any

import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from wikipedia import movies_documents


class Embed:
    def __init__(self, host: str = "localhost", port: int = 6333):
        self.collection_name = "documents_fr"
        # 1. Initialize the embedding model
        # Option 1: Optimized multilingual model
        self.model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

        # Option 2: CamemBERT via sentence-transformers (if available)
        # model = SentenceTransformer('dangvantuan/sentence-camembert-large')

        # 2. Initialize Qdrant client
        self.client = QdrantClient(host=host, port=port)
        # Or for Qdrant Cloud: client = QdrantClient(url="your-url", api_key="your-key")

    def create_db(self):
        # 3. Create a collection
        vector_size = self.model.get_sentence_embedding_dimension()

        if not self.client.collection_exists(self.collection_name):
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
            )

    def upsert(
        self,
        documents: Iterable[tuple[int, str, dict[str, Any]]],
        total_size: int,
        batch_size: int = 100,
    ):
        for batch in tqdm(
            batched(documents, batch_size),
            total=total_size / batch_size,
            unit=f"{batch_size} movies",
        ):
            batch = [i for i in batch if i[1] is not None]
            embeding: np.ndarray = self.model.encode([i[1] for i in batch])
            # 6. Insert into Qdrant
            points = [
                PointStruct(id=id_, vector=embeding[i], payload=payload)
                for i, (id_, text, payload) in enumerate(batch)
            ]

            self.client.upsert(
                collection_name=self.collection_name, points=points, wait=False
            )

    def query(self, query: str, score_threshold: float = 0.5):
        # 7. Semantic search
        query_vector = self.model.encode(query)

        search_result = self.client.query_points(
            collection_name=self.collection_name,
            query=query_vector.tolist(),
            limit=3,
            score_threshold=score_threshold,
        )
        return search_result


if __name__ == "__main__":
    import os

    embed = Embed(os.getenv("QDRANT_HOST", "192.168.1.35"))
    embed.create_db()
    embed.upsert(*movies_documents())
