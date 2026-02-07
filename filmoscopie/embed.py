from collections.abc import Iterable
import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams
from sentence_transformers import SentenceTransformer


class Embed:
    def __init__(self, address: str = "localhost:6333"):
        self.collection_name = "documents_fr"
        # 1. Initialize the embedding model
        # Option 1: Optimized multilingual model
        self.model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

        # Option 2: CamemBERT via sentence-transformers (if available)
        # model = SentenceTransformer('dangvantuan/sentence-camembert-large')

        # 2. Initialize Qdrant client
        host, port = address.split(":")
        self.client = QdrantClient(host=host, port=int(port))
        # Or for Qdrant Cloud: client = QdrantClient(url="your-url", api_key="your-key")

    def create_db(self):
        # 3. Create a collection
        vector_size = self.model.get_sentence_embedding_dimension()

        self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
        )

    def upsert(self, documents: Iterable[str]):
        # 4. Prepare your documents
        """
        documents = [
            "La tour Eiffel est un monument emblématique de Paris.",
            "Le fromage français est réputé dans le monde entier.",
            "L'intelligence artificielle transforme notre société.",
        ]
        """

        # 5. Generate embeddings
        embeddings = self.model.encode(documents)

        # 6. Insert into Qdrant
        points = [
            PointStruct(id=idx, vector=embedding.tolist(), payload={"text": doc})
            for idx, (doc, embedding) in enumerate(zip(documents, embeddings))
        ]

        self.client.upsert(collection_name=self.collection_name, points=points)

    def query(self, query: str):
        # 7. Semantic search
        """
        query = "monuments parisiens"
        """
        query_vector = self.model.encode(query)
        print(query_vector)

        search_result = self.client.query_points(
            collection_name=self.collection_name,
            query=query_vector.tolist(),
            limit=3,
        )
        return search_result

        """
        for result in search_result:
            print(f"Score: {result.score:.4f} - {result.payload['text']}")
        """


if __name__ == "__main__":
    embed = Embed("192.168.1.35:6333")
    """
    embed.create_db()
    documents = [
        "La tour Eiffel est un monument emblématique de Paris.",
        "Le fromage français est réputé dans le monde entier.",
        "L'intelligence artificielle transforme notre société.",
    ]
    embed.upsert(documents)
    """
    print(embed.query("monuments parisiens"))
