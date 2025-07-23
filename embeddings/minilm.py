from .base import EmbeddingModel
from sentence_transformers import SentenceTransformer
import numpy as np

class MiniLMEmbedder(EmbeddingModel):
    def __init__(self):
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        self.dim = self.model.get_sentence_embedding_dimension()

    def encode(self, texts):
        embeddings = self.model.encode(texts, normalize_embeddings=True)
        # Ensure output is a list of np.ndarray
        return [np.asarray(vec) for vec in embeddings]