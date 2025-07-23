import numpy as np
from typing import List, Sequence
from abc import ABC, abstractmethod


class EmbeddingModel(ABC):
    dim: int
    model: str

    @abstractmethod
    def encode(self, texts: Sequence[str]) -> List[np.ndarray]:
        """Return list of L2-normalised vectors."""
        raise NotImplementedError("Subclasses should implement this method.")