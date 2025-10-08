# knn\evaluation.py

from abc import ABC, abstractmethod
from dataclasses import dataclass
from sqlalchemy.orm import Session
from typing import List, Tuple

from db.models import FileCollection


class SplitSelectionStrategy(ABC):
    """
    Class which contains the logic and associated metadata for selecting training and test files, 
    ostensibly for storage in FileCollection tables.
    This would be run once per evaluation run.
    """
    
    @property
    def name(self) -> str:
        """Unique identifier for this split strategy."""
        raise NotImplementedError
    
    @property
    def description(self) -> str:
        """Human-readable description of what this strategy does."""
        raise NotImplementedError

    @abstractmethod
    def select_files(self, session: Session) -> Tuple[List[int], List[int]]:
        """Return (train_ids, test_ids)."""
        raise NotImplementedError


class NeighborFilterStrategy(ABC):
    """
    Class which contains the logic and associated metadata for filtering candidate neighbors in labeling pipeline.
    ie Limits which training files are compared to a given each file.
    """
    
    @property
    def name(self) -> str:
        """Unique identifier for this filter strategy."""
        raise NotImplementedError
    
    @property
    def description(self) -> str:
        """Human-readable description of what this filter does."""
        raise NotImplementedError

    @abstractmethod
    def filter(self, session: Session, candidate_ids: List[int], test_file_id: int) -> List[int]:
        """Return subset of candidate_ids allowed for this test file."""
        raise NotImplementedError


class LabelingStrategy(ABC):
    """
    Class which contains the logic and associated metadata for inferring labels from neighbors for a given file.
    ie Logic for deciding how to combine neighbor tags/scores into a single inferred tag.
    """
    
    @property
    def name(self) -> str:
        """Unique identifier for this labeling strategy."""
        raise NotImplementedError
    
    @property
    def description(self) -> str:
        """Human-readable description of what this strategy does."""
        raise NotImplementedError

    @abstractmethod
    def infer_label(self, neighbor_tags: List[str], neighbor_scores: List[float], filepath: str = None) -> str:
        raise NotImplementedError


@dataclass
class KNNRun:
    """
    Used to track a single KNN evaluation run.
    """
    k: int
    name: str
    description: str
    training_collection: FileCollection
    test_collection: FileCollection