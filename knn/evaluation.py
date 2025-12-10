# knn\evaluation.py


from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime
from sqlalchemy.orm import Session
from typing import Any, Dict, List, Tuple, Optional

from db.models import FileCollection


@dataclass
class KNNCollectionProvenance:
    """
    Describes the setup context for a set of FileCollections used in a KNN evaluation run.
    Intended for storage in FileCollection.meta.
    """
    purpose: str
    split_strategy: str
    embedding_column: str
    parents: List[str]
    split_ratio: float
    random_seed: Optional[int] = None
    extra_params: Dict[str, Any] = field(default_factory=dict)
    created_utc: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")

    def to_metadata(self) -> Dict[str, Any]:
        """Return dict suitable for FileCollection.meta"""
        return {
            "provenance": asdict(self),
            "counts": {},  # will be populated later
        }

    def to_description(self) -> str:
        train_pct = int(self.split_ratio * 100)
        test_pct = 100 - train_pct
        return f"""# Collection purpose
{self.purpose}

# Creation details
- Created: {self.created_utc}
- Split strategy: {self.split_strategy}
- Embedding column: {self.embedding_column}
- Parent tags: {', '.join(self.parents)}
- Train/Test split: {train_pct}/{test_pct}
- Random seed: {self.random_seed or 'None'}

# Notes
{self.extra_params.get('notes', '') if self.extra_params else ''}
"""


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

    def assemble_provenance_info(self, parents: List[str], embedding_col: str, split_ratio: float, per_child_cap: int, random_seed: int) -> KNNCollectionProvenance:
        """Return the provenance information for this split strategy."""
        extra_params = {
            "per_child_cap": per_child_cap,
            "notes": f"Split strategy: {self.description}"
        }
        
        prov = KNNCollectionProvenance(
            purpose=f"KNN evaluation run using {self.name}",
            split_strategy=self.name,
            embedding_column=embedding_col,
            parents=parents,
            split_ratio=split_ratio,
            random_seed=random_seed,
            extra_params=extra_params
        )
        
        return prov

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
    def filter(self,  test_file_hash: int, session: Session = None, candidate_ids: List[int] = []) -> List[int]:
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
    def infer_label(self, neighbor_tags: List[str], neighbor_scores: List[float], test_file_hash: str = None) -> str:
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