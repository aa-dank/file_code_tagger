# knn/base.py

import datetime
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Dict, Any

@dataclass
class KNNCollectionProvenance:
    """
    Describes the setup context for a set of FileCollections used in a KNN evaluation run.
    Intended for storage in FileCollection.meta.
    """
    purpose: str
    split_strategy: str                  # class or function name
    embedding_column: str
    parents: List[str]
    split_ratio: float
    random_seed: Optional[int] = None
    extra_params: Dict[str, Any] = field(default_factory=dict)
    created_utc: str = field(default_factory=lambda: datetime.datetime.utcnow().isoformat() + "Z")

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