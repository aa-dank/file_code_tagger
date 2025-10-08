# knn/base.py

import datetime
from dataclasses import dataclass

from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field, asdict

@dataclass
class KNNCollectionProvenance:
    purpose: str
    parents: List[str]
    strategy: str
    embedding_column: str
    split_ratio: float
    per_child_cap: int
    filetype_filter: Optional[List[str]] = field(default_factory=list)
    include_descendants: bool = True
    random_seed: Optional[int] = None
    created_utc: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")

    def to_metadata(self) -> Dict[str, Any]:
        return {"provenance": asdict(self), "counts": {}}

    def to_description(self) -> str:
        train_pct = int(self.split_ratio * 100)
        test_pct = 100 - train_pct
        ft = ", ".join(self.filetype_filter) if self.filetype_filter else "all"
        return f"""# Collection purpose
{self.purpose}

# Creation details
- Created: {self.created_utc}
- Parents: {", ".join(self.parents)}
- Strategy: {self.strategy}
- Embedding: {self.embedding_column}
- Split: {train_pct}/{test_pct}
- Per-child cap: {self.per_child_cap}
- Descendants: {self.include_descendants}
- Filetypes: {ft}
- Seed: {self.random_seed}
"""