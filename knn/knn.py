# knn/knn.py

from dataclasses import dataclass
from db.models import FileCollection, FileCollectionMember

@dataclass
class KNNRun:
    k: int
    name: str
    description: str
    training_collection: FileCollection
    test_collection: FileCollection

