from typing import Generic
from ray.data.dataset import Dataset

class JoinedDataset(Generic(T)):

    def __init__(self, left: Dataset, left_keys: List[str], right:Datastream[], right_key:Datastream[T]):
        
