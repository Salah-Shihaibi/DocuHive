import fitz
from abc import ABC, abstractmethod
from typing import List, Optional
from dataclasses import dataclass
import numpy as np


class State(dict):
    """dot.notation access to dictionary attributes"""

    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


@dataclass
class TagData:
    tag_name: str
    data_value: str
    polygons: Optional[List[List[int]]] = None


class DocDataExtractor(ABC):
    def __init__(self, doc_id, job_id=None):
        self.debug_data: State = State()
        self.doc_id: int = doc_id
        self.job_id: int = job_id

    @abstractmethod
    def run(
        self,
        doc: fitz.Document,
        combined_image: np.ndarray,
        file_name: str,
        page_dimensions: str,
        debug: bool,
    ) -> Optional[List[TagData]]:
        pass

    @abstractmethod
    def debug(self, debug_tags: list) -> np.ndarray:
        pass


def transformed_data_for_storage(image_sections) -> List[TagData]:
    transformed_data = []
    for key, value in image_sections.items():
        polygons = value.get("extra_polygons", [])
        if value.get("polygon"):
            polygons.append(value["polygon"])
        tag_data = TagData(key, value["text"], polygons)
        transformed_data.append(tag_data)
    return transformed_data
