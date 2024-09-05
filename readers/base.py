from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Type
import json

class MeasurementReader(ABC):
    """ABC to store and extracts data and metadata from a specific storage format, e.i.,
     one technique can have many MeasurementReaders, but each format (version) should have
     its own MeasurementReader"""
    def __init__(self, *paths: Path, single_file_only=True):
        self.path = paths
        if single_file_only:
            self.path = check_single(*paths)
        self.measurement_dict = {}  # where the extracted information from files are stored

    @abstractmethod
    def read(self) -> None:
        """Extracts data items and values and places them in self.measurement_dict"""

    def to_json(self) -> str:
        """Returns a json string using self.measurement_dict"""
        json_dict = {"metadata": {"method_specific_parameters":  self.measurement_dict}}
        return json.dumps(json_dict, indent=2)

class ReaderPicker(ABC):
    """ABC that picks the appropriate MeasurementReader given the supplied Path(s).
    Each technique should have exactly one ReaderPicker"""
    def __init__(self, *paths: Path, single_file_only=True):
        self.path = paths
        if single_file_only:
            self.path = check_single(*paths)
        self.readers = self._readers()
        self.picked_reader = self._pick()

    @abstractmethod
    def _readers(self) -> Dict[str, Type[MeasurementReader]]:
        """return dict of the available readers"""

    @abstractmethod
    def _pick(self) -> MeasurementReader:
        """Identifies the version of the file(s) and returns the corresponding MeasurementReader"""


def check_single(*paths: Path):
    """checks that only one Path is present and returns it,
    if multiple Paths are present a ValueError is raised"""
    try:
        path, = paths
    except ValueError:
        raise ValueError(f'only one file is allowed {len(paths)} was supplied')
    return path
