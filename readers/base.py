from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Type
import xmltodict
import json
from copy import deepcopy
import logging


class MeasurementReader(ABC):
    """ABC to store and extracts data and metadata from a specific single storage format, e.i.,
     one technique can have many MeasurementReaders, but each format (version) should have
     its own MeasurementReader"""
    def __init__(self, *paths: Path, single_file_only=True, conversion_dict=None):

        if conversion_dict is None:
            conversion_dict = {}
        self.conversion_dict = conversion_dict  # conversion rules for mapping extracted item names to mbdb item names

        self.path = paths
        if single_file_only:
            self.path = check_single(*paths)

        self.measurement_dict = {}  # where the extracted information from files are stored

    @abstractmethod
    def read(self) -> None:
        """Extracts data items and values and places them in self.measurement_dict"""

    @abstractmethod
    def to_json_storage(self) -> str:
        """Extracts data items self.measurement_dict to a json string using self.conversion_dict"""


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


def is_array_key(key: str) -> bool:
    """returns True if key ends in []"""
    return key[-2:] == '[]'


def change_array_key(key: str) -> str:
    """removes [] at end of string"""
    if is_array_key(key):
        return key[:-2]
    else:
        raise ValueError(f'{key} not ending in []')


def merge_dict(dictionary: dict, merged: dict):
    """Compares dictionary and merged; finds the level where they diverge;
    places remaining parts of dictionary within merged and returns it"""
    existing_keys = merged.keys()
    for key, value in dictionary.items():
        if key not in existing_keys:
            merged.update({key: value})
        else:
            merge_dict(value, merged[key])
    return merged


def remove_array_id(dictionary) -> None:
    """Remove [] from array keys in dictionary and convert the associated values
       into a list of values"""

    for key in list(dictionary.keys()):
        if is_array_key(key):
            dictionary[change_array_key(key)] = [dictionary.pop(key)]


def merge_dict_list(dictionary: dict, merged: dict) -> dict | None:
    """Recursively merges elements from multiple measurements"""
    if not isinstance(dictionary, dict):
        return
    logging.info(f'dict_d: {dictionary}')

    remove_array_id(merged)

    existing_keys = merged.keys()
    for key, value in dictionary.items():
        is_array = is_array_key(key)
        if is_array:
            key = change_array_key(key)
            value = [value]

        if key not in existing_keys:
            merged.update({key: value})

        else:
            if is_array:
                if value[0] not in merged[key]:
                    logging.debug(f'{key}, {value}')
                    # key exist and is an array item so the current value should be added to it
                    merged.update({key: merged[key] + value})
            else:
                # key exist but could have additional layers
                merge_dict_list(value, merged[key])

    return merged


def insert_value(dictionary: dict, value) -> None:
    """Recursively visits the values of the dictionary and replaces the
    first occurrence of '#_insert' with value"""

    for key, item_value in dictionary.items():
        if isinstance(item_value, dict):
            insert_value(item_value, value)
        if item_value == '#_insert':
            dictionary.update({key: value})
            return


def to_json_dict(measurements: dict, conversion_dict: dict) -> dict:
    """Returns the json string representation of the subset of extracted item names
       that could be mapped to mbdb item names along with the extracted values"""

    converted_measurement = {}
    for key, value in measurements.items():
        try:
            converted_key = deepcopy(conversion_dict[key])
            insert_value(converted_key, value)
            converted_measurement = merge_dict(converted_key, deepcopy(converted_measurement))
        except KeyError:
            continue

    return converted_measurement


def to_json_string(json_list: list) -> str:
    """Merges a list of measurements and converts them to a json string"""
    json_dict = {}
    for sample in json_list:
        json_dict = merge_dict_list(sample, json_dict)

    return json.dumps(json_dict, indent=2)


def xml_path_to_dict(xml_file_path: Path) -> dict:
    """Returns content of path as dictionary"""
    with open(xml_file_path, 'r') as file_in:
        xml = file_in.read()
    return xmltodict.parse(xml)
