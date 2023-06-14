import logging

from readers.base import MeasurementReader, ReaderPicker, to_json_dict, to_json_string, xml_path_to_dict
from pathlib import Path
from typing import Dict, Type, List
import numpy as np
from base64 import b64decode

octet_conversion_dict = {'FlowRate':
                            {'method_specific_parameters':
                                 {'measurement_protocol[]':
                                      {'shaking_speed': {'value': '#_insert',
                                                         'unit': 'RPM'}}}},
                         'ActualTime':
                             {'method_specific_parameters':
                                  {'measurement_protocol[]':
                                       {'time_length': {'value': '#_insert',
                                                       'unit': 'seconds'}}}},
                         'StartTime':
                             {'method_specific_parameters':
                                  {'measurement_protocol[]':
                                       {'start_time': {'value': '#_insert'}}}},
                         'StepName':
                             {'method_specific_parameters':
                                  {'measurement_protocol[]':
                                       {'name': '#_insert'}}},
                         'StepType':
                             {'method_specific_parameters':
                                  {'measurement_protocol[]':
                                       {'type': '#_insert'}}},
                         'AssayXData':
                             {'method_specific_parameters':
                                  {'measurements[]':
                                       {'measured_data':
                                            {'time': {'values': '#_insert',
                                                      'unit': 'seconds'}
                                                 }}}},
                         'AssayYData':
                             {'method_specific_parameters':
                                  {'measurements[]':
                                       {'measured_data':
                                            {'response': {'values': '#_insert',
                                                          'unit': 'nm'}
                                             }}}},

                         }


class BLIPicker(ReaderPicker):
    """Picks the correct MeasurementReader for an BLI file"""
    def __init__(self, *paths: Path):
        super().__init__(*paths, single_file_only=False)

    def _readers(self) -> Dict[str, Type[MeasurementReader]]:
        return {'octet': BLIReaderOctet,
                }

    def _pick(self) -> MeasurementReader:
        for path in self.path:
            if path.suffix != '.frd':
                raise ValueError(f'only raw BLI Octet data files (.frd) are allowed')

        return self.readers['octet']


class BLIReaderOctet(MeasurementReader):
    def __init__(self, *paths: Path):
        super().__init__(*paths, single_file_only=False, conversion_dict=octet_conversion_dict)
        self._check_data_files()

    def read(self) -> None:
        for i, path in enumerate(self.path):
            self.measurement_dict.update({i: self._read_frd(path)})

    def to_json_storage(self) -> str:
        return bli_storage_json(self)

    def _check_data_files(self) -> None:
        """Checks that data files are in the right format and that they can be treated as a
        single measurement"""
        run_ids = []
        for path in self.path:
            frd_dict = xml_path_to_dict(path)

            # RunIDs are assumed to be UUIDs and unique on the run level, i.e. all files from single run has the
            # same identical RunID, and no runs
            run_id = frd_dict['ExperimentResults']['ExperimentInfo']['RunID']
            run_ids.append(run_id)

        # TODO: if data files are from different runs, check that a compatible protocol was
        #       used in all files
        if len(np.unique(run_ids)) != 1:
            print("WARNING, the files originate from multiple runs")

    def _read_frd(self, path: Path) -> dict:
        """Reads and decodes the raw data from .brd files"""

        measurement_dict = xml_path_to_dict(path)
        xy_keys = ('AssayXData', 'AssayYData')
        for step in measurement_dict['ExperimentResults']['KineticsData']['Step']:
            logging.debug(step)
            for key in xy_keys:
                # raw data is assumed to be stored as a string of base64 encoded 32 bit floating point numbers
                step.update({key: self._b64_to_float(step[key]['#text'])})

            for key, value in step.pop('CommonData').items():
                step.update({key: value})

        return measurement_dict

    @staticmethod
    def _b64_to_float(data_string: str) -> List[float]:
        """Interprets data string as an array of base64 encoded 32bit floating point numbers"""
        data_string = data_string.replace('\n', '')
        data_array = np.frombuffer(b64decode(data_string), dtype=np.float32)
        # convert to json serializable types (list, float) rather than (array, np.float32)
        return list(data_array.astype(float))


def bli_storage_json(reader: MeasurementReader) -> str:
    """Returns the json string representation of the subset of extracted item names
       that could be mapped to mbdb item names along with the extracted values"""

    json_list = []

    for sensor_output in reader.measurement_dict.values():
        for step in sensor_output['ExperimentResults']['KineticsData']['Step']:

            json_list.append(to_json_dict(step, reader.conversion_dict))

    return to_json_string(json_list)

