from readers.base import MeasurementReader, ReaderPicker, to_json_dict, to_json_string
from pathlib import Path
from typing import Dict, Type
import sqlite3
import pandas as pd
import numpy as np

MST_conversion_dict = { 'MST-Power': {
                          'method_specific_parameters': {
                            'ir_mst_laser_power': '#_insert'}},
                        'Excitation-Power': {
                          'method_specific_parameters': {
                            'excitation_led_power': '#_insert'}},
                        'Capillary Position': {
                          'method_specific_parameters': {
                            'measurements[]':
                                {'position': '#_insert'}}},
                        'Ligand Concentration': {
                          'method_specific_parameters': {
                            'measurements[]': {
                              'sample': {
                                'ligands': {
                                  'concentration':
                                      {'value': '#_insert'}}}}}},
                        'TargetConcentration': {
                          'method_specific_parameters': {
                            'measurements[]': {
                              'sample': {
                                'targets': {
                                  'concentration':
                                      {'value': '#_insert'}}}}}},
                        'Time [s]': {
                          'method_specific_parameters': {
                            'measurements[]': {
                              'measured_data': {
                                'x_data': {'values': '#_insert',
                                           'unit': 'seconds'}}}}},
                        'Raw Fluorescence [counts]': {
                          'method_specific_parameters': {
                            'measurements[]': {
                              'measured_data': {
                                'y_data': {'values': '#_insert',
                                           'unit': 'counts'}}}}}
                        }


class MSTPicker(ReaderPicker):
    """Picks the correct (MST) MeasurementReader for an MST file"""
    def __init__(self, *paths: Path):
        super().__init__(*paths, single_file_only=True)

    def _readers(self) -> Dict[str, Type[MeasurementReader]]:
        return {'.moc': MSTReaderMOC,
                '.xlsx': MSTReaderXLSX,
                '.txt': MSTReaderTXT,
                }

    def _pick(self) -> MeasurementReader:
        # magic bytes might be a more robust approach in the long run
        file_extension = self.path.suffix

        try:
            picked_reader = self.readers[file_extension]
        except KeyError:
            raise ValueError(f" '{file_extension}' is not a known file type")

        try:
            # there should be a less brutal way of testing this
            picked_reader(self.path).read()
        except Exception:
            raise ValueError(f" '{self.path}' does not conform to a known MST format")

        return picked_reader


class MSTReaderMOC(MeasurementReader):
    """Class that reads from a nanotemper .moc files"""
    def __init__(self, path: Path):
        super().__init__(path, conversion_dict=MST_conversion_dict)
        self.join_expr = """
                         SELECT
                             mMst.ID, tCapillary.Annotations, IndexOnParentContainer,
                             ExcitationPower, MstPower, MstTrace
                         FROM
                             mMst
                         INNER JOIN
                             tCapillary ON mMst.container = tCapillary.ID
                         """
        self.annotation_expr = """
                               SELECT
                                   AnnotationRole, AnnotationType, Caption, NumericValue
                               FROM
                                   Annotation
                               WHERE ID = :anno_id
                               """

    def read(self) -> None:
        # Nanotemper .moc is an sqlite type file
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        for i, measurement, in enumerate(cur.execute(self.join_expr).fetchall()):
            self.measurement_dict.update({i: {}})
            current = self.measurement_dict[i]

            # instrument related metadata
            current.update(self._fetch_meta(measurement))

            # sample annotation metadata (target and ligand).
            # Annotations ids are stored in a single field (...) as strings seperated by ;
            for annotation_id in measurement['Annotations'].split(';'):
                annotation = cur.execute(self.annotation_expr, {'anno_id': annotation_id}).fetchone()
                current.update(self._fetch_annotation(annotation))

            # raw data values.
            # Assuming blob to be a bytestring of alternating x,y values
            # (i.e., x1 y1 x2 y2 etc.) in 32bit floating point numbers
            measurement_array = np.frombuffer(measurement['MstTrace'], dtype=np.float32)

            # convert to json serializable types (list of float rather than array of np.float32)
            measurement_array = list(measurement_array.astype(float))

            current.update({'Time [s]': measurement_array[::2],
                            'Raw Fluorescence [counts]': measurement_array[1::2]})

    def to_json_storage(self) -> str:
        return mst_storage_json(self)

    @staticmethod
    def _fetch_meta(measurement: sqlite3.Row) -> dict:
        """Returns the generic metadata"""
        return {'Capillary Position': measurement['IndexOnParentContainer'] + 1,
                'Excitation-Power': measurement['ExcitationPower'],
                'MST-Power': measurement['MstPower']}

    @staticmethod
    def _fetch_annotation(annotation: sqlite3.Row) -> dict:
        """Returns the annotations metadata based in one the annotation role"""
        # for unknown reasons the concentrations are stored in mM
        annotation_dict = {'ligand': {'Ligand': annotation['Caption'],
                                      'Ligand Concentration': annotation['NumericValue'] * 1e-3,
                                      },
                           'target': {'Target': annotation['Caption'],
                                      'TargetConcentration': annotation['NumericValue'] * 1e-3
                                      },
                           'dilutionseries': {}}
        role = annotation['AnnotationRole']
        return annotation_dict[role]


class MSTReaderXLSX(MeasurementReader):
    """Class that reads data from .xlsx that has been
       exported from nanotemper .moc files"""

    def __init__(self, path: Path):
        self.sample_df = pd.DataFrame
        self.index_identifiers = {}
        super().__init__(path, conversion_dict=MST_conversion_dict)

    def get_sample_df(self):
        xlsx_file = pd.ExcelFile(self.path)
        self.sample_df = pd.read_excel(xlsx_file, sheet_name='RawData', header=None)

    def read(self) -> None:
        self.get_sample_df()
        self._find_sections()
        self._clean()

        meta_start = self.index_identifiers['sample_info']
        xy_start = self.index_identifiers['xy'] + 1

        for i in range(len(self.sample_df.columns)//2):
            # each sample is described by two columns (time. fluorescence)
            current_df = self.sample_df.iloc[:, i*2:(i*2)+2]

            # the metadata part of current_df
            meta_df = current_df.loc[meta_start: xy_start-1, :].values
            self.measurement_dict[i] = {key[:-1]: value for (key, value) in meta_df}

            # the measured part of current_df
            x_key, y_key = current_df.loc[xy_start, :].values
            xy_df = current_df.loc[xy_start + 1:, :].dropna(how='all')
            xy_df.columns = [x_key, y_key]
            self.measurement_dict[i].update(xy_df.to_dict(orient='list'))

    def to_json_storage(self) -> str:
        return mst_storage_json(self)

    def _find_sections(self) -> None:
        info = self.sample_df.iloc[:, 0]
        headers = ['Origin of exported data',
                   'Analysis Settings',
                   'Sample Information',
                   'Measurement Settings',
                   'Included']
        names = ['origin', 'anal_set', 'sample_info', 'meas_set', 'xy']
        self.index_identifiers = {n: (info == head).idxmax() for n, head in zip(names, headers)}

    def _clean(self) -> None:
        # Remove rows and columns containing empty data
        self.sample_df.dropna(axis='rows', how='all', inplace=True)
        self.sample_df.dropna(axis='columns', how='all', inplace=True)
        # Third row contains a few empty rows
        self.sample_df.drop(2, axis=1, inplace=True)
        # remove rows with headers as they don't contain data
        for value in self.index_identifiers.values():
            self.sample_df.drop(value, axis=0, inplace=True)


class MSTReaderTXT(MeasurementReader):
    """Class that reads data from .txt that has been
       exported from nanotemper .nta files"""

    def __init__(self, path: Path):
        self.sample_df = pd.DataFrame
        super().__init__(path, conversion_dict=MST_conversion_dict)

    def read(self) -> None:
        self.sample_df = pd.read_csv(self.path, sep='\t', dtype=np.float32,
                                     na_values=['****.*****', '****.****'])
        self._clean()
        for i in range(len(self.sample_df.columns) // 2):

            # each sample is described by two columns (time. fluorescence)
            current_df = self.sample_df.iloc[:, i * 2:(i * 2) + 2]
            # the ligand concentration is in the name og the column e.g. 13.5_t
            ligand_conc = current_df.columns.values[0].split('_')[0]

            # enforcing the same naming scheme as in the .moc derived xlsx
            self.measurement_dict[i] = {'Ligand Concentration': float(ligand_conc)}
            current_df.columns = ['Time [s]', 'Raw Fluorescence [counts]']

            self.measurement_dict[i].update(current_df.to_dict(orient='list'))

    def to_json_storage(self) -> str:
        return mst_storage_json(self)

    def _clean(self) -> None:
        # Remove rows and columns containing empty data
        self.sample_df.dropna(axis='rows', how='all', inplace=True)
        self.sample_df.dropna(axis='columns', how='all', inplace=True)


def mst_storage_json(reader: MeasurementReader) -> str:
    """Returns the json string representation of the subset of extracted item names
       that could be mapped to mbdb item names along with the extracted values"""

    json_list = []
    for measurement_content in reader.measurement_dict.values():
        json_list.append(to_json_dict(measurement_content, reader.conversion_dict))

    return to_json_string(json_list)
