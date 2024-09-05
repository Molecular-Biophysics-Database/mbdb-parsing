import sqlite3

from copy import deepcopy
from pathlib import Path
from typing import Dict, Type
from uuid import uuid4

import pandas as pd

from readers.base import MeasurementReader, ReaderPicker

class MstPicker(ReaderPicker):
    """Picks the correct (MST) MeasurementReader for an MST file"""

    def __init__(self, *paths: Path):
        super().__init__(*paths, single_file_only=True)

    def _readers(self) -> Dict[str, Type[MeasurementReader]]:
        return {
            ".moc": MstMocReader,
            ".moc2": MstMocReader, # Changes from moc to moc2 doesn't affect the metadata items we're after
            ".xlsx": MstXlsxReader,
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


class MstMocReader(MeasurementReader):
    """Class that reads from nanotemper .moc and .moc2 files"""

    def __init__(self, path: Path):
        super().__init__(path)
        self._join_expr = """
                         SELECT
                             mMst.ID, tCapillary.Annotations, IndexOnParentContainer,
                             ExcitationPower, MstPower, MstTrace
                         FROM
                             mMst
                         INNER JOIN
                             tCapillary ON mMst.container = tCapillary.ID
                         """
        self._annotation_expr = """
                               SELECT
                                   AnnotationRole, AnnotationType, Caption, NumericValue
                               FROM
                                   Annotation
                               WHERE ID = :anno_id
                               """
        self._measurement = {
            "position": "",
            "sample": {},
        }

    def read(self) -> None:
        # .moc(2) are sqlite files
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        rows = cur.execute(self._join_expr).fetchall()
        measurements = []
        for row in rows:
            measurement = deepcopy(self._measurement)
            measurement["id"] = str(uuid4())

            # instrument related metadata
            ins = self._fetch_instrument(row)
            measurement["position"] = str(ins.pop("position"))
            self.measurement_dict.update(ins)

            # sample annotation metadata (target and ligand).
            # Annotations ids are stored in a single field (...) as uuids seperated by ;
            for annotation_id in row["Annotations"].split(";"):
                annotation = cur.execute(
                    self._annotation_expr, {"anno_id": annotation_id}
                ).fetchone()
                measurement["sample"].update(self._fetch_annotation(annotation))

            measurements.append(measurement)
        self.measurement_dict["measurements"] = measurements

    @staticmethod
    def _fetch_instrument(row: sqlite3.Row) -> dict:
        """Returns instrument related metadata"""
        return {
            "position": row["IndexOnParentContainer"] + 1,
            "excitation_led_power": row["ExcitationPower"],
            "ir_mst_laser_power": row["MstPower"],
        }

    @staticmethod
    def _fetch_annotation(annotation: sqlite3.Row) -> dict:
        """Return the (measurement) annotation metadata based in on the annotation role"""
        role = annotation["AnnotationRole"]
        if role == "dilutionseries": # this annotation is currently not used
            return {}
        elif role in ("target", "ligand"):
            return {
                role: { f"{role}s": [
                    {
                        "entity": {"name": annotation["Caption"]},
                        "concentration": {
                            "value": annotation["NumericValue"],
                            "unit": annotation["AnnotationType"]}
                    }
            ]}}
        else:
            raise ValueError(f"Unknown annotation: '{role}'")


class MstXlsxReader(MeasurementReader):
    """Class that reads data from .xlsx that has been
       exported from nanotemper .moc files"""

    def __init__(self, path: Path):
        super().__init__(path)
        self._sample_df = pd.DataFrame
        self._index_identifiers = {}
        self.measurement_dict = {"measurements": []}


    def get_sample_df(self):
        xlsx_file = pd.ExcelFile(self.path)
        self._sample_df = pd.read_excel(xlsx_file, sheet_name='RawData', header=None)

    def read(self) -> None:
        self.get_sample_df()
        self._find_sections()
        self._clean()

        meta_start = self.index_identifiers['sample_info']
        xy_start = self.index_identifiers['xy'] + 1

        for i in range(len(self._sample_df.columns)//2):
            # each measurement is described by two columns (time and fluorescence)
            current_df = self._sample_df.iloc[:, i*2:(i*2)+2]

            # the metadata part of current_df
            meta_df = dict(current_df.loc[meta_start: xy_start-1, :].values)
            # clean the keys
            meta_df = {"".join([char for char in key if char not in " :-"]): value
                for (key, value) in meta_df.items()}

            self._convert(meta_df)

    def _convert(self, meta_df) -> None:
        conversion_dict = {"Low": 20, "Medium": 40, "High": 60}
        # it is assumed that excitation power and IR intensity is the same for all samples
        self.measurement_dict.update({
            "ir_mst_laser_power": conversion_dict[meta_df["MSTPower"]],
            "excitation_led_power": meta_df["ExcitationPower"]
        })

        self.measurement_dict["measurements"].append({
            "id": str(uuid4()),
            "position": str(meta_df["CapillaryPosition"]),
            "sample": {
                "targets": [{
                    "entity": {"name": meta_df["Target"]},
                    "concentration": {"value": meta_df["TargetConcentration"]}
                }],
                "ligands": [{
                    "entity": {"name": meta_df["Ligand"]},
                    "concentration": {"value": meta_df["LigandConcentration"]}
                }],
            }
        })

    def _find_sections(self) -> None:
        info = self._sample_df.iloc[:, 0]
        headers = ['Origin of exported data',
                   'Analysis Settings',
                   'Sample Information',
                   'Measurement Settings',
                   'Included']
        names = ['origin', 'anal_set', 'sample_info', 'meas_set', 'xy']
        self.index_identifiers = {n: (info == head).idxmax() for n, head in zip(names, headers)}

    def _clean(self) -> None:
        # Remove rows and columns containing empty data
        self._sample_df.dropna(axis='rows', how='all', inplace=True)
        self._sample_df.dropna(axis='columns', how='all', inplace=True)
        # Third row contains a few empty rows
        self._sample_df.drop(2, axis=1, inplace=True)
        # remove rows with headers as they don't contain data
        for value in self.index_identifiers.values():
            self._sample_df.drop(value, axis=0, inplace=True)
