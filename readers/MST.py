import sqlite3

from copy import deepcopy
from pathlib import Path
from typing import Dict, Type
from uuid import uuid4

from readers.base import MeasurementReader, ReaderPicker

class MstPicker(ReaderPicker):
    """Picks the correct (MST) MeasurementReader for an MST file"""

    def __init__(self, *paths: Path):
        super().__init__(*paths, single_file_only=True)

    def _readers(self) -> Dict[str, Type[MeasurementReader]]:
        return {
            ".moc": MstMocReader,
            ".moc2": MstMocReader, # Changes from moc to moc2 doesn't affect the metadata items we're after
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
        if role == "dilutionseries":
            return {}
        elif role in ("target", "ligand"):
            return {
                role: {
                    f"{role}s": [
                        {
                            "entity": {"name": annotation["Caption"]},
                            "concentration": {
                                "value": annotation["NumericValue"],
                                "unit": annotation["AnnotationType"]}
                        }
                    ]
                }
            }
        else:
            raise ValueError(f"Unknown annotation role: '{role}'")