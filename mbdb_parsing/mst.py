import logging
import sqlite3

from copy import deepcopy
from uuid import uuid4

import pandas as pd

from .base import (
    MethodFileProcessor,
    log_exception,
    commit_to_record,
    has_correct_signature,
)

moc_log = logging.getLogger("extract-mst-moc-parameters-processor")
xlsx_log = logging.getLogger("extract-mst-xlxs-parameters-processor")


#
# MST baseclass
class MstFileProcessor(MethodFileProcessor):

    @property
    def applicable_method(self) -> str:
        return "MST"


class MocProcessor(MstFileProcessor):
    def can_process(self, file_record):
        return (
            has_correct_signature(file_record, (b"SQLite format 3\x00",))
            and self.has_valid_method(file_record)
            and self.file_extension(file_record) in (".moc", ".moc2")
        )

    def process(self, file_record):
        try:
            print(f"Extracting parameters from file {file_record.key} in record {file_record.record['id']}")

            # create in-memory copy of db
            cursor = self._make_cursor(file_record)
            # extract information from the db
            measurements = self._create_measurements(cursor)

            record = file_record.record
            record.metadata["method_specific_parameters"].update(measurements)
            commit_to_record(record)

        except Exception as e:
            log_exception(e, moc_log, file_record)

    @staticmethod
    def _make_cursor(file_record):
        with file_record.open_stream("rb") as f:
            db_stream = f.read()

        conn = sqlite3.connect(":memory:")
        conn.deserialize(db_stream)
        conn.row_factory = sqlite3.Row
        return conn.cursor()

    def _create_measurements(self, cursor):
        measurement_sql = """
            SELECT
                mMst.ID, tCapillary.Annotations, IndexOnParentContainer,
                ExcitationPower, MstPower
            FROM
                mMst
            INNER JOIN
                tCapillary ON mMst.container = tCapillary.ID
        """

        measurement_template = {
            "position": "",
            "sample": {},
        }

        measurements = {"measurements": []}
        rows = cursor.execute(measurement_sql).fetchall()
        for row in rows:
            measurement = deepcopy(measurement_template)
            measurement["id"] = str(uuid4())

            # instrument related metadata
            ins = self._fetch_instrument(row)
            measurement["position"] = str(ins.pop("position"))
            measurements.update(ins)

            # sample annotation metadata (target and ligand).
            # Annotations ids are stored in a single field (...) as uuids separated by ;
            for annotation_id in row["Annotations"].split(";"):
                annotation = self._fetch_annotation(annotation_id, cursor)
                measurement["sample"].update(annotation)

            measurements["measurements"].append(measurement)

        return measurements

    @staticmethod
    def _fetch_instrument(row: sqlite3.Row) -> dict:
        """Returns instrument related metadata"""
        return {
            "position": row["IndexOnParentContainer"] + 1,
            "excitation_led_power": row["ExcitationPower"],
            "ir_mst_laser_power": row["MstPower"],
        }

    @staticmethod
    def _fetch_annotation(annotation_id, cursor) -> dict:
        """Return the (measurement) annotation metadata based in on the annotation role"""

        annotation_sql = """
            SELECT
                AnnotationRole, AnnotationType, Caption, NumericValue
            FROM
                Annotation
            WHERE ID = :anno_id
        """

        annotation = cursor.execute(
            annotation_sql, {"anno_id": annotation_id}
        ).fetchone()

        role = annotation["AnnotationRole"]
        if role == "dilutionseries":  # this annotation is currently not used
            return {}
        elif role in ("target", "ligand"):
            return {
                f"{role}s": [
                    {
                        #"entity": {"name": annotation["Caption"]},
                        "concentration": {
                            "value": annotation["NumericValue"],
                            "unit": annotation["AnnotationType"],
                        },
                    }
                ]
            }
        else:
            raise ValueError(f"Unknown annotation: '{role}'")


class XlxsProcessor(MstFileProcessor):
    def can_process(self, file_record):
        magic_bytes = (
            bytes([0x50, 0x4B, 0x03, 0x04]),
            bytes([0x50, 0x4B, 0x05, 0x06]),
            bytes([0x50, 0x4B, 0x07, 0x08]),
        )

        return (
            has_correct_signature(file_record, magic_bytes)
            and self.has_valid_method(file_record)
            and self.file_extension(file_record) == ".xlsx"
        )

    def process(self, file_record):
        try:
            print(f"Extracting parameters from file {file_record.key}")
            measurements = self.read(file_record)

            record = file_record.record
            record.metadata["method_specific_parameters"].update(measurements)
            commit_to_record(record)

        except Exception as e:
            log_exception(e, moc_log, file_record)

    @staticmethod
    def _get_sample_df(file_record):
        with file_record.open_stream("rb") as f:
            return pd.read_excel(pd.ExcelFile(f), sheet_name="RawData", header=None)

    def read(self, file_record) -> dict:
        sample_df = self._get_sample_df(file_record)
        index_identifiers = self._get_index_identifiers(sample_df)
        # clean sample_df inplace
        self._clean(sample_df, index_identifiers)

        meta_start = index_identifiers["sample_info"]
        xy_start = index_identifiers["xy"] + 1

        measurements = {"measurements": []}
        for i in range(len(sample_df.columns) // 2):
            # each measurement is described by two columns (time and fluorescence)
            current_df = sample_df.iloc[:, i * 2 : (i * 2) + 2]

            # the metadata part of current_df
            meta_df = dict(current_df.loc[meta_start : xy_start - 1, :].values)
            # clean the keys
            meta_df = {
                "".join([char for char in key if char not in " :-"]): value
                for (key, value) in meta_df.items()
            }

            self._convert(meta_df, measurements)
        return measurements

    @staticmethod
    def _convert(meta_df, measurements) -> None:
        conversion_dict = {"Low": 20, "Medium": 40, "High": 60}
        # it is assumed that excitation power and IR intensity is the same for all samples
        measurements.update(
            {
                "ir_mst_laser_power": conversion_dict[meta_df["MSTPower"]],
                "excitation_led_power": meta_df["ExcitationPower"],
            }
        )

        measurements["measurements"].append(
            {
                "id": str(uuid4()),
                "position": str(meta_df["CapillaryPosition"]),
                "sample": {
                    "targets": [
                        {
                         #   "entity": {"name": meta_df["Target"]},
                            "concentration": {
                                "value": meta_df["TargetConcentration"],
                                "unit": "M"
                            },
                        }
                    ],
                    "ligands": [
                        {
                         #   "entity": {"name": meta_df["Ligand"]},
                            "concentration": {
                                "value": meta_df["LigandConcentration"],
                                "unit": "M"
                            },
                        }
                    ],
                },
            }
        )

    @staticmethod
    def _get_index_identifiers(sample_df: pd.DataFrame) -> dict:
        info = sample_df.iloc[:, 0]
        headers = [
            "Origin of exported data",
            "Analysis Settings",
            "Sample Information",
            "Measurement Settings",
            "Included",
        ]
        names = ["origin", "anal_set", "sample_info", "meas_set", "xy"]
        return {n: (info == head).idxmax() for n, head in zip(names, headers)}

    @staticmethod
    def _clean(sample_df, index_identifiers) -> None:
        # Remove rows and columns containing empty data
        sample_df.dropna(axis="rows", how="all", inplace=True)
        sample_df.dropna(axis="columns", how="all", inplace=True)
        # Third row contains a few empty rows
        sample_df.drop(2, axis=1, inplace=True)
        # remove rows with headers as they don't contain data
        for value in index_identifiers.values():
            sample_df.drop(value, axis=0, inplace=True)
