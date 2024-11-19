import re

from invenio_records_resources.services.files.processors import FileProcessor
from invenio_records_resources.services.uow import UnitOfWork, RecordCommitOp
from oarepo_runtime.datastreams.utils import get_record_service_for_record


class MethodFileProcessor(FileProcessor):
    def has_valid_method(self, file_record) -> bool:
        method = file_record.record.metadata["general_parameters"]["technique"]
        # the method abbreviation between the brackets in the technique
        method = re.search(r"\([A-Z]+", method).group(0)[1:]
        return method == self.applicable_method

    @property
    def applicable_method(self) -> str:
        raise NotImplementedError(f"allowed methods need to be a implemented")


def log_exception(exception, logger, file_record):
    record_id = file_record.record.get('id') or file_record.record.id
    logger.exception(
        f"Error extracting text from pdf file {file_record.key} on record {record_id}: {exception}"
    )

def commit_to_record(record):
    with UnitOfWork() as uow:
        record_service = get_record_service_for_record(record)
        if record_service:
            indexer = record_service.indexer
            uow.register(RecordCommitOp(record, indexer, index_refresh=True))
            uow.commit()

def has_correct_signature(file_record, magic_bytes: list | tuple | set):
    with file_record.open_stream("rb") as f:
        signature = f.read(len(magic_bytes[0]))
    return signature in magic_bytes
