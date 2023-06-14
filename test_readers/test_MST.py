import pytest
from readers import MST
from pathlib import Path
import pandas as pd

MST_FILES = (
    {'path': 'test_data/labelfree_test.xlsx', 'reader': MST.MSTReaderXLSX,
        'index': {'anal_set': 11, 'meas_set': 30, 'origin': 3, 'sample_info': 19, 'xy': 36}},
    {'path': 'test_data/red_test.xlsx', 'reader': MST.MSTReaderXLSX,
        'index': {'anal_set': 11, 'meas_set': 26, 'origin': 3, 'sample_info': 15, 'xy': 32}},
    {'path': 'test_data/nta_test.txt', 'reader': MST.MSTReaderTXT},
    {'path': 'test_data/Show_WS_May21.moc', 'reader': MST.MSTReaderMOC},
             )


class TestMSTPicker:
    invalid_filetype = Path('/file.phony')
    invalid_file_content = Path('test_data/incorrect_mst.xlsx')

    def test_invalid_mst_filetype(self):
        with pytest.raises(ValueError):
            MST.MSTPicker(self.invalid_filetype)

    def test_invalid_mst_file_content(self):
        with pytest.raises(ValueError):
            MST.MSTPicker(self.invalid_filetype)

    def test_picker(self):
        for file in MST_FILES:
            picked_reader = MST.MSTPicker(Path(file['path'])).picked_reader
            assert issubclass(picked_reader, file['reader'])


class TestXLSXMeasurementReader:
    get_sample_test_file = Path('test_data/xlsx_to_df_test.xlsx')
    read_test_files = [file for file in MST_FILES if issubclass(file['reader'], MST.MSTReaderXLSX)]

    def test_get_sample_df(self):
        reader = MST.MSTReaderXLSX(self.get_sample_test_file)
        reader.get_sample_df()
        assert reader.sample_df.equals(pd.DataFrame({0: [1, 3], 1: [2, 4]}))

    def test_find_sections(self):
        for file in self.read_test_files:
            reader = MST.MSTReaderXLSX(Path(file['path']))
            reader.get_sample_df()
            reader._find_sections()
            assert reader.index_identifiers == file['index']

    def test_clean(self):
        pass

    def test_read(self):
        pass

    def test_to_json(self):
        pass
