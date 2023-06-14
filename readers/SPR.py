from readers.base import MeasurementReader, ReaderPicker
from pathlib import Path
from typing import Dict, Type
import numpy as np
import olefile
import matplotlib.pyplot as plt


class SPRPicker(ReaderPicker):
    def __init__(self, *paths: Path):
        super().__init__(*paths, single_file_only=False)

    def _readers(self) -> Dict[str, Type[MeasurementReader]]:
        return {'biacore': SPRReaderBiacore,
                }

    def _pick(self) -> MeasurementReader:
        pass


class SPRReaderBiacore(MeasurementReader):
    def __init__(self, path: Path):
        super().__init__(path)

    def read(self) -> None:
        ole = olefile.OleFileIO(self.path)
        streams = ole.listdir()
        fig, axes = plt.subplots(2, 2, tight_layout=True)
        axes = axes.flatten()

        for stream in streams:
            print(stream)
            if stream[-1] in ('XYData'):
                d = ole.openstream(stream).read()
                data = np.frombuffer(d, dtype=np.float32)

                split_point = len(data)//2
                x_data = data[:split_point]
                y_data = data[split_point:-1]
                axes_number = int(stream[0][-1]) - 1
                label = ' '.join(stream)
                axes[axes_number].plot(x_data, y_data, label=' '.join(stream)[1:])
                axes[axes_number].legend()





