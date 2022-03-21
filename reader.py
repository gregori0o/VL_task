import csv
from typing import List, Optional

Chunk = List[List[any]]


class CSVReader(object):
    def __init__(self, filepath: str, chunk_size: int = 100):
        self.chunk_size = chunk_size
        self.filepath = filepath
        try:
            self.csvfile = open(filepath)
            self.reader = csv.reader(self.csvfile)
        except:
            print("File {} can not be opened!".format(self.filepath))
            if self.csvfile:
                self.csvfile.close()
            exit(-1)

        self.headers = next(self.reader)

    def set_column(self, column_name: str) -> Optional[int]:
        for i, column in enumerate(self.headers):
            if column == column_name:
                return i
        return None

    def get_chunk(self) -> Optional[Chunk]:
        while(True):
            chunk = []
            idx = 0
            for row in self.reader:
                chunk.append(row)
                idx += 1
                if idx == self.chunk_size:
                    yield chunk
                    idx = 0
                    del chunk[:]
                    #chunk = []
            if chunk:
                yield chunk
            yield None
            #todo coś żeby self.reader od nowa leciał

    def end_reading(self):
        self.csvfile.close()


