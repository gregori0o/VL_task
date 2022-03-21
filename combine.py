from reader import CSVReader, Chunk
from typing import Generator, List


class Join(object):
    def __init__(self, left_filepath: str, right_filepath: str, column_name: str):
        self.left_reader = CSVReader(left_filepath)
        self.left_column = self.reader.set_column(column_name)
        self.right_reader = CSVReader(right_filepath)
        self.right_column = self.reader.set_column(column_name)
        if self.left_column is None or self.right_column is None:
            print("Invalid name of column - {}".format(column_name))
            self.left_reader.end_reading()
            self.right_reader.end_reading()
            exit(-1)

    def pretty_print(self, left_row: List[any], right_row: List[any]):
        row_format = "{:>15}" * (len(left_row) + len(right_row))
        print(row_format.format(*left_row, *right_row))

    def inner(self):
        self.pretty_print(self.left_reader.headers, self.right_reader.headers)
        for left_chunk in self.left_reader.get_chunk():
            if left_chunk is None:
                break
            left_values = dict()
            for row in left_chunk:
                tmp = left_values.get(row[self.left_column], set())
                tmp.add(row)
                left_values[row[self.left_column]] = tmp
            for right_chunk in self.right_reader.get_chunk():
                if right_chunk is None:
                    break
                for row in right_chunk:
                    left_rows = left_values.get(row[self.right_column], None)
                    if left_rows is None:
                        continue
                    for element in left_rows:
                        self.pretty_print(element, row)

    def right(self):
        self.pretty_print(self.left_reader.headers, self.right_reader.headers)
        left_none = [None] * len(self.left_reader.headers)
        for left_chunk in self.left_reader.get_chunk():
            if left_chunk is None:
                break
            left_values = dict()
            for row in left_chunk:
                tmp = left_values.get(row[self.left_column], set())
                tmp.add(row)
                left_values[row[self.left_column]] = tmp
            for right_chunk in self.right_reader.get_chunk():
                if right_chunk is None:
                    break
                for row in right_chunk:
                    left_rows = left_values.get(row[self.right_column], None)
                    if left_rows is None:
                        self.pretty_print(left_none, row)
                        continue
                    for element in left_rows:
                        self.pretty_print(element, row)

    def left(self):
        self.left_reader, self.right_reader = self.right_reader, self.left_reader
        self.left_column, self.right_column = self.right_column, self.left_column
        self.right()

    def end_join(self):
        self.left_reader.end_reading()
        self.right_reader.end_reading()
