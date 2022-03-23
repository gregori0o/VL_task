#!/usr/bin/python3

import sys
import os
from combine import Join

LEFT = 'left'
RIGHT = 'right'
INNER = 'inner'
join_types = [LEFT, RIGHT, INNER]
CHUNK_SIZE = 1000


def main():
    if len(sys.argv) < 4:
        print("Not enough arguments! Correct function call is: ./join 'file_path' 'file_path' 'column_name' ['join_type'].")
        exit(-1)
    if not os.path.exists(sys.argv[1]):
        print("File {} is not exists!".format(sys.argv[1]))
        exit(-1)
    if not os.path.exists(sys.argv[2]):
        print("File {} is not exists!".format(sys.argv[2]))
        exit(-1)
    if len(sys.argv) == 4:
        join_type = INNER
    else:
        join_type = sys.argv[4]
        if join_type not in join_types:
            print("Invalid type of join: {}, correct types is {}".format(join_type, " ".join(join_types)))
            exit(-1)

    join = Join(sys.argv[1], sys.argv[2], sys.argv[3], chunk_size=CHUNK_SIZE)
    if join_type == INNER:
        join.inner()
    elif join_type == LEFT:
        join.left()
    elif join_type == RIGHT:
        join.right()
    join.end_join()


if __name__ == '__main__':
    main()
