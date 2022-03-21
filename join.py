#!/usr/bin/python3

import sys
import os


join_types = ['inner', 'left', 'right']
if len(sys.argv) < 4:
    print(
        "Not enough arguments! Correct function call is: ./join 'file_path' 'file_path' 'column_name' ['join_type'].")
    exit(-1)
if not os.path.exists(sys.argv[1]):
    print("File {} is not exists!".format(sys.argv[1]))
    exit(-1)
if not os.path.exists(sys.argv[2]):
    print("File {} is not exists!".format(sys.argv[2]))
    exit(-1)
if len(sys.argv) == 4:
    join_type = 'inner'
else:
    join_type = sys.argv[4]
    if join_type in join_types:
        print("Invalid type of join: {}, correct types is {}".format(join_type, " ".join(join_types)))
        exit(-1)





