from reader import CSVReader
from combine import Join

"""
'left.csv'
id,num,surname,address
1,1,Arm,None
2,2,Nowak,Krakow
3,3,Kowal,Zakopane
4,4,Pol,Rzeszow
5,5,Nowo,Kielce
6,1,Kra,Kielce
7,2,Bart,Wawa
8,3,Auto,Szczecin
9,4,Ona,Gdansk
"""

"""
'right.csv'
id,name,address,num
1,Marek,Krakow,1
2,Magda,Huta,1
3,Ola,Zakopane,8
4,Zuza,Wroc,6
5,Lukasz,Krakow,6
6,Krzys,Wroc,4
7,Grzes,Zakopane,8
8,Tomek,Wawa,6
9,Piotr,Wawa,3
10,Jan,Wawa,3
"""


#if correct: print headers once, print data in 2-elements chunk, then None and repeat printing data
def test_reader():
    reader = CSVReader('../data/left.csv', 2)
    print("Headers: {}".format(",".join(reader.headers)))
    i = 1
    for chunk in reader.get_chunk():
        print("Chunk {} -> {}".format(i, str(chunk)))
        i += 1
        if i >= 14:
            break

#print inner join for 'address' column
def test_inner():
    join = Join('../data/left.csv', 'data/right.csv', 'address')
    join.inner()

#print left join for 'num' column and size of chunk=3
def test_left():
    join = Join('../data/left.csv', 'data/right.csv', 'num', 3)
    join.left()

#test handle big file
def test_big():
    join = Join('../data/big.csv', 'data/big.csv', 'Order ID')
    join.inner()


if __name__ == "__main__":
    test_reader()
    print()
    test_inner()
    print()
    test_left()
    print()
    #test_big()