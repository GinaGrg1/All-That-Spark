from pyspark import SparkContext

"""
Create a spark program to read the house data from in/RealEstate.csv, output the average
price for house with different number of bedrooms.

Sample Output:
(3, 325000)
(1, 266356)
(2, 325000)

"""

sc = SparkContext("local[3]", "Avg House Price")
sc.setLogLevel("ERROR")

lines = sc.textFile("./in/RealEstate.csv")

# First get rid of the header. The split the line and take the bedroom as key & price as value.
# [('3', (1, 795000.0)), ('4', (1, 399000.0)), ('4', (1, 545000.0)), ('4', (1, 909000.0)), ('3', (1, 109900.0))..]
houses = lines.filter(lambda line: 'Price' not in line)\
              .map(lambda line: (int(line.split(",")[3]), (1, float(line.split(",")[2]))))

# output of doing reduceByKey from above:
# [('4', (177, 85575190.0)), ('1', (11, 1869800.0)), ('0', (2, 586900.0)), ('10', (1, 699000.0)),
# ('3', (431, 154755811.0))]
# We used mapValues here as we only need to change the Values and not the key.
avgprice = houses.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
                  .mapValues(lambda x: x[1]/x[0])

sortedHousePrice = avgprice.sortByKey(ascending=False)

for bedrooms, avgprice in sortedHousePrice.collect():
    print("{} : {}".format(bedrooms, round(avgprice, 3)))

"""
10 : 699000.0
7 : 325000.0
6 : 603225.0
5 : 657858.0645161291
4 : 483475.6497175141
3 : 359062.20649651974
2 : 266356.3739837398
1 : 169981.81818181818
0 : 293450.0
"""