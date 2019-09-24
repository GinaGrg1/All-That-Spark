from pyspark import SparkContext
import re

sc = SparkContext("local[3]", "Airport Not In USA Problem")
sc.setLogLevel("ERROR")

"""
Create a spark program to read the airport data from in/airports.text;
Generate a pair RDD with airport name being the key & country name being the value.
Then remove all the airports which are located in the US & output the pair RDD to
out/airports_not_in_usa_pair_rdd.text

Sample output:
    ("Kamloops", "Canada")
    ("Wewak Intl", "Papua New Guinea")
"""

airportline = sc.textFile('./in/airports.text')

airportsNotInUSA = airportline\
    .filter(lambda line: re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line)[3] != "\"United States\"")\
    .map(lambda x: (x.split(",")[1], x.split(",")[3]))

airportsNotInUSA.saveAsTextFile("out/airports_not_in_usa_pair_rdd.text")