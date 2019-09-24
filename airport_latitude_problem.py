from pyspark import SparkContext
import re

"""
Find all airports whose latitude are bigger than 40. Then output the airport's name & airport's latitude 
to out/airports_by_latitude.txt.
"""


def splitcomma(line):
    splits = re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line)
    return "{}, {}".format(splits[1], splits[6])


sc = SparkContext("local[2]", "Airport Latitude Problem")
sc.setLogLevel("ERROR")

airportline = sc.textFile('./in/airports.text')

airportslatgreaterthan40 = airportline\
    .filter(lambda line: float(re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line)[6]) > 40)\
    .sortBy(lambda x: x[6], ascending=False)

airportnamewithlatgreaterthan40 = airportslatgreaterthan40.map(splitcomma)

airportnamewithlatgreaterthan40.saveAsTextFile("out/latitudesol_sorted.csv")