from pyspark import SparkContext
import re


class Utils:
    """
    The commas inside the double quotes should not be used to split the file.
    """
    COMMA_DELIMETER = re.compile(r',(?=(?:[^"]*"[^"]*")*[^"]*$)')


def splitcomma(line):
    splits = Utils.COMMA_DELIMETER.split(line)
    return "{}, {}".format(splits[1], splits[2])


sc = SparkContext("local[3]", "Airport Problem")
sc.setLogLevel("ERROR")

airportline = sc.textFile('./in/airports.text')

airport = airportline.map(lambda line: line.split(','))\
                     .filter(lambda x: x[3] == "\"United States\"")\
                     .map(lambda x: (x[2], x[3]))

# Orielly solution

airportsInUSA = airportline\
    .filter(lambda line: re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line)[3] == "\"United States\"")

airportsNameandCity = airportsInUSA.map(splitcomma)
airportsNameandCity.saveAsTextFile("out/airports_in_usa_orielly.text")
# airport.saveAsTextFile('airports_in_usa.txt')
