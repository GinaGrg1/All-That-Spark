from pyspark import SparkContext
import re

sc = SparkContext("local[3]", "Airport Not In USA Problem")
sc.setLogLevel("ERROR")

"""
Create a Spark program to read the airport data from in/airports.text,
generate a pair RDD with airport name being the key & country name being the value.
Then convert the country name to uppercase and output the pair RDD to out/airports_uppercase.txt

"""

airportline = sc.textFile('./in/airports.text')

# First create an RDD with airport name & country name.
airportpairRDD = airportline.map(lambda line: (re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)',line)[1],
                                                 re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)',line)[3]))

# We can use mapValues here as we only need to change the value to uppercase and not the key
airportuppercase = airportpairRDD.mapValues(lambda countryName: countryName.upper())

airportuppercase.saveAsTextFile("out/airports_uppercase.txt")