from pyspark import SparkContext
import re

"""
Create a Spark program to read the airport data from in/airports.text, output the list
of the names of the airports located in each country.

Sample output :
    "Canada", ["Bagotville", "Montreal", "Coronation", ...]
    "Norway", ["Vigra, "Andenes", "Bomoen", "Bronnoy"...]
    
    r',(?=(?:[^"]*"[^"]*")*[^"]*$)'
"""
sc = SparkContext("local[3]", "Avg House Price")
sc.setLogLevel("ERROR")

lines = sc.textFile("in/airports.text")

airports = lines.map(lambda line: (re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line)[3],  # key is country
                                   re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line)[1]))  # value is airports

groupbyairports = airports.groupByKey()  # gives the country name as the key and an iterator as value
for country, airportname in groupbyairports.collectAsMap().items():
    print("{} : {}".format(country, list(airportname)))

