import sys
from math import sqrt

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Assignment1").getOrCreate()
sc = spark.sparkContext


def loadmoviename(movienamefile):
    movienames = dict()
    with open(movienamefile, encoding='ISO-8859-1') as f:
        for line in f:
            fields = line.split('|')
            movienames[int(fields[0])] = fields[1].decode('ascii', 'ignore')  # {1: 'Toy Story (1995)'}
    return movienames


moviename = "./ml-100k/u.ITEM"
namedict = sc.broadcast(loadmoviename(moviename))  # returns a dictionary of {movieid: 'moviename'}
