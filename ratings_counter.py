"""
The data is : 
user_id, movie_id, rating, timestamp
196	        242	    3	    881250949
186	        302	    3	    891717742
22	        377	    1	    878887116
244	        51	    2	    880606923
"""
from pyspark.sql import SparkSession
import collections


def loadmoviename(movienamefile):
    movienames = dict()
    with open(movienamefile, encoding='ISO-8859-1') as f:
        for line in f:
            fields = line.split('|')
            movienames[int(fields[0])] = fields[1]  # {1: 'Toy Story (1995)'}
    return movienames


def loadmoviedata(filename, sparkcon):
    return sparkcon.textFile(filename)


def ratingsresult(lines):
    ratings = lines.map(lambda x: x.split()[2])  # This will be a list of ratings: ['3', '3', '1', '2', '1']
    ratings_result = ratings.countByValue()  # This is a collection and not an RDD
    # defaultdict(<class 'int'>, {'3': 27145, '1': 6110, '2': 11370, '4': 34174, '5': 21201})
    return collections.OrderedDict(sorted(ratings_result.items()))


def popularmovies(lines):
    """
    To filter this RDD for movie_id 242
    movies.filter(lambda x: x[0] == 242).take(10)

    """
    movies = lines.map(lambda x: (int(x.split()[1]), 1))  # [(242, 1), (302, 1), (377, 1), (51, 1), (346, 1)]
    movies_counts = movies.reduceByKey(lambda x, y: x + y)  # This is an RDD. [(242, 117), (302, 297), (377, 13), ..]
    flipped = movies_counts.map(lambda x: (x[1], x[0]))  # Flip the key & value. [(117, 242), (297, 302), (13, 377)]
    return flipped.sortByKey()


if __name__ == '__main__':
    spark = SparkSession.builder.appName("Movie data analysis").getOrCreate()
    sc = spark.sparkContext

    file = "./ml-100k/u.data"
    moviename = "./ml-100k/u.ITEM"

    lines = loadmoviedata(file, sc)     # returns the RDD
    namedict = sc.broadcast(loadmoviename(moviename))  # returns a dictionary of {movieid: 'moviename'}
    sortedmovies = popularmovies(lines)  # returns a list of tuple: [(count, movieid)]

    sortedmovieswithnames = sortedmovies.map(lambda item: (namedict.value[item[1]], item[0]))
    # sortedmovies = [(count, movieid)] [(583, 50)]
    # namedict = {movieid: 'moviename'} {50 : 'Star Wars (1977)'}

    for result in sortedmovieswithnames.collect():
        print(result)

"""
To see 5 items in a dictionary:
from itertools import islice

list(islice(movienames.items(), 5))

"""
