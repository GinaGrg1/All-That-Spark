from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Movie data analysis").getOrCreate()
sc = spark.sparkContext


def countoccurences(line):
    elements = line.split()
    return int(elements[0]), (len(elements) - 1)


def parsenames(line):
    fields = line.split('\"')
    return int(fields[0]), fields[1].encode("utf8")


names = sc.textFile("./Marvel-Names.txt")
namesRDD = names.map(parsenames)  # This is of type: <class 'pyspark.rdd.PipelinedRDD'>

# print(namesRDD.lookup(859)) ==> [b'CAPTAIN AMERICA']

# print(namesRDD.take(3)) [(1, b'24-HOUR MAN/EMMANUEL'), (2, b'3-D MAN/CHARLES CHAN'), (3, b'4-D MAN/MERCURIO')]

graph = sc.textFile("./Marvel-Graph.txt")
pairings = graph.map(countoccurences)  # [(5988, 48), (5989, 40), (5982, 42)]
# print(pairings.take(3))

totalfriendsbycharacter = pairings.reduceByKey(lambda x, y: x+y)  # [(5988, 48), (5982, 42), (5980, 24)]
flipped = totalfriendsbycharacter.map(lambda x: (x[1], x[0]))     # [(48, 5988), (42, 5982), (24, 5980)]


mostpopular = flipped.max()  # this is a tuple of length 1. Takes the max value of flipped. (1933, 859)

mostpopularname = namesRDD.lookup(mostpopular[1])[0]  # looks in namesRDD for value 859

print(mostpopularname.decode('utf8') + " is the most popular superhero, with " + str(mostpopular[0]) + " co-appearances.")
