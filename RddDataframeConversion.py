from pyspark.sql import SparkSession
import re


class Utils:
    COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')


def mapResponseRdd(line):
    splits = Utils.COMMA_DELIMITER.split(line)
    double1 = None if not splits[6] else float(splits[6])
    double2 = None if not splits[14] else float(splits[14])
    return splits[2], double1, splits[9], double2


def getcolnames(line):
    splits = Utils.COMMA_DELIMITER.split(line)
    return [splits[2], splits[6], splits[9], splits[14]]


spark = SparkSession.builder.appName("Stackoverflow Survey").master("local[*]").getOrCreate()
sc = spark.sparkContext

lines = sc.textFile("./in/2016-stack-overflow-survey-responses.csv")

responseRDD = lines.filter(lambda line: not Utils.COMMA_DELIMITER.split(line)[2] == "country")\
                   .map(mapResponseRdd)

colnames = lines.filter(lambda line: Utils.COMMA_DELIMITER.split(line)[2] == "country")\
                .map(getcolnames)

# Convert RDD to  Dataframe
responseDataFrame = responseRDD.toDF(colnames.collect()[0])

# Print out the schema.
responseDataFrame.printSchema()

# Print first 20 rows of response tables
responseDataFrame.show()

for response in responseDataFrame.rdd.take(10):
    print(response)


