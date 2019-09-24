from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("create").setMaster("local")
sc = SparkContext(conf=conf)

tuples = [("Regina", 33), ("Ajay", 31), ("Sabina", 34), ("Hitender", 5)]
pairRDD = sc.parallelize(tuples)

pairRDD.coalesce(1).saveAsTextFile('out/pair_rdd_from_tuple_list')


# Creating pair RDD from a Regular RDD
inputstrings = ["Regina 33", "Ajay 31", "Sabina 34", "Hitender 5"]
regularRDD = sc.parallelize(inputstrings)

regularRDD_final = regularRDD.map(lambda s: (s.split(" ")[0], int(s.split(" ")[1])))
regularRDD_final.coalesce(1).saveAsTextFile('out/pair_rdd_from_regular_rdd')

"""
def coalesce(self, numPartitions, shuffle=False):
    Returns a new RDD that is reduced into 'numPartitions' partitions
Makes sure that we only have one file instead of creating multiple 'part-0000' files

"""