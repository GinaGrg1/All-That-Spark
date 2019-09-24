from pyspark import SparkConf, SparkContext, StorageLevel

conf = SparkConf().setAppName("persist").setMaster("local[*]")
sc = SparkContext(conf=conf)

integerrdd = sc.parallelize([1, 2, 3, 4, 5])

integerrdd.persist(StorageLevel.MEMORY_ONLY)

integerrdd.reduce(lambda x, y: x*y)

integerrdd.count()
