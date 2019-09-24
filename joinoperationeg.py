from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Join Operations").setMaster("local[1")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

ages = sc.parallelize([("Tom", 29), ("John", 22)])
addresses = sc.parallelize([("James", "USA"), ("John", "Nepal")])

# use collect() to take the action.
join = ages.join(addresses)                      # [('John', (22, 'Nepal'))]
leftOuterJoin = ages.leftOuterJoin(addresses)    # [('Tom', (29, None)), ('John', (22, 'Nepal'))]
rightOuterJoin = ages.rightOuterJoin(addresses)  # [('James', (None, 'USA')), ('John', (22, 'Nepal'))]
fullOuterJoin = ages.fullOuterJoin(addresses)    # [('Tom', (29, None)), ('James', (None, 'USA')), ('John', (22, 'Nepal'))]

