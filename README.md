Setting cores:
local[2] : 2 cores
local[*] : all available cores
local    : 1 core

Broadcast variable:
Boardcast variables allows to keep a read-only variable cached on each machine rather than
shipping a copy of it with tasks.
Broadcasts objects to every executors node, so that they are always there whenever needed.
Use sc.broadcast() to ship off whatever you want and use .value() to get the object back
Broadcast variables share objects across all nodes in the cluster.
They can be used, for eg, to give every node a copy of a large input dataset (like dictionary), 
in an efficient manner.

DEGREES OF SEPARATION: BREADTH-FIRST SEARCH

Accumulator:
Allows all the executors in your cluster to increment some shared variable.So its basically a counter as 
maintained and synchronised across all of the different nodes in your cluster.
These are variables that are used for aggregating information across the executors.
For e.g, we can calculate how many records are corrupted or count events that occur during job execution
for debugging purposes.

Flatmap vs Map:
greet = sc.textFile("greetings.txt")
greet.map(lambda x: x.split()).collect()
[['Good', 'Morning'], ['Good', 'Evening'], ['Good', 'Day'], ['Happy', 'Birthday'], ['Happy', 'New', 'Year']]
greet.map(lambda x: x.split())

greet.flatMap(lambda x: x.split()).collect()
['Good', 'Morning', 'Good', 'Evening', 'Good', 'Day', 'Happy', 'Birthday', 'Happy', 'New', 'Year']
greet.flatMap(lambda x: x.split()).countByValue()
{'Good': 3, 'Morning': 1, 'Evening': 1, 'Day': 1, 'Happy': 2, 'Birthday': 1, 'New': 1, 'Year': 1}

greet.flatMap(lambda x: x.split()).countByKey()
{'G': 3, 'M': 1, 'E': 1, 'D': 1, 'H': 2, 'B': 1, 'N': 1, 'Y': 1}

Another example:
rdd = sc.parallelize([2,3,4]) 
rdd.flatMap(lambda x: range(1, x)).collect()
[1, 1, 2, 1, 2, 3]

rdd.map(lambda x: range(1, x)).collect()
[range(1, 2), range(1, 3), range(1, 4)]

cache() vs persist():
persist() lets you cache it to disk instead of just memory, in case a node fails.
When you persist an RDD, the first time it is computed in an action, it will be kept in
memory across the nodes.

Set Operations:
- distinct : Returns a new RDD with distinct elements. Expensive transformation as it
             requires shuffling all the data across partitions.
- sample : creates a random sample from an RDD
            [withreplacement=True|False, fraction=sample size, seed=]

Set operations performed on two RDDs [must be same type] and produce one resulting RDD:
- union
- intersection : returns common elements in both input RDDs.
                 removes all duplicates including the duplicates from single RDD
                 expensive as it requires shuffling
- subtract  : takes in another RDD as an argument & returns an RDD that only contains
              element present in the first RDD and not the second RDD
- cartesian product

Getting the nth row in an RDD:
RDD.take(N)[N-1]

Common Actions in Spark:
- collect : returns a list?
- count
- countByValue : return a dict {'value', count}
- take : returns a list
- saveAsTextFile : can write to local system, S3, hdfs
- reduce : integerRdd.reduce(lambda x,y: x*y)

Transformations returns RDDs, whereas actions return some other data type.

Paired RDD:
Key Value pairs.
E.g: A dataset which contains passport IDs and the names of the passport holders

map and mapValues transformations:
* The map transformation also works for Pair RDDs. It can be used to convert an RDD to another one.
* When working with pair RDDs, we don't want to modify the keys, we just want to access the value part.
* mapValues function will be applied to each key value pair & will convert the values based on mapValues
  but will not change the keys.
  
Reduce By Key Aggregation:

groupByKey:
Not very efficient. groupByKey + [reduce, map, mapValues] can be replaced by one of the per key aggregation function
such as reduceByKey.
To reduce the amount of shuffle for groupByKey, we can use (hash) partitioning.
partiitonedRDD = wordPair.partitionBy(4).persist(StorageLevel.DISK_ONLY)
partiitonedRDD.groupByKey().collect()

This would avoid random shuffling.

sortByKey:
    sortByKey(self, ascending=True, numPartitions=None, keyfnc=lambda x:x)

The sortBy transformation maps the pair RDD to (keyfunc(x), x) tuples using the function that
receives as parameter, applies sortByKey and finally return the values.

Data Partitioning:
Some operations that would benefit from partitioning:
- join, leftOuterJoin, rightOuterJoin, groupByKey, reduceByKey, combineByKey, lookup

Running reduceByKey on a pre-partitioned RDD will cause all the values for each key to be computed
locally on a single machine, requiring only the final, locally reduced value to be sent from each
worker node back to the master.

Operations which would be affected by partitioning:
Operations like map could cause the new RDD to forget the parent's partitioning information, as such
operations could, in theory, change the key of each element in the RDD.
General guidance is to prefer mapValues over map partition.

Join Operations Best Practices:
If both RDDs have duplicate keys, join operation can dramatically expand the size of the data. It's
recommended to perform a distinct or combineByKey operation to reduce the key space if possible.
Join operation may require large network transfers or even create data sets beyond our capability to handle.
Joins, in general, are expensive since they require that corresponding keys from each RDD are located at the
same partition so that they can be combined locally.
If the RDDs do not have known partitioners, they will need to be shuffled so that both RDDs share a 
partitioner and data with the same keys lives in the same partitions.

Spark SQL:
- DataFrame: tabular data abstraction. Is a data abstraction or a domain-specific language for working with structured
             and semi-structured data. Stores data in a more efficient manner than native RDDs, taking advantage of 
             their schema. It uses immutable, in-memory, resilient, distributed and parallel capabilities of RDD &
             applies a structure called schema to the data, allowing Spark to manage the schema and only pass data
             between nodes, in a much more efficient way than using Java serialization. Unlike an RDD, data is organised
             into named columns, like a table in a relational database.
- Dataset : The Dataset API provides the familiar object-oriented programming style & compile-time type safety of the RDD
            API. It also has the benefits of leveraging schema to work with structured data.
            A dataset is a set of structured data, not necessarily a row but it could be of a particular type.

Catalyst Optimizer
Spark SQL uses an optimizer called Catalyst to optimize all the queries written both in Spark SQL & DataFrame DSL.
This optimizer makes queries run much faster than their RDD counterparts.
The Catalyst is a modular library which is built as a rule-based system.
Each rule in the framework focuses on the specific optimization. For e.g, rule like ConstantFolding focuses on
removing constant expression from the query.

Spark SQL Joins [ RDD joins have to be the same type ?]
Spark SQL supports the same basic join types as core spark
Spark SQL Catalyst optimizer can do more of the heavy lifting for us to optimize the join performance
Spark SQL can sometimes push down or re-order operations to make the joins more efficient.
The downside is that we don't have controls over the partitioner for DataFrames, so we can't manually avoid
shuffles as we did with core Spark joins.

Spark SQL join types:
- inner, outer, left outer, right outer, left semi
def join(self, other, on=None, how=None)

left semi joins : only returns row in the left tables, nothing from the right table even if there is a match

Dataframe or RDD?:
In general, Dataframes should be considered over RDDs.RDD is still the underlying building blocks for Dataframes.
DataFrames are the new hotness. MLlib is on the shift to DataFrame based API.
RDD is the primary user-facing API in Spark.
At the core, RDD is an immutable distributed collection of elements of your data, partitioned across nodes in your
cluster that can be operated in parallel with a low-level API that offers transformations and actions.
Use RDDs when:
    - Low-level transformation, actions and control on our dataset are needed
    - Unstructured data, such as media streams or streams of text.
    - Need to manipulate our data with functional programming constructs than domain specific expressions.
    - Optimization & performance benefits available with DataFrames are NOT needed.
Use DataFrames when:
    - Rich semantics, high-level abstractions, & domain specific APIs are needed.
    - Our processing requires aggregation, average, sum, SQL queries and columnar access on semi-structured data
    - We want the benefit of catalyst optimization
    - Unification and simplification of APIs across Spark Libraries are needed.

Dataframe and RDD conversion:

Performance Tuning of Spark SQL
Caching: responseDataFrame.cache()
When caching a dataframe, Spark SQL uses an in-memory columnar storage for the dataframe
If our subsequent queries depend only on subsets of the data, Spark SQL will minimize the data read, & automatically
tune compression to reduce garbage collection pressure and memory usage.

Configure Spark Properties:
    session = SparkSession.builder.config("spark.sql.codegen", value=False).getOrCreate()

It will ask Spark SQL to compile each query to java byte code before executing it.
This codegen option could make long queries or repeated queries substantially faster, as Spark generates specific code
to run them.
It's recommended to use codegen option for workflows which involves large queries, or with the same repeated query.

    session = SparkSession.builder.config("spark.sql.inMemoryColumnarStorage.batchsize", value=1000).getOrCreate()

spark-submit options:
    spark-submit --executor-memory 20G --total-executor-cores 100 path/to/ex.py
    