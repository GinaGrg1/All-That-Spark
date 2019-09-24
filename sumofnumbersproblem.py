from pyspark import SparkContext, SparkConf

"""
Create a Spark program to read the first 100 prime number from in/prime_nums.text,
print the sum of those numbers to console.
Each row of the input file contains 10 prime numbers separated by spaces.
"""

conf = SparkConf().setAppName("Sum of Numbers Problem").setMaster("local[1]")
sc = SparkContext(conf=conf)

prime_number = sc.textFile('./in/prime_nums.text')

prime_numero = prime_number.flatMap(lambda line: line.split('\t'))\
                           .map(lambda x: int(x))\
                            .reduce(lambda x, y: x+y)

