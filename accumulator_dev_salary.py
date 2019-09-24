from pyspark import SparkContext, SparkConf
import re

"""
We want to find all three below while reading in the file only once.
How many records do we have in this survey result?
How many records are missing the salary middle point?
How many records are from Canada?

Here: row 2 is country & 14 is salary_midpoint
Add another accumulator to count bytes.
"""
conf = SparkConf().setAppName('StackOverFlow Salary Survey').setMaster("local[*]")
sc = SparkContext(conf=conf)

total = sc.accumulator(0)   # no of records?
missingSalaryMidpoint = sc.accumulator(0)
processedBytes = sc.accumulator(0)

stackoverflowdata = sc.textFile("./in/2016-stack-overflow-survey-responses.csv")


def filterResponseFromCanada(response):
    processedBytes.add(len(response.encode('utf-8')))
    splits = re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', response)
    total.add(1)
    if not splits[14]:  # if salary_midpoint is empty
        missingSalaryMidpoint.add(1)
    return splits[2] == "Canada"


responseFromCanada = stackoverflowdata.filter(filterResponseFromCanada)
print("Number of bytes processed : {}".format(processedBytes.value))
print("Total responses from Canada : {}".format(responseFromCanada.count()))
print("Total number of responses : {}".format(total.value))
print("Total number of missing salary midpoint value: {}".format(missingSalaryMidpoint.value))

"""
This problem could also be solved using reduce or reduceByKey.
It is possible to aggregate values from an entire RDD back to the driver program using reduce or
reduceByKey.
"""