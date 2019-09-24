from pyspark import SparkContext

"""
nasa_19950701.tsv contains 10000 log lines from one of NASA's apache server for
1st July 1995.
nasa_19950801.tsv contains 10000 log lines for 1st Aug 1995.
Create a spark program to generate a new RDD which contains the log lines
from both July 1st & Aug 1st, take 0.1 sample of those log lines and save it
to "out/sample_nasa_logs.tsv"

The file has header lines:
host logname time method url response bytes

Make sure the head lines are removed in the resulting RDD
"""

sc = SparkContext("local[2]", "NASA log Problem")
sc.setLogLevel("ERROR")

nasa_july = sc.textFile("./in/nasa_19950701.tsv")
nasa_aug = sc.textFile("./in/nasa_19950801.tsv")

nasa_both = nasa_july.union(nasa_aug)

nasa_both_without_header = nasa_both.filter(lambda line: not (line.startswith("host") and "bytes" in line))

nasa_both_sample = nasa_both_without_header.sample(withReplacement=True, fraction=0.1)

nasa_both_sample.saveAsTextFile("out/sample_nasa_logs.csv")
