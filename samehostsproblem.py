from pyspark import SparkContext, SparkConf

"""
Create a spark program to generate a new RDD which contains the hosts which
are accessed on BOTH days.
Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file

"""
conf = SparkConf().setAppName("Same Host Problem").setMaster("local[1]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

nasa_july = sc.textFile("./in/nasa_19950701.tsv")
nasa_aug = sc.textFile("./in/nasa_19950801.tsv")


nasa_july_hosts = nasa_july.map(lambda line: line.split('\t')[0])
nasa_aug_hosts = nasa_aug.map(lambda line: line.split("\t")[0])

nasa_both_hosts = nasa_july_hosts.intersection(nasa_aug_hosts)

clean_nasa_both_hosts = nasa_both_hosts.filter(lambda line: line != 'host')

clean_nasa_both_hosts.saveAsTextFile("out/nasa_logs_same_hosts.csv")
