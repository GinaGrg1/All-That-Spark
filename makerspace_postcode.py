from pyspark import SparkContext, SparkConf
import re

"""
How are those maker spaces distributed across different regions in the UK?

Soln:
    load the postcode dataset and broadcast it across the cluster
    load the maker space dataset and call map operation on the maker space RDD to look
    up region using the postcode of the maker space.

The postcode in the maker space RDD is the full postcode: W1T 3AC, E14 9TC, SW12 7YC
The postcode in the postcode dataset is only the prefix : W1T, E14, SW12

"""


def loadpostcodemap():
    lines = open("./in/uk-postcode.csv", "r").read().split("\n")
    splitsForLines = [re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line) for line in lines if line != ""]
    return {splits[0]: splits[7] for splits in splitsForLines}  # {'AB1' : 'Aberdeen'}


def getpostprefix(line):
    """
    This func takes the line from makerspaces file, takes the postcode, splits it & returns the first part
    SE15 3SN ==> SE15
    If this field is empty, return None.
    """
    splits = re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line)
    postcode = splits[4]
    return None if not postcode else postcode.split(" ")[0]


if __name__ == '__main__':
    conf = SparkConf().setAppName("Maker space postcode problem").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    postCodeMap = sc.broadcast(loadpostcodemap())  # dictionary lookup. postCodeMap.value['SE15'] ==> 'Lewisham'

    makerSpaceRDD = sc.textFile("./in/uk-makerspaces-identifiable-data.csv")

    # First remove the header line. Then only keep those lines whose postcode can be split into, say for eg, 'SE15'
    # If the postcode is in postCodeMap dictionary, return it from there else return 'Unknown'
    regions = makerSpaceRDD.filter(lambda line: re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line) != 'Timestamp')\
                           .filter(lambda line: getpostprefix(line) is not None)\
                           .map(lambda line: postCodeMap.value[getpostprefix(line)] if getpostprefix(line) in postCodeMap.value else "Unknown")

    for region, count in regions.countByValue().items():
        print("{} : {}".format(region, count))

    """
    regions.take(10)
    ['Region', 'Lewisham', 'Gedling', 'Belfast', 'Medway', 'Birmingham', 'Lambeth', 'Brent', 'Cardiff', 'Unknown']
    """