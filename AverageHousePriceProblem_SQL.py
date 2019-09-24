"""
Create a spark program to read the house data from in/RealEstate.csv, group by location,
aggregate the avg price per SQ FT and sort by average price per SQ Ft.

"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

PRICE_SQ_FT = 'Price SQ Ft'

session = SparkSession.builder.appName("SparkSQL House price problem").master("local[1]").getOrCreate()
dataFrameReader = session.read

realestatedata = dataFrameReader.option("header", "true").option("inferschema", value=True)\
                                .csv("./in/RealEstate.csv")
# First group by 'Location'.
# Find the aggregate and rename the col as agg_price. Finally order by the agg_price
sumsqftpricebylocation = realestatedata.groupBy("Location").agg(f.sum('Price SQ Ft').alias('agg_price'))\
                                        .orderBy('agg_price')

# To find the average
avgsqftpricebylocation = realestatedata.groupBy("Location").agg(f.avg('Price SQ Ft').alias('agg_price'))\
                                        .orderBy('agg_price')

# Oreilly's answer
realEstate = session.read.option("header", "true").option("inferSchema", value=True)\
                         .csv("./in/RealEstate.csv")

realEstate.groupBy("Location").avg(PRICE_SQ_FT).orderBy("avg(Price SQ Ft)").show()
