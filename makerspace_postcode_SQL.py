"""
The postcode in the makerspace data source is the full postcode : W1T 3AC
The postcode in the postcode data source is only prefix : W1T
Join condition:
    If the postcode column in the makerspace data source starts with the postcode
    column in the postcode data source.

Corner case:
    W14D T2Y might match W14D and W14.
Solution:
    Append a space to the postcode prefix. So W14D will only match "W14D " & not "W14 "

df.withColumn("new", F.col("A"))  ==> creates a new col 'new'
F.col is equivalent to df.A or df['A']

df.withColumn("new", F.lit("A")).show() ==> creates a constant column with the given string as the value.
"""

from pyspark.sql import SparkSession, functions as fs

spark = SparkSession.builder.appName("UK Maker Spaces SQL").master("local[*]").getOrCreate()
dataFrameReader = spark.read

makerSpace = dataFrameReader.csv("./in/uk-makerspaces-identifiable-data.csv", header=True)

# creates a new column PostCode, the value of which is the concat of postcode and a " "
postCode = dataFrameReader.csv("./in/uk-postcode.csv", header=True)\
                          .withColumn("PostCode", fs.concat_ws("", fs.col("PostCode"), fs.lit(" ")))

print("=== Print 20 records of makerspace table ===")
makerSpace.select("Name of makerspace", "Postcode").show()

print("=== Print 20 records of postcode table ===")
postCode.select("PostCode", "Region").show()

joined = makerSpace.join(postCode, makerSpace["Postcode"].startswith(postCode["Postcode"]), "left_outer")

print("=== Group by Region ===")
joined.groupBy("Region").count().show(200)

