"""
3rd col : country
5th col : age_mid
10th col : occupation
15th col : salary_midpoint

"""

from pyspark.sql import SparkSession

AGE_MIDPOINT = "age_midpoint"
SALARY_MIDPOINT = "salary_midpoint"
SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket"

session = SparkSession.builder.appName("Stackoverflow Dev salary").master("local[1]").getOrCreate()
dataFrameReader = session.read

responses = dataFrameReader.option("header", "true").option("inferschema", value=True)\
                   .csv("./in/2016-stack-overflow-survey-responses.csv")

responses.printSchema()

# Select the dataframe with some columns
responsesWithSelectedColumns = responses.select("country", "occupation", AGE_MIDPOINT, SALARY_MIDPOINT)

# Selecting a particular country. show() shows the first 20 rows.
responsesWithSelectedColumns.filter(responsesWithSelectedColumns['country'] == "Canada").show()

# Grouping by occupations
groupedData = responsesWithSelectedColumns.groupBy("occupation")
# To see all the grouped occupation
groupedData.count().show()
# To find max, min, avg across this grouped data we can use groupedData.max().show() etc.

# To print records with avg mid age less than 20
responsesWithSelectedColumns.filter(responsesWithSelectedColumns[AGE_MIDPOINT] < 20).show()

# Print the result salary middle point in descending order
responsesWithSelectedColumns.orderBy(responsesWithSelectedColumns[SALARY_MIDPOINT], ascending=False).show(5)

# Group by country and aggregate by average salary middle point
avgmidsalary = responsesWithSelectedColumns.groupBy("country").avg(SALARY_MIDPOINT).show()

# Create SalaryBucket column.
responseWithSalaryBucket = responses.withColumn(SALARY_MIDPOINT_BUCKET,
                                                ((responses[SALARY_MIDPOINT]/20000).cast("integer")*20000))

# To see the result
responseWithSalaryBucket.select(SALARY_MIDPOINT, SALARY_MIDPOINT_BUCKET).show()

# Group by salary bucket
responseWithSalaryBucket.groupBy(SALARY_MIDPOINT_BUCKET)\
                        .count()\
                        .orderBy(SALARY_MIDPOINT_BUCKET)\
                        .show()

session.stop()









