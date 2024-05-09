import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, split, regexp_replace, trim
from pyspark.sql.types import StringType, ArrayType

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW
# Read the data into a DataFrame# Define the input and output paths
input_path = f"hdfs://{hdfs_nn}/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"


# Load the data
df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)


df = df.withColumn("Cuisine Style", regexp_replace("Cuisine Style", "\\[", ""))
df = df.withColumn("Cuisine Style", regexp_replace("Cuisine Style", "\\]", ""))
df = df.withColumn("Cuisine Style", split(col("Cuisine Style"), ", "))

df_exploded = (
    df.withColumn("Cuisine Style", explode("Cuisine Style"))
    .withColumn("Cuisine Style", regexp_replace("Cuisine Style", "'", ""))
    .withColumn("Cuisine Style", trim(col("Cuisine Style")))
)
df_exploded.show()
df_exploded = df_exploded.select("City", "Cuisine Style")
df_exploded.show()

# Group by 'City' and 'Cuisine' and count
df_count = df_exploded.groupBy("City", "Cuisine Style").count()


df_count.show()

df_count = df_count.withColumnRenamed("Cuisine Style", "Cuisine")

df_count.show()

# Write the output as CSV files into the HDFS path
output_path = f"hdfs://{hdfs_nn}/assignment2/output/question4/"

df_count.write.csv(output_path, header=True)

# Stop the Spark session
spark.stop()