# Develop a Spark application that extracts the three cities with the highest and
# lowest average rating per restaurant. Combine them, sorted, such that the
# output looks like this:
# For instance:
# +--------+------------------+-----------+
# | City| AverageRating|RatingGroup|
# +--------+------------------+-----------+
# | Athens| 4.241316931982634| Top|
# | London| 4.178003263308178| Top|
# | Krakow| 4.164012738853503| Top|
# | Geneva| 3.97270245677889| Bottom|
# |Helsinki|3.9153318077803205| Bottom|
# |Brussels| 3.900580875781948| Bottom|
# +--------+------------------+-----------+
# Write the output as CSV files into HDFS path /assignment2/output/question3/.










import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col,asc, desc,lit

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
# Define the input and output paths
input_path = f"hdfs://{hdfs_nn}/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
output_path = f"hdfs://{hdfs_nn}/assignment2/output/question3/"

# Load the data
df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

df.show()

# Calculate average rating for each city
avg_rating_df = df.groupBy("City").agg(avg("Rating").alias("AverageRating"))

avg_rating_df.show()

# Get top 3 cities with highest average ratings
top_cities = avg_rating_df.orderBy(desc("AverageRating")).limit(3)
top_cities = top_cities.withColumn("RatingGroup", lit("Top"))

top_cities.show()

# Get bottom 3 cities with lowest average ratings
bottom_cities = avg_rating_df.orderBy("AverageRating").limit(3)
bottom_cities = bottom_cities.withColumn("RatingGroup", lit("Bottom"))

bottom_cities.show()

# Combine the top and bottom cities
combined_cities = top_cities.union(bottom_cities)

# Sort the cities by the group and then by AverageRating in descending order
combined_cities = combined_cities.orderBy(desc("AverageRating"))

combined_cities.show()
# Write the output as CSV files into HDFS
combined_cities.write.option("header", "true").csv(output_path)

# Stop Spark session
spark.stop()
