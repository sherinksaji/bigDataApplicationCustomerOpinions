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
