import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,first,avg,desc,col,min,max

# Initialize the Spark session
spark = SparkSession.builder.appName("Assignment 2 Question 2").getOrCreate()

# Define the HDFS name node from command line arguments
hdfs_nn = sys.argv[1]

# Define input and output paths
input_path = f"hdfs://{hdfs_nn}/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
output_path = f"hdfs://{hdfs_nn}/assignment2/output/question2/"

# Load the data
df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
df = df.na.drop(how="any", subset=["Price Range","Rating"])
best_df = (
    df.groupBy(["Price Range", "City"])
    .agg(max("Rating"))
    .withColumn("Rating", col("max(Rating)"))
    .drop("max(Rating)")
)
worst_df = (
    df.groupBy(["Price Range", "City"])
    .agg(min("Rating"))
    .withColumn("Rating", col("min(Rating)"))
    .drop("min(Rating)")
)
union_df = best_df.union(worst_df)
final_df = union_df.join(df, on=["Price Range", "City", "Rating"], how="inner")
final_df = (
    final_df.dropDuplicates(["Price Range", "City", "Rating"])
    .select(
        "_c0",
        "Name",
        "City",
        "Cuisine Style",
        "Ranking",
        "Rating",
        "Price Range",
        "Number of Reviews",
        "Reviews",
        "URL_TA",
        "ID_TA",
    )
    .sort(col("City").asc(), col("Price Range").asc(), col("Rating").desc())
)

final_df.show()
final_df.write.csv(output_path,header=True)
