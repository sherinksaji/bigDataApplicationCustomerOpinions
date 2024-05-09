import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.functions import from_json, col, explode, array, array_sort, count

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW

jsonSchema = ArrayType(StructType([StructField("name", StringType(), False)]))

#/assignment2/part2/input/tmdb_5000_credits.parquet:
input_path = f"hdfs://{hdfs_nn}/assignment2/part2/input/"
df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .parquet(input_path)
)

df = df.drop("crew")

df = df.withColumn(
    "actor1", explode(from_json(col("cast"), jsonSchema).getField("name"))
)
df = df.withColumn(
    "actor2", explode(from_json(col("cast"), jsonSchema).getField("name"))
)
df = df.select("movie_id", "title", "actor1", "actor2")


df = df.filter(col("actor1") != col("actor2"))

df = df.withColumn("cast_pair", array(col("actor1"), col("actor2"))).withColumn(
    "cast_pair", array_sort(col("cast_pair")).cast("string")
)


df = df.dropDuplicates(["movie_id", "title", "cast_pair"]).sort(col("cast_pair").asc())

counts_df = (
    df.groupBy("cast_pair")
    .agg(count("*").alias("count"))
    .filter(col("count") >= 2)
    .sort(col("cast_pair").asc())
)

joined_df = (
    counts_df.join(df, ["cast_pair"], "inner")
    .drop("count")
    .sort(col("cast_pair").asc())
)

joined_df=joined_df.drop("cast_pair")


joined_df.show()
output_path = f"hdfs://{hdfs_nn}/assignment2/part2/output/question5/"
joined_df.write.option("header", True).mode("overwrite").parquet(output_path)
