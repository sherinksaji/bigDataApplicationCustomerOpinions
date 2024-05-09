# Develop a Spark application that finds the pairs of actors/actresses that are
# co-cast for at least 2 movies. The output should be in a (set of) Parquet files
# in the following schema:
# movie_id, title, actor1, actor2
# Note that the result should not contain any repetition, e.g.
# 49026, The Dark Knight Rises, Michael Caine, Christian Bale
# is considered as a duplicate entry of
# 49026, The Dark Knight Rises, Christian Bale, Michael Caine
# The output should be something like the following:
# +--------+--------------------+--------------------+--------------------+
# |movie_id| title| actor1| actor2|
# +--------+--------------------+--------------------+--------------------+
# | 69848| One Man's Hero| James Gammon| Tom Berenger|
# | 9942| Major League| James Gammon| Tom Berenger|
# | 285|Pirates of the Ca...| David Bailie| Ho-Kwan Tse|
# | 58|Pirates of the Ca...| David Bailie| Ho-Kwan Tse|
# | 921| Cinderella Man| Michael Stevens|Conrad Bergschneider|
# | 14577| Dirty Work| Michael Stevens|Conrad Bergschneider|
# | 16290| Jackass 3D|Dimitry Elyashkevich| Manny Puig|
# | 12094| Jackass Number Two|Dimitry Elyashkevich| Manny Puig|
# | 9012| Jackass: The Movie|Dimitry Elyashkevich| Manny Puig|







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
