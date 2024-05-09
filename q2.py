# Develop a Spark application that finds the best and the worst restaurants for
# each city for each price range (in terms of rating). Write the output as CSV
# into HDFS path /assignment2/output/question2/. For simplicity, you can
# ignore rows with Price Range field as null.
# You may use RDD API and/or Dataframe API. You are not allowed to use
# Spark SQL API.
# Sample output:
# +-----+--------------------+----------+--------------------+-------+------+-----------+-----------------+--------------------+--------------------+---------+
# | _c0| Name| City| Cuisine Style|Ranking|Rating|Price Range|Number of Reviews| Reviews| URL_TA| ID_TA|
# +-----+--------------------+----------+--------------------+-------+------+-----------+-----------------+--------------------+--------------------+---------+
# | 3198| Pietersma Snacks| Amsterdam|[ 'Dutch', 'Europ...| 3209.0| 5.0| $| null| [ [ ], [ ] ]|/Restaurant_Revie...|d10587448|
# | 2932| Grillroom Sabba| Amsterdam|[ 'Middle Eastern' ]| 2942.0| 2.5| $| 12.0|[ [ 'This is a gr...|/Restaurant_Revie...| d6464568|
# | 1503| 1 Chefalyon| Lyon|[ 'Pub', 'Gastrop...| 1485.0| 5.0| $$$$| null| [ [ ], [ ] ]|/Restaurant_Revie...|d12408653|
# | 2605| Papagayo| Lyon| [ 'Diner' ]| 2606.0| 2.0| $$$$| 33.0| [ [ ], [ ] ]|/Restaurant_Revie...| d1329792|
# | 2951| le bountje| Brussels|[ 'Belgian', 'Eur...| 2952.0| 5.0| $$ - $$$| null| [ [ ], [ ] ]|/Restaurant_Revie...| d1563747|
# | 3009| Belga & Co| Brussels| [ 'European' ]| null| -1.0| $$ - $$$| null| [ [ ], [ ] ]|/Restaurant_Revie...|d13531979|
# | 2462| Sushi Express| Stockholm|[ 'Japanese', 'Su...| null| 5.0| $$ - $$$| 2.0| [ [ ], [ ] ]|/Restaurant_Revie...|d13344590|






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
