# Develop a Spark application that cleans up the CSV file by removing rows
# with no reviews or rating < 1.0. Write the output as CSV into HDFS path
# /assignment2/output/question1/.
# Sample output
# +---+--------------------+-----+--------------------+-------+------+-----------+-----------------+--------------------+--------------------+---------+
# |_c0| Name| City| Cuisine Style|Ranking|Rating|Price Range|Number of Reviews| Reviews| URL_TA| ID_TA|
# +---+--------------------+-----+--------------------+-------+------+-----------+-----------------+--------------------+--------------------+---------+
# |700|Auberge de la Rei...|Paris|[ 'French', 'Euro...| 701.0| 4.0| $$ - $$$| 489.0|[ [ 'Cozy Restaur...|/Restaurant_Revie...| d695128|
# |701| Le Petit Vendome|Paris|[ 'French', 'Euro...| 702.0| 4.0| $$ - $$$| 343.0|[ [ 'Parisian way...|/Restaurant_Revie...| d1146488|
# |702| La Cave Lanrezac|Paris|[ 'Wine Bar', 'Eu...| 703.0| 4.5| $$ - $$$| 178.0|[ [ 'Dinner with ...|/Restaurant_Revie...| d812970|
# |704|Chez Fernand Chri...|Paris|[ 'French', 'Euro...| 705.0| 4.0| $$ - $$$| 892.0|[ [ 'Tourist Area...|/Restaurant_Revie...| d1580042|






import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, IntegerType
# you may add more import if you need to
from pyspark.sql.functions import col, udf, asc, desc, lit, length, count
import logging


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"hdfs://{hdfs_nn}/assignment2/part1/input/TA_restaurants_curated_cleaned.csv")


@udf(returnType=IntegerType())
def count_characters(review):
    if review:
        return len(review)
    else:
        return 0


df_with_char_count = df.withColumn("Review_Length", count_characters(df["Reviews"]))
df_with_char_count = df_with_char_count.orderBy(desc("Review_Length"))
df_with_char_count.show()

df_with_char_count = df_with_char_count.orderBy(asc("Review_Length"))
df_with_char_count.show()
df_with_char_count = df_with_char_count.filter(
    ~(col("Number of Reviews").isNull() & (col("Review_Length") <= 14))
)
df_with_char_count.show()


df_with_char_count = df_with_char_count.na.drop(subset=["Rating"])
df_filtered = df_with_char_count.filter(
    (col("Rating").cast("float") >= 1.0)
    )
df_filtered.show()    
df_filtered = df_filtered.orderBy(asc("Rating"))



df_filtered.show()

df_filtered = df_filtered.drop("Review_Length")


#check whether rows with null Number Of Reviews and empty reviews exist using Rakadiko Stoa which has rating=4
contains_rakadiko_stoa = df_filtered.filter(col("Name").contains("Rakadiko Stoa"))
contains_rakadiko_stoa.show()

df_filtered.show()

# Define the output path
output_path = f"hdfs://{hdfs_nn}/assignment2/output/question1/"

# Write the filtered DataFrame to CSV in the specified HDFS path
df_filtered.write.option("header", True).csv(output_path)

# Stop the Spark session
spark.stop()

