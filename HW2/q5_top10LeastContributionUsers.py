from os import truncate
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType

spark = SparkSession.builder.appName("Top10LeastContribution").getOrCreate()

reviewSchema = StructType([ \
                     StructField("review_id", StringType(), True), \
                         StructField("user_id", StringType(), True), \
                             StructField("business_id", StringType(), True), \
                                 StructField("stars", FloatType(), True)])

userSchema = StructType([ \
                     StructField("user_id", StringType(), True), \
                         StructField("name", StringType(), True), \
                             StructField("url", StringType(), True)])


reviews = spark.read.schema(reviewSchema).option("sep", "::").csv("/Users/adhutsav/Desktop/BigData/HW2/input/review.csv")
users = spark.read.schema(userSchema).option("sep", "::").csv("/Users/adhutsav/Desktop/BigData/HW2/input/user.csv")

reviews.createOrReplaceTempView("Reviews")
users.createOrReplaceTempView("Users")
query = spark.sql("SELECT u.name , 100 * ((count(*) / (SELECT count(*) FROM Reviews))) as contribution\
        FROM Reviews as r JOIN Users as u ON r.user_id = u.user_id\
        GROUP BY u.name \
        ORDER BY contribution ASC\
        LIMIT 10")
res = query.show(truncate=False)

spark.stop()