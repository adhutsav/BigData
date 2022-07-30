from os import truncate
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import sum, desc
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType

spark = SparkSession.builder.appName("Top10GreatestContribution").getOrCreate()

businessSchema = StructType([ \
                     StructField("business_id", StringType(), True), \
                         StructField("full_address", StringType(), True), \
                             StructField("categories", StringType(), True)])

reviewSchema = StructType([ \
                     StructField("review_id", StringType(), True), \
                         StructField("user_id", StringType(), True), \
                             StructField("business_id", StringType(), True), \
                                 StructField("stars", FloatType(), True)])

reviews = spark.read.schema(reviewSchema).option("sep", "::").csv("/Users/adhutsav/Desktop/BigData/HW2/input/review.csv")
businesses = spark.read.schema(businessSchema).option("sep", "::").csv("/Users/adhutsav/Desktop/BigData/HW2/input/business.csv")
businesses = businesses.distinct()
reviews.createOrReplaceTempView("Reviews")
businesses.createOrReplaceTempView("Business")
str_query = "SELECT r.business_id, b.full_address, b.categories, count(*) as no_of_ratings \
    FROM Reviews as r JOIN Business as b ON r.business_id = b.business_id\
    GROUP BY r.business_id, b.full_address, b.categories\
        ORDER BY (no_of_ratings) DESC\
            LIMIT 10"

query = spark.sql(str_query)
query.show(truncate = False)
spark.stop()