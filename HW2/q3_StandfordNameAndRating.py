from os import truncate
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType

spark = SparkSession.builder.appName("BusinessWithStandford").getOrCreate()

userSchema = StructType([ \
                     StructField("user_id", StringType(), True), \
                         StructField("name", StringType(), True), \
                             StructField("url", StringType(), True)])

reviewSchema = StructType([ \
                     StructField("review_id", StringType(), True), \
                         StructField("user_id", StringType(), True), \
                             StructField("business_id", StringType(), True), \
                                 StructField("stars", FloatType(), True)])

businessSchema = StructType([ \
                     StructField("business_id", StringType(), True), \
                         StructField("full_address", StringType(), True), \
                             StructField("categories", StringType(), True)])

users = spark.read.schema(userSchema).option("sep", "::").csv("/Users/adhutsav/Desktop/BigData/HW2/input/user.csv")
reviews = spark.read.schema(reviewSchema).option("sep", "::").csv("/Users/adhutsav/Desktop/BigData/HW2/input/review.csv")
businesses = spark.read.schema(businessSchema).option("sep", "::").csv("/Users/adhutsav/Desktop/BigData/HW2/input/business.csv")
businesses = businesses.distinct()

reviews.createOrReplaceTempView("Reviews")
businesses.createOrReplaceTempView("Business")
users.createOrReplaceTempView("Users")
"""
Query:
        SELECT name, stars 
        FROM Business JOIN Reviews JOIN Users
        WHERE full_address LIKE '%Stanford%'

"""

businesses.filter(businesses.full_address.like('%Stanford%'))\
    .join(reviews, businesses.business_id == reviews.business_id)\
        .join(users, reviews.user_id == users.user_id).select("name", "stars").show(500, truncate=False)

spark.stop()