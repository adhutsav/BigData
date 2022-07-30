from pyspark.sql import SparkSession
import json
from pyspark.sql.functions import udf
#from pyspark.sql.types import LongType


#display(df.select("id", squared_udf("id").alias("id_squared")))

def helper(sentense):
    return sentense + "neg"


spark = SparkSession.builder.appName("StreamingTest").config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0').getOrCreate()
helper_udf = udf(helper)
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "hashtags") \
  .option("startingOffsets", "earliest")\
  .load()

print("Fine Till now!!")
newDf = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
newDf.printSchema()
df.printSchema()

newDf.writeStream.format("console").outputMode("append").start().awaitTermination()

"""
newDf.foreach(lambda x: json.loads(x.decode('utf-8')))
newDf.show()
"""