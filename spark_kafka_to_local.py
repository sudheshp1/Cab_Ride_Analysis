# Importing necessary libraries
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

appName = "Kafka to HDFS"
master = "local"

# Creating a spark session
spark = SparkSession.builder \
        .master(master) \
        .appName(appName) \
        .getOrCreate()
        
# Setting log level to ERROR
spark.sparkContext.setLogLevel('ERROR')

kafka_server = "18.211.252.152:9092"
topic ="de-capstone5"

#Reading Streaming data from de-capstone3 kafka topic
df = spark.readStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", kafka_server) \
       .option("startingOffsets", "earliest") \
       .option("subscribe", topic) \
       .load()

df= df.withColumn('value_str',df['value'].cast('string').alias('key_str')).drop('value') \
      .drop('key','topic','partition','offset','timestamp','timestampType')

#Writing data from kakfa to local file
df.writeStream \
  .format("json") \
  .outputMode("append") \
  .option("path", "/user/hadoop/kafka_stream/kafka_clickstream") \
  .option("checkpointLocation", "/user/hadoop/kafka_stream/kafka_clickstream_Checkpoint") \
  .start() \
  .awaitTermination()
