# Importing necessary libraries
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

appName = "Json to csv"
master = "local"

# Creating a spark session
spark = SparkSession.builder \
        .master(master) \
        .appName(appName) \
        .getOrCreate()

#Reading data from hdfs
df=spark.read.json("/user/hadoop/kafka_stream/kafka_clickstream/*.json")


df.show(10,truncate=False)

#Selecting the columns from the clickstream data set and assigning alias
df=df.select(get_json_object(df['value_str'],"$.customer_id").alias("customer_id"),
            get_json_object(df['value_str'],"$.app_version").alias("app_version"),
            get_json_object(df['value_str'],"$.OS_version").alias("OS_version"),
            get_json_object(df['value_str'],"$.lat").alias("lat"),
            get_json_object(df['value_str'],"$.lon").alias("lon"),
            get_json_object(df['value_str'],"$.page_id").alias("page_id"),
            get_json_object(df['value_str'],"$.button_id").alias("button_id"),
            get_json_object(df['value_str'],"$.is_button_click").alias("is_button_click"),
            get_json_object(df['value_str'],"$.is_page_view").alias("is_page_view"),
            get_json_object(df['value_str'],"$.is_scroll_up").alias("is_scroll_up"),
            get_json_object(df['value_str'],"$.is_scroll_down").alias("is_scroll_down"),
            get_json_object(df['value_str'],"$.timestamp\n").alias("timestamp"))

#validating schema
df.printSchema()

# removing \n from timestamp column
df_cleaned = df.withColumn("timestamp", regexp_replace("timestamp", "\n", ""))

#Validating records in top 10 records
df_cleaned.show(10,False)

#Writing the format final dataset to hdfs
df_cleaned.coalesce(1).write.format('csv').mode('overwrite').save('/user/hadoop/kafka_stream/clickstream',header='true')