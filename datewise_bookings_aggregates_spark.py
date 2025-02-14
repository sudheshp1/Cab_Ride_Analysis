# Importing necessary libraries
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

appName = "datewise_bookings_aggregates"
master = "local"

# Iniliating Spark application/session
spark=SparkSession.builder.appName(appName).master(master).getOrCreate()

#Reading bookings data stored into HDFS directory
df=spark.read.csv("/user/hadoop/bookings/part-m-00000")

#Verifing that all columns are imported
df.printSchema()

# Verify imported data with first 10 records
df.show(10,False)

#Verifing that entier file is imported and no of records are equal to no of records in HDFS file
df.count()

#Renaming the default columns names 
new_col = ["booking_id","customer_id","driver_id","customer_app_version","customer_phone_os_version","pickup_lat","pickup_lon","drop_lat",
          "drop_lon","pickup_timestamp","drop_timestamp","trip_fare","tip_amount","currency_code","cab_color","cab_registration_no","customer_rating_by_driver",
          "rating_by_customer","passenger_count"]

#Assigning column names
new_df = df.toDF(*new_col)

#Converting pickup_timestamp to date by extracting date from pickup_timestamp for aggregation
new_df=new_df.select("booking_id","customer_id","driver_id","customer_app_version","customer_phone_os_version","pickup_lat","pickup_lon","drop_lat",
          "drop_lon",to_date(col('pickup_timestamp')).alias('pickup_date').cast("date"),"drop_timestamp","trip_fare","tip_amount","currency_code","cab_color","cab_registration_no","customer_rating_by_driver",
          "rating_by_customer","passenger_count")

#Verifying column names
new_df.show(10,False)

#Aggregation on pickup_date
agg_df=new_df.groupBy("pickup_date").count().orderBy("pickup_date")

#Verifying Aggregated dataframe
agg_df.show(10,False)

# Saving aggregated data into .csv format back to HDFS location
agg_df.coalesce(1).write.format('csv').mode('overwrite').save('/user/hadoop/datewise_bookings_agg',header='true')