import os
from pyspark.sql import SparkSession
from schema import Schema
from stream_processing import Stream

import psycopg2

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1'

spark = SparkSession.builder \
    .master("spark://pntloi:7077") \
    .config("spark.dynamicAllocation.executorIdleTimeout", "600s") \
    .appName("writeHDFS").getOrCreate()



weather_events = Stream.read(spark, "openweather").load()

spark.sparkContext.setLogLevel("ERROR")


### processStream
weather_schema = Schema.raw_weather_schema()

raw_response, processed_response = Stream.processData(weather_events, weather_schema)

Stream.writeToHDFS(processed_response)


