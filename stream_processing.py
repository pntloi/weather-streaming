from pyspark.sql.functions import (
    from_json,
    col,
    year, month, dayofmonth
)
import pyspark.sql.functions as func
from utility import *

new_col_names = {
    "city_id": "cityId",
    "city_name": "cityName",
    "lat": "latitude",
    "lon": "longitude",
    "country": "countryName",
    "temp": "temperature",
    "max_temp": "maxTemp",
    "min_temp": "minTemp",
    "feels_like": "feelsLike",
    "humidity": "humidity",
    "created_at": "createdAt"
}

class Stream():
    @staticmethod
    def read(spark, topic):
        response = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic)
        return response
    


    def processData(stream, schema):
        raw_response = stream.selectExpr("CAST(value AS STRING)") \
                        .select(from_json(col("value"), schema).alias("data")) \
                        .select("data.*")
        
        # raw_response.writeStream.format("console").option("truncate", "false").start().awaitTermination()

        processed_response = raw_response \
            .withColumn("temp", func.round(col("temp"), 2)) \
            .withColumn("max_temp", func.round(col("max_temp"), 2)) \
            .withColumn("min_temp", func.round(col("min_temp"), 2)) \
            .withColumn("feels_like", func.round(col("feels_like"), 2)) \
            .withColumn("_year", year(col("created_at"))) \
            .withColumn("year", year(col("created_at"))) \
            .withColumn("_month", month(col("created_at"))) \
            .withColumn("month", month(col("created_at"))) \
            .withColumn("_day", dayofmonth(col("created_at"))) \
            .withColumn("day", dayofmonth(col("created_at"))) \
            
        processed_response = rename_columns(processed_response, new_col_names)
        # processed_response.printSchema()
        # processed_response.writeStream.format("console").option("truncate", "false").start().awaitTermination()
        return raw_response, processed_response


    def writeToDB(stream, connectDatabase):
        # stream.printSchema()
        stream.describe()
        stream.writeStream.format("console").option("truncate", "false").outputMode("append").start()
        stream.writeStream.outputMode("update").foreach(connectDatabase).start().awaitTermination(1000)
        
                
    def writeToHDFS(stream, path="hdfs://localhost:9001/pntloi/weather_streaming", format="csv", formatOption="append", outputMode="append", chkpointPath="hdfs://localhost:9001/pntloi/weather_streaming/checkpoint"):
        csv_df = stream.writeStream \
            .partitionBy("_year", "_month", "_day") \
            .format(format) \
            .option("format", formatOption) \
            .trigger(processingTime="5 seconds")\
            .option("path", path)\
            .option("checkpointLocation", chkpointPath) \
            .outputMode(outputMode) \
            .start()
        
        csv_df.awaitTermination()
            