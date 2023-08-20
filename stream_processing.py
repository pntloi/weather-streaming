from pyspark.sql.functions import (
    from_json,
    col,
    year, month, dayofmonth
)
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
        
                
    # def writeToHDFS(stream, path='', )


#   def writeToDB(stream, path, format, topic, isPartition=False):
# if isPartition is True:
#             response = stream.writeStream \
#                 .format(format) \
#                 .partitionBy('_year', '_month') \
#                 .option('header', True) \