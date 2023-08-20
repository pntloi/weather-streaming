from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType, LongType, FloatType, TimestampType

class Schema():
    @staticmethod
    def raw_weather_schema():
        return StructType([
            StructField("city_id", IntegerType(), False),
            StructField("city_name", StringType(), True),
            StructField("lat", StringType(), True),
            StructField("lon", StringType(), True),
            StructField("country", StringType(), True),
            StructField("temp", FloatType(), True),
            StructField("max_temp", FloatType(), True),
            StructField("min_temp", FloatType(), True),
            StructField("feels_like", FloatType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("created_at", TimestampType(), True)
        ])
    
    @staticmethod
    def processed_weather_schema():
        return StructType([
            StructField("cityId", IntegerType(), False),
            StructField("cityName", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("countryName", StringType(), True),
            StructField("temperature", FloatType(), True),
            StructField("maxTemp", FloatType(), True),
            StructField("minTemp", FloatType(), True),
            StructField("feelsLike", FloatType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("createdAt", TimestampType(), True),
            StructField("year", IntegerType(), True),
            StructField("_year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("_month", IntegerType(), True),
            StructField("day", IntegerType(), True),
            StructField("_day", IntegerType(), True)
        ])

