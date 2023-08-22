import os
from pyspark.sql import SparkSession
from schema import Schema
from stream_processing import Stream

import psycopg2

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1'

spark = SparkSession.builder \
    .master("spark://pntloi:7077") \
    .config("spark.dynamicAllocation.executorIdleTimeout", "600s") \
    .appName("WeatherStreaming").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


def connectDatabase(row):
        HOSTNAME="localhost"
        DATABASE="airflow"
        PORT=5432
        USER="airflow"
        PASSWORD="airflow"
        DBTABLE="openweather"

        try:
            conn = psycopg2.connect(
                host=HOSTNAME,
                database=DATABASE,
                port=PORT,
                user=USER,
                password=PASSWORD
            )
            cur = conn.cursor()
            print("\n \n \n 111111111111111111111111111111111111111111111111111111111111111111111111")
            create_sql = f"CREATE TABLE IF NOT EXISTS {DBTABLE} (id serial primary key, \
                cityId integer, \
                cityName varchar, \
                latitude float, \
                longitude float, \
                countryName varchar, \
                temperature float, \
                maxTemp float, \
                minTemp float, \
                feelsLike float, \
                humidity float, \
                createdAt timestamp, \
                year integer, \
                _year integer, \
                month integer, \
                _month integer, \
                day integer, \
                _day integer \
            );"

            sql=f"INSERT INTO {DBTABLE} (cityId, cityName, latitude, longitude, countryName, temperature, \
                maxTemp, minTemp, feelsLike, humidity, createdAt, year, \
                _year, month, _month, day, _day) \
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            
            val=(row['cityId'], row['cityName'], row['latitude'], row['longitude'], \
                row['countryName'], row['temperature'], row['maxTemp'], row['minTemp'], row['feelsLike'], \
                row['humidity'], row['createdAt'], row['year'], row['_year'], \
                row['month'], row['_month'], row['_day'], row['day'])
            
            cur.execute(create_sql)
            cur.execute(sql, val)
            # return cur
            conn.commit()
            conn.close()
            cur.close()

        except(Exception, psycopg2.DatabaseError) as e:
            print(e)





### readStream
weather_events = Stream.read(spark, "openweather").load()



### processStream
weather_schema = Schema.raw_weather_schema()

raw_response, processed_response = Stream.processData(weather_events, weather_schema)

# raw_response.printSchema()
# processed_response.printSchema()


### writeToDB
Stream.writeToDB(processed_response, connectDatabase)



### writeStreamToCSV
Stream.writeToHDFS(processed_response)
