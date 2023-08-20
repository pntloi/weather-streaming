# Version
Hadoop 3.3.6 
Kafka 2.12-3.5.0
Spark/PySpark 3.4.1

## Start airflow, postgres, redis, hive
docker-compose up airflow-init
docker-compose up -d


## format namenode
$HADOOP_HOME/bin/hdfs namenode -format

# start hadoop
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

List dir: hdfs dfs -ls /

hdfs dfs -mkdir /pntloi/weather_streaming
hdfs dfs -ls /pntloi


## ZK
$ZK_HOME/bin/zkServer.sh start


#### Create topics
kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --topic  \
    --create \
    --partitions 3 \
    --replication-factor 1


## SPARK
start-master.sh
nano <log-path>

start-worker.sh <masterUrl> (spark://<host>:7077)
spark-class org.apache.spark.deploy.worker.Worker spark://<host>:7077 (To start 2nd spark worker)

## KafkaSpark to stream data
spark-submit streamExtraction.py


